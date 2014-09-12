namespace Nessos.MBrace.Runtime

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Store
    open Nessos.MBrace.Utils
    open System.Runtime.Caching

    
    [<AutoOpen>]
    module private Helpers =

        [<Diagnostics.DebuggerDisplay("{Id}:{Folder}/{Name}:{IndexFile}   ({StartIndex},{EndIndex})")>]
        type SegmentDescription = 
            {   Id          : int
                Name        : string
                Folder      : string
                IndexFile   : string
                StartIndex  : int64
                EndIndex    : int64 }
    
        type CloudArrayDescription =
            {   Count    : int64
                Segments : List<SegmentDescription> } 

        /// Max segment size in bytes.     // TODO : tune
        let MaxSegmentSize = 1024L * 1024L // TODO : tune
        /// Max PageCache size in bytes.   // TODO : tune
        let MaxPageCacheSize  = 1024L     // TODO : tune
        /// Max PageCache items.           // TODO : tune
        let MaxPageCacheItems = 1024       // TODO : tune

        let newSegment(id, name, folder, descriptor, start, ``end``) =
            { Id         = id
              Name       = name
              Folder     = folder
              IndexFile  = descriptor
              StartIndex = start
              EndIndex   = ``end`` }
    
        let newCloudArrayDescription(count, segments) =
            { Count = count; Segments = segments}
    
    /// Forward only caching. When asked for item i caches the next MaxPageSize items.
    type internal PageCache<'T>(folder : string, descriptor : string, length : int64, store : ICloudStore) =

        let mutable currentPageStart = -1L

        let buffer = new List<'T>()

        let isCached index =
            currentPageStart <> -1L 
            && currentPageStart <= index 
            && index < currentPageStart + int64 buffer.Count

        //member internal this.Buffer : List<'T> = buffer

        member internal this.FetchPageAsync(index : int64, ?pageItems : int) =
            async {
                    buffer.Clear()
                    currentPageStart <- -1L
                    let pageItems = defaultArg pageItems MaxPageCacheItems

                    // Read descriptor file, find the segment, read from segment
                    use! descriptorStream = store.ReadImmutable(folder, descriptor)

                    let segmentsDescription = Serialization.DefaultPickler.Deserialize<CloudArrayDescription>(descriptorStream).Segments
                    let segment = segmentsDescription |> Seq.find (fun s -> s.StartIndex <= index && index <= s.EndIndex )
                    use! segmentDescrStream = store.ReadImmutable(folder, segment.IndexFile)

                    use br = new BinaryReader(segmentDescrStream)
                    let relativePos = index - segment.StartIndex
                    br.BaseStream.Seek(relativePos * int64 sizeof<int64> , SeekOrigin.Begin) |> ignore
                    let startPosition = br.ReadInt64()
    
                    let endPosition = ref startPosition
                    let itemCounter = ref 1
                    while !itemCounter < pageItems && !endPosition - startPosition <= MaxPageCacheSize && br.BaseStream.Position < br.BaseStream.Length do
                        endPosition := !endPosition + br.ReadInt64()
                        incr itemCounter

                    use! segmentStream = store.ReadImmutable(folder, segment.Name)

                    segmentStream.Seek(startPosition, SeekOrigin.Begin) |> ignore

                    for i = 1 to !itemCounter do
                        buffer.Add(Serialization.DefaultPickler.Deserialize<'T>(segmentStream, leaveOpen = true))

                    currentPageStart <- index
            }

        member this.GetItem<'T>(index : int64) : 'T = 
            if isCached index then
                buffer.[int(index-currentPageStart)]
            else
                this.FetchPageAsync(index)
                |> Async.RunSynchronously
                this.GetItem<'T>(index)

        member this.GetItem<'T>(index : int64, pageItems : int) : 'T = 
            if isCached index then
                buffer.[int(index-currentPageStart)]
            else
                this.FetchPageAsync(index)
                |> Async.RunSynchronously
                this.GetItem<'T>(index, pageItems)

    type [<Serializable>] CloudArray<'T> internal (folder : string, descriptorName : string, count : int64, storeId : StoreId) as this =

        let provider  = lazy CloudArrayProvider.GetById storeId  
        let pageCache = lazy provider.Value.GetPageCache(this)

        member internal this.Provider   = provider
        member internal this.StoreId    = storeId

        override this.ToString() = sprintf "cloudarray:%s/%s" folder descriptorName

        internal new(info : SerializationInfo, context : StreamingContext) =
            let folder     = info.GetValue("folder", typeof<string>) :?> string
            let descriptor = info.GetValue("descriptor", typeof<string>) :?> string
            let count      = info.GetValue("count", typeof<int64>) :?> int64
            let storeId    = info.GetValue("storeId", typeof<StoreId>) :?> StoreId
            new CloudArray<'T>(folder, descriptor, count, storeId)

        interface ISerializable with
            member this.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("folder",     folder)            
                info.AddValue("descriptor", descriptorName)
                info.AddValue("count" ,     count)
                info.AddValue("storeId" ,   storeId, typeof<StoreId>)

        interface ICloudArray with
  
            member this.Length with get () = count
            member this.Container with get () = folder
            member this.Name with get () = descriptorName
            member this.Type with get () = typeof<'T>

            member this.Append(cloudArray: ICloudArray): ICloudArray = 
                let ltype, rtype = (this :> ICloudArray).Type, cloudArray.Type 
                if ltype <> rtype then
                    raise <| StoreException(sprintf "Cannot Append CloudArrays of different types '%A' and '%A'" ltype rtype)
                else
                    (this :> ICloudArray<'T>).Append(cloudArray :?> ICloudArray<'T>) :> _

            member this.Cache(): ICachedCloudArray = 
                (this :> ICloudArray<'T>).Cache() :> _

            member this.Item
                with get (index: int64): obj = 
                    (this :> ICloudArray<'T>).Item index :> _

            member this.Range(start: int64, count: int): obj [] = 
                (this :> ICloudArray<'T>).Range(start, count) // TODO : inefficient
                |> Seq.cast<obj>
                |> Seq.toArray

        interface ICloudArray<'T> with 
            member this.Item 
                with get (index : int64) : 'T =
                    if index < 0L || index >= count then
                        raise <| IndexOutOfRangeException(sprintf "Index = %d" index)
                    else
                        pageCache.Value.GetItem<'T>(index)

            member __.Range(start : int64, length : int) : 'T [] =
                __.RangeAsync<'T>(start, length) 
                |> Async.RunSynchronously

            member this.Append(cloudArray : ICloudArray<'T>) : ICloudArray<'T> =
                this.AppendAsync(cloudArray :?> CloudArray<'T>) // TODO: CHANGE
                |> Async.RunSynchronously :> _

            member this.Cache() : ICachedCloudArray<'T> =
                new CachedCloudArray<'T>(folder, descriptorName, count, storeId) :> _
                     
        interface IEnumerable<'T> with
            member this.GetEnumerator() : IEnumerator<'T> =
                let ca = this :> ICloudArray<'T>
                let length = count
                let index = ref 0L
                let current = ref Unchecked.defaultof<'T>
                { new IEnumerator<'T> with
                    member this.Current = !current
                    member this.Current = !current :> obj
                    member this.Dispose() = ()
                    member this.MoveNext () =
                        if !index < length then
                            current := ca.[!index]
                            index := !index + 1L
                            true
                        else
                            false
                    member this.Reset () = ()
                }

        interface IEnumerable with
            member this.GetEnumerator() : IEnumerator =
                (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator

        member __.RangeAsync<'T>(start : int64, length : int) : Async<'T []> =
            async {
                if start < 0L || length < 0 || start + int64 length > count then
                    return raise <| IndexOutOfRangeException(sprintf "Start = %d, Length = %d" start length) 
                elif count = 0L then
                    return Array.empty
                else
                    return! provider.Value.GetRangeAsync(start, length, this)
            }

        member this.AppendAsync(cloudArray : CloudArray<'T>) : Async<CloudArray<'T>> =
            provider.Value.AppendAsync(this, cloudArray)


    and [<Serializable>] CachedCloudArray<'T> internal (folder : string, descriptorName : string, count : int64, storeId : StoreId) =

        inherit CloudArray<'T>(folder, descriptorName, count, storeId) 

        interface ICloudArray<'T> with
            member this.Range(start : int64, length : int) =
                if start < 0L || length < 0 || start + int64 length > count then
                    raise <| IndexOutOfRangeException(sprintf "Start = %d, Length = %d" start length)
                elif count = 0L then
                    Array.empty
                else
                    async {
                        let fetch s l = this.Provider.Value.GetRangeAsync(s,l, this)
                        let! items = CloudArrayCache.FetchAsync<'T>(this, fetch, start, length)
                        return items
                    } |> Async.RunSynchronously

        interface ICachedCloudArray<'T>

        internal new(info : SerializationInfo, context : StreamingContext) =
            let folder     = info.GetValue("folder", typeof<string>) :?> string
            let descriptor = info.GetValue("descriptor", typeof<string>) :?> string
            let count      = info.GetValue("count", typeof<int64>) :?> int64
            let storeId    = info.GetValue("storeId", typeof<StoreId>) :?> StoreId
            new CachedCloudArray<'T>(folder, descriptor, count, storeId)

        interface ISerializable with
            member this.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("folder",     folder)            
                info.AddValue("descriptor", descriptorName)
                info.AddValue("count" ,     count)
                info.AddValue("storeId" ,     storeId, typeof<StoreId>)

    and CloudArrayProvider private (storeId : StoreId, store : ICloudStore) =
        static let providers = new System.Collections.Concurrent.ConcurrentDictionary<StoreId, CloudArrayProvider>()

        let mkCloudArrayId () = Guid.NewGuid().ToString("N") 
    
        let mkDescriptorName = sprintf "%s.ca"
        let mkSegmentName    = sprintf "%s.ca.segment.%d"
        let mkIndexFileName  = sprintf "%s.ca.segment.%d.index"
    
        let getRangeAsync start length folder descriptorName =
            async {
                let result = Array.zeroCreate length
                
                use! descriptorStream = store.ReadImmutable(folder, descriptorName)
                let segmentsDescription = Serialization.DefaultPickler.Deserialize<CloudArrayDescription>(descriptorStream).Segments
                let segment = segmentsDescription |> Seq.find (fun s -> s.StartIndex <= start && start <= s.EndIndex )
                use! segmentDescrStream = store.ReadImmutable(folder, segment.IndexFile)
                
                use br = new BinaryReader(segmentDescrStream)
                let relativePos = start - segment.StartIndex
                br.BaseStream.Seek(relativePos * int64 sizeof<int64> , SeekOrigin.Begin) |> ignore
                let startPosition = br.ReadInt64()
    
                let! segmentStream = store.ReadImmutable(folder, segment.Name)
                segmentStream.Seek(startPosition, SeekOrigin.Begin) |> ignore
    
                let currentSegment = ref segment
                let currentSegmentStream = ref segmentStream
    
                for i = 0 to length-1 do
                    if currentSegmentStream.Value.Position = currentSegmentStream.Value.Length then
                        currentSegmentStream.Value.Dispose()
                        let index = start + int64 i
                        currentSegment := segmentsDescription |> Seq.find (fun s -> s.StartIndex <= index && index <= s.EndIndex )
                        let! nextSegmentStream = store.ReadImmutable(folder, currentSegment.Value.Name)
                        currentSegmentStream := nextSegmentStream
                    let item = Serialization.DefaultPickler.Deserialize<'T>(currentSegmentStream.Value, leaveOpen = true)
                    result.[i] <- item
    
                currentSegmentStream.Value.Dispose()
    
                return result
            }

        
        let defineUntyped(ty : Type, container : string, descriptor : string, count : int64) =
            let existential = Existential.Create ty
            let ctor =
                {
                    new IFunc<ICloudArray> with
                        member __.Invoke<'T> () = new CloudArray<'T>(container, descriptor, count, storeId) :> ICloudArray
                }

            existential.Apply ctor

        let createAsync folder (source : IEnumerable) (ty : Type) : Async<ICloudArray> = async {
            let guid                = mkCloudArrayId()
            let descriptorName      = mkDescriptorName guid
            let sourceEnd           = ref false
            let segmentItems        = ref 0

            let serialize (e : IEnumerator) (segmentIndexFile : string) (stream : Stream) : Async<unit> =
                async {
                    let serialize' (segmentStream : Stream) =
                        async {
                            segmentItems := 0

                            let bw = new BinaryWriter(segmentStream)
                            let moveNext = ref true

                            let segmentEndCheck () =
                                if stream.Length < MaxSegmentSize then 
                                    moveNext := e.MoveNext()
                                    !moveNext
                                else
                                    false

                            while segmentEndCheck() do
                                let item = e.Current
                                bw.Write(stream.Position)
                                Serialization.DefaultPickler.Serialize(ty, stream, item, leaveOpen = true)
                                incr segmentItems
                            if not !moveNext then
                                sourceEnd := true
                            bw.Flush()
                            segmentStream.Dispose()
                        }
                    do! store.CreateImmutable(folder, segmentIndexFile, serialize' , true)
                    stream.Dispose()
                }
        
            let e            = source.GetEnumerator()
            let segmentId    = ref 0
            let segmentStart = ref 0L
            let segmentsDescription  = new List<SegmentDescription>()

            while not !sourceEnd do
                let filename  = mkSegmentName   guid !segmentId
                let indexFile = mkIndexFileName guid !segmentId

                do! store.CreateImmutable(folder, filename , serialize e indexFile, true)

                let segmentEnd = !segmentStart + int64 (!segmentItems - 1)
                segmentsDescription.Add(newSegment(!segmentId, filename, folder, indexFile, !segmentStart, segmentEnd))
                segmentStart := segmentEnd + 1L
                segmentId := !segmentId + 1

            // Delete last empty segment
            let segmentsDescription = segmentsDescription
                                        |> Seq.groupBy(fun segment -> segment.StartIndex <= segment.EndIndex)
                                        |> fun s ->
                                            match s |> Seq.tryFind (fst >> not) with
                                            | None -> ()
                                            | Some (_, segment) ->
                                                let segment = segment |> Seq.exactlyOne
                                                async {
                                                    do! store.Delete(segment.Folder, segment.IndexFile)   
                                                    do! store.Delete(segment.Folder, segment.Name)
                                                } |> Async.RunSynchronously
                                            let s = s |> Seq.tryFind fst 
                                            match s with
                                            | None   -> new List<_>()
                                            | Some s -> new List<_>(snd s)

            let arrayDescription = newCloudArrayDescription(!segmentStart, segmentsDescription)

            let writeSegmentsDescription (stream : Stream) =
                async {
                    Serialization.DefaultPickler.Serialize<CloudArrayDescription>(stream, arrayDescription) |> ignore
                    stream.Dispose()
                }

            do! store.CreateImmutable(folder, descriptorName, writeSegmentsDescription, true)

            return defineUntyped(ty, folder, descriptorName, !segmentStart)
        }

        let appendAsync (left : CloudArray<'T>) (right : CloudArray<'T>) : Async<CloudArray<'T>> = async {
            if left.StoreId <> right.StoreId then
                failwithf "StoreId mismatch %A, %A" left.StoreId right.StoreId

            // Read
            let readDescriptor(cloudArray : CloudArray<'T>) = async {
                use! descriptorStream = store.ReadImmutable((cloudArray :> ICloudArray).Container , (cloudArray :> ICloudArray).Name)
                return Serialization.DefaultPickler.Deserialize<CloudArrayDescription>(descriptorStream)
            }

            let! leftDescr  = readDescriptor left
            let! rightDescr = readDescriptor right

            // Merge
            let leftSegmentCount = leftDescr.Segments.Count
            let rightDescr' = rightDescr.Segments
                                |> Seq.map (fun segment -> 
                                            newSegment(segment.Id + leftSegmentCount, 
                                                        segment.Name, 
                                                        segment.Folder,
                                                        segment.IndexFile,
                                                        segment.StartIndex + leftDescr.Count,
                                                        segment.EndIndex   + leftDescr.Count))
            let finalSegmentsDescription = List<_>(Seq.append leftDescr.Segments rightDescr')
            let finalDescr = newCloudArrayDescription(leftDescr.Count + rightDescr.Count, finalSegmentsDescription)
                                              
            // Write
            let guid           = mkCloudArrayId()
            let descriptorName = mkDescriptorName guid

            let serialize stream =
                async {
                    Serialization.DefaultPickler.Serialize<CloudArrayDescription>(stream, finalDescr) |> ignore
                    stream.Dispose()
                }

            do! store.CreateImmutable((left :> ICloudArray).Container, descriptorName , serialize, true)

            return new CloudArray<'T>((left :> ICloudArray).Container, descriptorName, finalDescr.Count, storeId)
        }

        static member internal Create (storeId : StoreId, store : ICloudStore) =
            providers.GetOrAdd(storeId, fun id -> new CloudArrayProvider(id, store))

        static member internal GetById (storeId : StoreId) : CloudArrayProvider =
            let ok, provider = providers.TryGetValue storeId
            if ok then provider
            else
                let msg = sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName
                raise <| new StoreException(msg)

        member internal __.GetPageCache<'T>(ca : CloudArray<'T>) : PageCache<'T> =
            let ca = ca :> ICloudArray
            new PageCache<'T>(ca.Container, ca.Name, ca.Length, store)

        member __.GetRangeAsync<'T>(start : int64, length : int, ca : CloudArray<'T>) : Async<'T []> =
            let ca = ca :> ICloudArray
            getRangeAsync start length ca.Container ca.Name

        member __.Create(container : string, values : IEnumerable, ty : Type) : Async<ICloudArray> =
            createAsync container values ty

        member __.AppendAsync<'T>(left : CloudArray<'T>, right : CloudArray<'T>) : Async<CloudArray<'T>> =
            appendAsync left right

    and [<AbstractClass; Sealed>] CloudArrayCache () =

        static let createKey name start ``end`` = 
            sprintf "%s %d %d" name start ``end``

        static let parseKey(key : string) =
            let key = key.Split()
            key.[0], int64 key.[1], int64 key.[2]

        static let config = new System.Collections.Specialized.NameValueCollection()
        static do  config.Add("PhysicalMemoryLimitPercentage", "70")

        static let mc = new MemoryCache("CloudArrayMemoryCache", config)

        static let sync      = new obj()
        static let itemsSet  = new HashSet<string * int64 * int64>()
        static let policy    = new CacheItemPolicy()
        static do  policy.RemovedCallback <-
                    new CacheEntryRemovedCallback(
                        fun args ->
                            lock sync (fun () -> itemsSet.Remove(parseKey args.CacheItem.Key) |> ignore)
                    )

        static let registerAsync(ca : CachedCloudArray<'T>) (startIndex : int64) (endIndex : int64) (items : 'T []) : Async<unit> = 
            async {
                    let key = createKey (ca.ToString()) startIndex endIndex
                    mc.Add(key, items, policy) |> ignore
                    lock sync (fun () -> itemsSet.Add((ca.ToString()),startIndex,endIndex) |> ignore)
                    return ()
            }

        static member FetchAsync<'T>(ca : CachedCloudArray<'T>, fetch : int64 -> int -> Async<'T []> , startIndex : int64, length : int) : Async<'T []> =
            async {
                let endIndex = startIndex + int64 length - 1L
                let key = createKey (ca.ToString()) startIndex endIndex
                if mc.Contains(key) then
                    return mc.Get(key) :?> _
                else
                    let entries = 
                        mc 
                        |> Seq.map    (fun kvp -> kvp.Key, parseKey kvp.Key)
                        |> Seq.filter (fun (kvp, (cas, s, e)) -> cas = ca.ToString())
                        |> Seq.map    (fun (kvp, (cas, s, e)) -> kvp, s, e)
                        |> Seq.filter (fun (cas, s, e) -> not (e < startIndex || s > endIndex) )
                        |> Seq.sortBy (fun (_, s, e) -> s, e)
                        |> Seq.toList
                
                    if Seq.isEmpty entries then
                        let! fetched = fetch startIndex length
                        do! registerAsync ca startIndex endIndex fetched
                        return fetched
                    else
                        let result = Array.zeroCreate<'T> length

                        let rec concatLoop currentStartIndex entries = 
                            async {
                                match entries with
                                | [] when currentStartIndex > endIndex -> 
                                    return ()
                                | [] -> 
                                    let! fetched = fetch currentStartIndex (int (endIndex-currentStartIndex))
                                    do! registerAsync ca currentStartIndex (currentStartIndex + fetched.LongLength) fetched
                                    Array.Copy(fetched, 0L, result, currentStartIndex - startIndex, fetched.LongLength)
                                | (key, s, e) :: t when currentStartIndex >= s ->
                                    let cached = mc.Get(key) :?> 'T []
                                    let len = 1L + if endIndex > e then e - currentStartIndex else endIndex - currentStartIndex
                                    Array.Copy(cached, currentStartIndex - s , result, currentStartIndex - startIndex, len)
                                    return! concatLoop (currentStartIndex + len) t
                                | (key, s, e) :: t  ->
                                    let len = s - currentStartIndex
                                    let! fetched = fetch currentStartIndex (int len)
                                    do! registerAsync ca currentStartIndex (currentStartIndex + len) fetched
                                    Array.Copy(fetched, 0L, result, currentStartIndex - startIndex, len)
                                    return! concatLoop (currentStartIndex + len) entries
                            }
                        do! concatLoop startIndex entries
                        return result

            }

        static member State =
            itemsSet :> seq<_>

    /// StartIndex * Length.
    type internal CloudArrayRange = int64 * int
    /// CloudArray identifier.
    type internal CloudArrayId    = string

    [<Sealed;AbstractClass>]
    type internal CloudArrayRegistry () =
        static let guid     = System.Guid.NewGuid()
        static let registry = Concurrent.ConcurrentDictionary<CloudArrayId, List<CloudArrayRange>>()
        
        static member GetRegistryId () = guid
        
        static member Add(id : CloudArrayId, range : CloudArrayRange) =
            registry.AddOrUpdate(
                id, 
                new List<CloudArrayRange>([range]), 
                fun _ (existing : List<CloudArrayRange>) -> existing.Add(range); existing)
            |> ignore
        
        static member Get(id : CloudArrayId) =
            match registry.TryGetValue(id) with
            | true, ranges -> ranges
            | _ -> failwith "Non-existent CloudArray"

        static member GetNumberOfRanges(id : CloudArrayId) =
            CloudArrayRegistry.Get(id).Count

        static member Contains(id : CloudArrayId) =
            registry.ContainsKey(id)


