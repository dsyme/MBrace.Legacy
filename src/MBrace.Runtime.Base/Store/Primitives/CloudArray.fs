namespace Nessos.MBrace.Runtime

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Store
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime.StoreUtils

    [<Diagnostics.DebuggerDisplay("{Id}:{Folder}/{Name}:{IndexFile}   ({StartIndex},{EndIndex})")>]
    type internal SegmentDescription = 
        {   Id          : int
            Name        : string
            Folder      : string
            IndexFile   : string
            StartIndex  : int64
            EndIndex    : int64 }
    
    type internal CloudArrayDescription =
        {   Count    : int64
            Type     : Type
            Segments : List<SegmentDescription> } 
    
    [<AutoOpen>]
    module private Helpers =

        // TODO : Tune

        /// Max segment size in bytes.     
        let MaxSegmentSize = 1024L * 1024L * 1024L 
        /// Max PageCache size in bytes.  
        let MaxPageCacheSize  = 1024L     
        /// Max PageCache items.          
        let MaxPageCacheItems = 1024      

        let newSegment(id, name, folder, descriptor, start, ``end``) =
            { Id         = id
              Name       = name
              Folder     = folder
              IndexFile  = descriptor
              StartIndex = start
              EndIndex   = ``end`` }
    
        let newCloudArrayDescription(ty, count, segments) =
            { Count = count; Type = ty; Segments = segments}
    
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
            } |> onDereferenceError descriptor

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
    
    [<Serializable; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type CloudArray<'T> internal (folder : string, descriptorName : string, count : int64, partitionCount : int, storeId : StoreId) as this =

        let provider  = lazy CloudArrayProvider.GetById storeId  
        let pageCache = lazy provider.Value.GetPageCache(this)

        member internal this.Provider   = provider
        member internal this.StoreId    = storeId

        override this.ToString() = sprintf "cloudarray:%s/%s" folder descriptorName

        member private this.StructuredFormatDisplay = this.ToString()

        internal new(info : SerializationInfo, context : StreamingContext) =
            let folder = info.GetValue("folder", typeof<string>) :?> string
            let descriptor = info.GetValue("descriptor", typeof<string>) :?> string
            let count = info.GetValue("count", typeof<int64>) :?> int64
            let partitionCount = info.GetValue("partitionCount", typeof<int>) :?> int
            let storeId = info.GetValue("storeId", typeof<StoreId>) :?> StoreId
            
            new CloudArray<'T>(folder, descriptor, count, partitionCount, storeId)

        interface ISerializable with
            member this.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("folder", folder)
                info.AddValue("descriptor", descriptorName)
                info.AddValue("count", count)
                info.AddValue("partitionCount", count)
                info.AddValue("storeId", storeId, typeof<StoreId>)

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

            member this.Item
                with get (index: int64): obj = 
                    (this :> ICloudArray<'T>).Item index :> _

            member this.Partitions with get () = partitionCount

            member this.GetPartition(index : int) : obj [] =
                (this :> ICloudArray<'T>).GetPartition(index) 
                |> Seq.cast<obj> // Inefficient
                |> Seq.toArray
                    

        interface ICloudArray<'T> with 
            member this.Item 
                with get (index : int64) : 'T =
                    if index < 0L || index >= count then
                        raise <| IndexOutOfRangeException(sprintf "Index = %d" index)
                    else
                        pageCache.Value.GetItem<'T>(index)

            member this.GetPartition(index : int) : 'T [] =
                if index < 0 || index >= partitionCount then
                    raise <| IndexOutOfRangeException(sprintf "Index = %d, but Partitions = %d" index partitionCount)
                provider.Value.GetPartition(this, index)
                |> Async.RunSynchronously 

            member this.Append(cloudArray : ICloudArray<'T>) : ICloudArray<'T> =
                provider.Value.AppendAsync(this, cloudArray :?> CloudArray<'T>)
                |> Async.RunSynchronously :> _

                     
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

        interface ICloudDisposable with
            member this.Dispose() : Async<unit> =
                provider.Value.DisposeAsync(this)

        member __.RangeAsync<'T>(start : int64, length : int) : Async<'T []> =
            async {
                if start < 0L || length < 0 || start + int64 length > count then
                    return raise <| IndexOutOfRangeException(sprintf "Start = %d, Length = %d" start length) 
                elif count = 0L then
                    return Array.empty
                else
                    return! provider.Value.GetRangeAsync(start, length, this)
            }


    and CloudArrayProvider private (storeId : StoreId, store : ICloudStore) =
        static let providers = new System.Collections.Concurrent.ConcurrentDictionary<StoreId, CloudArrayProvider>()

        let mkCloudArrayId () = Guid.NewGuid().ToString("N") 
        
        let extension = "ca"
        let mkDescriptorName (name : string) = sprintf "%s.%s" name extension
        let mkSegmentName    (name : string) (id : int) = sprintf "%s.%s.segment.%d" name extension id
        let mkIndexFileName  (name : string) (id : int) = sprintf "%s.%s.segment.%d.index" name extension id
    
        let readDescriptor container name = async {
            use! descriptorStream = store.ReadImmutable(container, name)
            return Serialization.DefaultPickler.Deserialize<CloudArrayDescription>(descriptorStream)
        }

        let readArrayDescriptor (ca : CloudArray<'T>) =
            readDescriptor (ca :> ICloudArray).Container (ca :> ICloudArray).Name

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
        
        let getPartitionAsync (ca : ICloudArray<'T>) (index : int) =
            async {
                let! descr = readDescriptor ca.Container ca.Name
                let segment = descr.Segments.[index]
                let count = int(segment.EndIndex - segment.StartIndex + 1L)
                let result = Array.zeroCreate count

                use! segmentStream = store.ReadImmutable(ca.Container, segment.Name)
                for i = 0 to count - 1 do
                    let item = Serialization.DefaultPickler.Deserialize<'T>(segmentStream, leaveOpen = true)
                    result.[i] <- item
                return result
            }


        let defineUntyped(ty : Type, container : string, descriptor : string, count : int64, partitionCount : int) =
            let existential = Existential.Create ty
            let ctor =
                {
                    new IFunc<ICloudArray> with
                        member __.Invoke<'T> () = new CloudArray<'T>(container, descriptor, count, partitionCount, storeId) :> ICloudArray
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
                            let streamPosition = ref 0L

                            let segmentEndCheck () =
                                if streamPosition.Value < MaxSegmentSize then 
                                    moveNext := e.MoveNext()
                                    !moveNext
                                else
                                    false

                            while segmentEndCheck() do
                                let item = e.Current
                                bw.Write(streamPosition.Value)
                                use ms = new MemoryStream()
                                Serialization.DefaultPickler.Serialize(ty, ms, item, leaveOpen = true)
                                streamPosition := streamPosition.Value + ms.Length
                                ms.Position <- 0L
                                do! Async.AwaitTask(ms.CopyToAsync(stream))
                                //Serialization.DefaultPickler.Serialize(ty, stream, item, leaveOpen = true)
                                incr segmentItems
                            if not !moveNext then
                                sourceEnd := true
                            bw.Flush()
                            segmentStream.Dispose()
                        }
                    do! store.CreateImmutable(folder, segmentIndexFile, serialize' , asFile = true)
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

            let arrayDescription = newCloudArrayDescription(ty, !segmentStart, segmentsDescription)

            let writeSegmentsDescription (stream : Stream) =
                async {
                    Serialization.DefaultPickler.Serialize<CloudArrayDescription>(stream, arrayDescription) |> ignore
                    stream.Dispose()
                }

            do! store.CreateImmutable(folder, descriptorName, writeSegmentsDescription, true)

            return defineUntyped(ty, folder, descriptorName, !segmentStart, arrayDescription.Segments.Count)
        }

        let appendAsync (left : CloudArray<'T>) (right : CloudArray<'T>) : Async<CloudArray<'T>> = async {
            if left.StoreId <> right.StoreId then
                failwithf "StoreId mismatch %A, %A" left.StoreId right.StoreId


            let! leftDescr  = readArrayDescriptor left
            let! rightDescr = readArrayDescriptor right

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
            let finalDescr = newCloudArrayDescription(typeof<'T>, leftDescr.Count + rightDescr.Count, finalSegmentsDescription)
                                              
            // Write
            let guid           = mkCloudArrayId()
            let descriptorName = mkDescriptorName guid

            let serialize stream =
                async {
                    Serialization.DefaultPickler.Serialize<CloudArrayDescription>(stream, finalDescr) |> ignore
                    stream.Dispose()
                }

            do! store.CreateImmutable((left :> ICloudArray).Container, descriptorName , serialize, true)

            return new CloudArray<'T>((left :> ICloudArray).Container, descriptorName, finalDescr.Count, finalDescr.Segments.Count, storeId)
        }

        let disposeAsync (ca : CloudArray<'T>) : Async<unit> =
            async {
                let! descriptor = readArrayDescriptor ca
                do! descriptor.Segments
                    |> Seq.toArray
                    |> Array.map (fun segment -> 
                        async {
                            do! store.Delete(segment.Folder, segment.IndexFile)   
                            do! store.Delete(segment.Folder, segment.Name)
                        } )
                    |> Async.Parallel 
                    |> Async.Ignore
                do! store.Delete((ca :> ICloudArray).Container, (ca :> ICloudArray).Name)
            }

        let getExistingAsync(container : string) (name : string) : Async<ICloudArray> =
            async {
                let! descriptor = readDescriptor container name
                return defineUntyped(descriptor.Type, container, name, descriptor.Count, descriptor.Segments.Count)
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

        member internal __.GetDescription(ca : CloudArray<'T>) : Async<CloudArrayDescription> =
            readArrayDescriptor ca
            |> onDereferenceError ca

        member __.GetRangeAsync<'T>(start : int64, length : int, ca : CloudArray<'T>) : Async<'T []> =
            let ca = ca :> ICloudArray
            getRangeAsync start length ca.Container ca.Name
            |> onDereferenceError ca

        member __.Create(container : string, values : IEnumerable, ty : Type) : Async<ICloudArray> =
            createAsync container values ty
            |> onCreateError container "cloudarray"

        member __.AppendAsync<'T>(left : CloudArray<'T>, right : CloudArray<'T>) : Async<CloudArray<'T>> =
            appendAsync left right

        member __.DisposeAsync<'T>(ca : CloudArray<'T>) : Async<unit> =
            disposeAsync ca
            |> onDeleteError ca

        member __.GetExisting(container, id) = 
            getExistingAsync container id
            |> onGetError container id


        member __.GetPartition<'T>(ca : CloudArray<'T>, index : int) : Async<'T []> =
            getPartitionAsync ca index
            |> onDereferenceError ca

        member __.GetContained(container : string) : Async<ICloudArray []> =
            async {
                let! files = store.EnumerateFiles(container)

                let caIds =
                    files
                    |> Array.filter(fun f -> f.EndsWith <| sprintf' ".%s" extension)

                return!
                    caIds 
                    |> Array.map (fun id -> __.GetExisting(container, id))
                    |> Async.Parallel
            } |> onListError container

