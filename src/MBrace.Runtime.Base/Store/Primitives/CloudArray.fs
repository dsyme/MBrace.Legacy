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

    /// A CloudArray partition.
    type Partition = 
        { StartIndex : int64
          EndIndex : int64
          Container : string
          Name : string }

    /// CloudArray description.
    type Descriptor = 
        { Length : int64
          Type : Type
          Partitions : Partition [] }

    /// Helper type to partition a seq<'T> to seq<seq<'T>> using a predicate
    type InnerEnumerator(predicate : unit -> bool, e : IEnumerator) = 
        let mutable sourceMoveNext = ref true
        member __.SourceMoveNext = sourceMoveNext.Value
    
        interface IEnumerator with
            member x.Current : obj = e.Current 
        
            member x.MoveNext() : bool = 
                if predicate() then 
                    sourceMoveNext := e.MoveNext()
                    sourceMoveNext.Value
                else false

            member x.Reset() : unit = invalidOp "Reset"

        member this.ToSeq() = { new IEnumerable with member __.GetEnumerator() : IEnumerator = this :> _ }

    type InnerEnumerator<'T>(predicate : unit -> bool, e : IEnumerator<'T>) = 
        inherit InnerEnumerator(predicate, e) 

        interface IEnumerator<'T> with
            member x.Current : 'T = e.Current
            member x.Dispose() : unit = ()
    
        member this.ToSeq() = 
            { new IEnumerable<'T> with
                  member __.GetEnumerator() : IEnumerator = this :> _
                  member __.GetEnumerator() : IEnumerator<'T> = this :> _ }

    [<Serializable; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type CloudArray<'T> internal (id : string, container : string, storeId : StoreId) as self = 
        let provider = lazy CloudArrayProvider.GetById storeId
        let descriptor = lazy(provider.Value.GetDescriptor(self) |> Async.RunSynchronously)

        member private this.Descriptor = descriptor
        member private this.StructuredFormatDisplay = this.ToString()
        override this.ToString() = sprintf "cloudarray:%s/%s" container id
        
        interface IEnumerable<'T> with
            member this.GetEnumerator() : Collections.IEnumerator = (this :> IEnumerable<'T>).GetEnumerator() :> _
            member this.GetEnumerator() : IEnumerator<'T> = provider.Value.GetEnumerator<'T>(this) |> Async.RunSynchronously
        
        interface ICloudArray<'T> with
            member this.Name = id
            member this.Container = container
            member this.Length = descriptor.Value.Length
            member this.Type = typeof<'T>
            member this.Partitions = descriptor.Value.Partitions.Length
            member this.GetPartition(index : int) = 
                provider.Value.GetPartition(this, index) |> Async.RunSynchronously
            member this.Item 
                with get (index : int64) = provider.Value.GetElement(this, index) |> Async.RunSynchronously
            member left.Append(right : ICloudArray<'T>) : ICloudArray<'T> = 
                provider.Value.Append(left, right :?> CloudArray<'T>) 
                |> Async.RunSynchronously :> _
        
        interface ICloudDisposable with
            member this.Dispose() = provider.Value.Dispose(this)
            member this.GetObjectData(info : Runtime.Serialization.SerializationInfo, 
                                      context : Runtime.Serialization.StreamingContext) : unit = 
                info.AddValue("id", (this :> ICloudArray<'T>).Name)
                info.AddValue("container", (this :> ICloudArray<'T>).Container)
                info.AddValue("storeId", storeId, typeof<StoreId>)
        
        new(info : SerializationInfo, context : StreamingContext) = 
            let id = info.GetString "id"
            let container = info.GetString "container"
            let storeId = info.GetValue("storeId", typeof<StoreId>) :?> StoreId
            new CloudArray<'T>(id, container, storeId)

    and CloudArrayProvider private (storeId : StoreId, store : ICloudStore, fsCache : CacheStore) =
        static let providers = new System.Collections.Concurrent.ConcurrentDictionary<StoreId, CloudArrayProvider>()

        /// Max segment size in bytes.     
        let maxPartitionSize = 1024L * 1024L * 1024L 

        let extension = "ca"
        let mkDescriptorName (name : string) = sprintf "%s.%s" name extension
        let mkPartitionName  (name : string) (id : int) = sprintf "%s.%s.partition.%d" name extension id

        let defineUntyped(ty : Type, id : string, container : string) =
            let existential = Existential.Create ty
            let ctor =
                {
                    new IFunc<ICloudArray> with
                        member __.Invoke<'T> () = new CloudArray<'T>(id, container, storeId) :> ICloudArray
                }

            existential.Apply ctor

        let createDescriptor(container, id, d) =
            async {
                let serializeDescriptor d stream = async {
                    Serialization.DefaultPickler.Serialize<Descriptor>(stream, d)
                }
                do! store.CreateImmutable(container, mkDescriptorName id, serializeDescriptor d, true)
            }

        static member internal Create(storeId : StoreId, store : ICloudStore, fscache : CacheStore) =
            providers.GetOrAdd(storeId, fun id -> new CloudArrayProvider(storeId, store, fscache))

        static member internal GetById(storeId : StoreId) : CloudArrayProvider =
            let ok, provider = providers.TryGetValue storeId
            if ok then provider
            else
                raise <| new MBraceException(sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName)

        member __.Create(container : string, id : string, source : IEnumerable, ty : Type) : Async<ICloudArray> =
            async {
                let currentStream = ref Unchecked.defaultof<Stream>
                let totalCount = ref 0L
                let startIndex = ref 0L
                let partitions = new ResizeArray<Partition>()
                let predicate () = currentStream.Value.Position < maxPartitionSize
                
                let serializePartition name values stream = async {
                    currentStream := stream
                    let count = Serialization.DefaultPickler.SerializeSequence(ty, stream, values, leaveOpen = false)
                    partitions.Add({ StartIndex = !startIndex; EndIndex = !startIndex + int64 count - 1L; Container = container; Name = name })
                    startIndex := !startIndex + int64 count
                    totalCount := !totalCount + int64 count
                }

                let partitioned : IEnumerable<IEnumerable> =
                    let sourceEnumerator = source.GetEnumerator()
                    let e = new InnerEnumerator(predicate, sourceEnumerator)
                    let rec aux _ = seq { 
                        if e.SourceMoveNext then
                            yield e.ToSeq()
                            yield! aux ()
                    }
                    aux ()

                let pid = ref 0
                for xs in partitioned do
                    let name = mkPartitionName id !pid
                    incr pid
                    do! fsCache.Create(container, name, serializePartition name xs, true)

                let d = { Length = !totalCount; Type = ty; Partitions = partitions.ToArray() }
                do! createDescriptor(container, id, d)
                return defineUntyped(ty, container, id)
            } |> onCreateError container id


        member __.GetDescriptor(container, id) : Async<Descriptor> =
            async {
                let! stream = store.ReadImmutable(container, mkDescriptorName id)
                return Serialization.DefaultPickler.Deserialize<Descriptor>(stream)
            }
            |> onGetError container id

        member __.GetDescriptor<'T>(ca : CloudArray<'T>) : Async<Descriptor> =
            async {
                let ca = ca :> ICloudArray<'T>
                return! __.GetDescriptor(ca.Container, mkDescriptorName ca.Name)
            } |> onDereferenceError ca

        member __.Append<'T>(left : CloudArray<'T>, right : CloudArray<'T>) : Async<CloudArray<'T>> =
            async {
                let! leftD  = __.GetDescriptor(left)
                let! rightD = __.GetDescriptor(right)
                let id = Guid.NewGuid().ToString() // huh?
                let container = (left :> ICloudArray).Container // huh?
                let partitions = Array.append leftD.Partitions rightD.Partitions
                let descriptor = { Length = leftD.Length + rightD.Length; Type = typeof<'T>; Partitions = partitions}
                do! createDescriptor(container, id, descriptor)
                return new CloudArray<'T>(id, container, storeId)
            } 

        member __.Dispose<'T>(ca : CloudArray<'T>) : Async<unit> =
            async {
                let! descriptor = __.GetDescriptor(ca)
                for p in descriptor.Partitions do
                    do! store.Delete(p.Container, p.Name)
                let ca = ca :> ICloudArray<'T>
                do! store.Delete(ca.Container, mkDescriptorName ca.Name)
            } |> onDeleteError (ca.ToString())

        member __.GetExisting(container, id) : Async<ICloudArray> = 
            async {
                let! descriptor = __.GetDescriptor(container, id)
                return defineUntyped(descriptor.Type, id, container)
            } |> onGetError container id

        member __.GetElement<'T>(ca : CloudArray<'T>, index : int64) : Async<'T> =
            async {
                let! d = __.GetDescriptor(ca)
                let partitions = d.Partitions
                let i, partition = 
                    partitions
                    |> Seq.mapi (fun i e -> i,e)
                    |> Seq.find (fun (_,p) -> p.StartIndex <= index && index <= p.EndIndex) 
                let relativeIndex = int (index - partition.StartIndex)
                let! p = __.GetPartition(ca, i)
                return p.[relativeIndex]
            } |> onDereferenceError ca

        member __.GetPartition<'T>(ca : CloudArray<'T>, index : int) : Async<'T []> =
            async {
                let! d = __.GetDescriptor(ca)
                let p = d.Partitions.[index]
                use! stream = store.ReadImmutable(p.Container, p.Name)
                return Serialization.DefaultPickler.DeserializeSequence<'T>(stream)
                       |> Seq.toArray
            } |> onDereferenceError ca

        member __.GetEnumerator<'T>(ca : CloudArray<'T>) : Async<IEnumerator<'T>> =
            async {
                let s =
                    {0..(ca :> ICloudArray<'T>).Partitions-1}
                    |> Seq.collect (fun i -> __.GetPartition(ca,i) |> Async.RunSynchronously)
                return s.GetEnumerator()
            } |> onDereferenceError ca

        member __.GetContained(container : string) : Async<ICloudArray []> =
            async {
                let! files = store.EnumerateFiles(container)

                let caIds =
                    files
                    |> Array.choose (fun f ->
                        if f.EndsWith <| sprintf ".%s" extension then 
                            Some <| f.Substring(0, f.Length - extension.Length - 1)
                        else
                            None)
                return!
                    caIds 
                    |> Array.map (fun id -> __.GetExisting(container, id))
                    |> Async.Parallel
            } |> onListError container

