namespace Nessos.MBrace.Runtime

    open System
    open System.IO
    open System.Reflection
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.FsPickler

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime.StoreUtils

    type internal CloudSeqInfo = { Size : int64; Count : int; Type : Type }

    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>]
    type CloudSeq<'T> internal (id : string, container : string, storeId : StoreId) as self =

        let provider : Lazy<CloudSeqProvider> = lazy CloudSeqProvider.GetById storeId  

        let mutable info = None
        let getInfoLazy () =
            match info with
            | Some i -> i
            | None ->
                let i = provider.Value.GetCloudSeqInfo self |> Async.RunSynchronously
                info <- Some i
                i

        member __.Name = id
        member __.Container = container

        override this.ToString() = sprintf' "cloudseq:%s/%s" container id

        member private this.StructuredFormatDisplay = this.ToString()

        interface ICloudSeq<'T> with
            member this.Name = id
            member this.Container = container
            member this.Type = typeof<'T>
            member this.Count = getInfoLazy().Count
            member this.Size = getInfoLazy().Size
            member this.Dispose () = provider.Value.Delete this

        interface IEnumerable with
            member this.GetEnumerator () = 
                provider.Value.GetEnumerator(this)
                |> Async.RunSynchronously :> IEnumerator

        interface IEnumerable<'T> with
            member this.GetEnumerator () = 
                provider.Value.GetEnumerator(this)  
                |> Async.RunSynchronously
            
        interface ISerializable with
            member this.GetObjectData (info : SerializationInfo , context : StreamingContext) =
                info.AddValue("id", (this :> ICloudSeq<'T>).Name)
                info.AddValue("container", (this :> ICloudSeq<'T>).Container)
                info.AddValue("storeId", storeId, typeof<StoreId>)

        new (info : SerializationInfo , context : StreamingContext) =
            let id        = info.GetString "id"
            let container = info.GetString "container"
            let storeId   = info.GetValue ("storeId", typeof<StoreId>) :?> StoreId

            new CloudSeq<'T>(id, container, storeId)
    
    and CloudSeqProvider private (storeId : StoreId, store : ICloudStore, cacheStore : LocalCache) =

        static let extension = "seq"
        static let postfix s = sprintf' "%s.%s" s extension

        static let providers = new System.Collections.Concurrent.ConcurrentDictionary<StoreId, CloudSeqProvider>()

        let getInfo (stream : Stream) : CloudSeqInfo =
            let pos = stream.Position
            stream.Seek(int64 -sizeof<int>, SeekOrigin.End) |> ignore
            let br = new BinaryReader(stream)
            let headerSize = br.ReadInt32()
            stream.Seek(int64 -sizeof<int> - int64 headerSize, SeekOrigin.End) |> ignore
            
            let count = br.ReadInt32()
            let ty = Serialization.DefaultPickler.Deserialize<Type>(stream, leaveOpen = true)
            let size = br.ReadInt64()

            stream.Position <- pos
            { Count = count; Size = size; Type = ty }

        let setInfo (stream : Stream) (info : CloudSeqInfo) =
            let bw = new BinaryWriter(stream)
            let headerStart = stream.Position
            
            bw.Write(info.Count)
            Serialization.DefaultPickler.Serialize(stream, info.Type, leaveOpen = true)
            bw.Write(stream.Position + 2L * int64 sizeof<int>)
            
            let headerEnd = stream.Position
            bw.Write(int(headerEnd - headerStart))

        let getCloudSeqInfo cont id = 
            async {
                let id = postfix id
                use! stream = cacheStore.Read(cont,id)
                return getInfo stream
            }

        let defineUntyped(ty : Type, container : string, id : string) =
            let existential = Existential.Create ty
            let ctor =
                {
                    new IFunc<ICloudSeq> with
                        member __.Invoke<'T> () = new CloudSeq<'T>(id, container, storeId) :> ICloudSeq
                }

            existential.Apply ctor

        static member internal Create (storeId : StoreId, store : ICloudStore, cacheStore : LocalCache) =
            providers.GetOrAdd(storeId, fun id -> new CloudSeqProvider(id, store, cacheStore))

        static member internal GetById (storeId : StoreId) =
            let ok, provider = providers.TryGetValue storeId
            if ok then provider
            else
                let msg = sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName
                raise <| new StoreException(msg)

        member internal __.GetCloudSeqInfo<'T> (cseq : CloudSeq<'T>) : Async<CloudSeqInfo> =
            getCloudSeqInfo cseq.Container cseq.Name
            |> onDereferenceError cseq

        member __.GetEnumerator<'T> (cseq : CloudSeq<'T>) : Async<IEnumerator<'T>> =
            async {
                let cont, id = cseq.Container, postfix cseq.Name
                let! stream = cacheStore.Read(cont, id)

                let info = getInfo stream

                if info.Type <> typeof<'T> then
                    // TODO : include CloudSeq url in message
                    let msg = sprintf' "CloudSeq type mismatch. Internal type '%s', expected '%s'." info.Type.FullName typeof<'T>.FullName
                    return raise <| MBraceException(msg)

                let sequence = Serialization.DefaultPickler.DeserializeSequence<'T>(stream)
                return sequence.GetEnumerator()
            } |> onDereferenceError cseq

        member __.Delete<'T> (cseq : CloudSeq<'T>) : Async<unit> = 
            // TODO : item should be deleted through the cache?
            store.Delete(cseq.Container, postfix cseq.Name)
            |> onDeleteError cseq

        member this.Create<'T>(container, id, values : seq<'T>) = 
            async {
                let serializeTo stream = async {
                    let length = Serialization.DefaultPickler.SerializeSequence(typeof<'T>, stream, values, leaveOpen = true)
                    return setInfo stream { Size = -1L; Count = length; Type = typeof<'T> }
                }
                do! cacheStore.Create(container, postfix id, serializeTo, false)
                return CloudSeq<'T>(id, container, storeId) :> ICloudSeq<'T>
            } |> onCreateError container id

        member this.Create (container : string, id : string, ty : Type, values : IEnumerable) : Async<ICloudSeq> =
            async {
                let serializeTo stream = async {
                    let length = Serialization.DefaultPickler.SerializeSequence(ty, stream, values, leaveOpen = true)
                    return setInfo stream { Size = -1L; Count = length; Type = ty }
                }
                do! cacheStore.Create(container, postfix id, serializeTo, false)
                return defineUntyped(ty, container, id)
            } |> onCreateError container id

        member this.GetExisting (container, id) = 
            async {
                let! cseqInfo = getCloudSeqInfo container id
                return defineUntyped(cseqInfo.Type, container, id)
            } |> onGetError container id

        member this.GetContainedSeqs(container : string) : Async<ICloudSeq []> =
            async {
                let! files = store.GetAllFiles(container)
                    
                // TODO : find a better heuristic?
                let cseqIds =
                    files
                    |> Array.choose (fun f ->
                        if f.EndsWith <| sprintf' ".%s" extension then 
                            Some <| f.Substring(0, f.Length - extension.Length - 1)
                        else
                            None)

                return!
                    cseqIds 
                    |> Array.map (fun id -> this.GetExisting(container, id))
                    |> Async.Parallel
            } |> onListError container
