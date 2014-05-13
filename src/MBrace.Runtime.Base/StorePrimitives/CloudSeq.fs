namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.FsPickler

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils

    type internal CloudSeqInfo = { Size : int64; Count : int; Type : Type }

    // TODO: CLOUDSEQINFO CTOR

    [<Serializable>]
    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>] 
    type internal CloudSeq<'T> (id : string, container : string, provider : CloudSeqProvider) as this =

        let info = lazy (Async.RunSynchronously <| provider.GetCloudSeqInfo(this))

        interface ICloudSeq with
            member this.Name = id
            member this.Container = container
            member this.Type = info.Value.Type
            member this.Count = info.Value.Count
            member this.Size = info.Value.Size
            member this.Dispose () =
                (provider :> ICloudSeqProvider).Delete(this)

        interface ICloudSeq<'T>

        override this.ToString() = sprintf' "cloudseq:%s/%s" container id

        member private this.StructuredFormatDisplay = this.ToString()

        interface IEnumerable with
            member this.GetEnumerator () = 
                provider.GetEnumerator(this)
                |> Async.RunSynchronously :> IEnumerator
        
        interface IEnumerable<'T> with
            member this.GetEnumerator () = 
                provider.GetEnumerator(this)  
                |> Async.RunSynchronously
            
        interface ISerializable with
            member this.GetObjectData (info : SerializationInfo , context : StreamingContext) =
                info.AddValue ("id", (this :> ICloudSeq<'T>).Name)
                info.AddValue ("container", (this :> ICloudSeq<'T>).Container)
                info.AddValue("storeId", provider.StoreId, typeof<StoreId>)

        new (info : SerializationInfo , context : StreamingContext) =
            let id        = info.GetString "id"
            let container = info.GetString "container"
            let storeId   = info.GetValue( "storeId", typeof<StoreId>) :?> StoreId

            let provider =
                match StoreRegistry.TryGetCoreConfiguration storeId with
                | None -> raise <| new MBraceException(sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName)
                | Some config -> config.CloudSeqProvider :?> CloudSeqProvider

            new CloudSeq<'T>(id, container, provider)
    
    and internal CloudSeqProvider (storeInfo : StoreInfo, cacheStore : LocalCacheStore) as this = 

        let pickler = Nessos.MBrace.Runtime.Serializer.Pickler
        let store = storeInfo.Store
        let extension = "seq"
        let postfix = fun s -> sprintf' "%s.%s" s extension

        let getInfo (stream : Stream) : CloudSeqInfo =
            let pos = stream.Position
            stream.Seek(int64 -sizeof<int>, SeekOrigin.End) |> ignore
            let br = new BinaryReader(stream)
            let headerSize = br.ReadInt32()
            stream.Seek(int64 -sizeof<int> - int64 headerSize, SeekOrigin.End) |> ignore
            
            let count = br.ReadInt32()
            let ty = pickler.Deserialize<Type> stream
            let size = br.ReadInt64()

            stream.Position <- pos
            { Count = count; Size = size; Type = ty }

        let setInfo (stream : Stream) (info : CloudSeqInfo) =
            let bw = new BinaryWriter(stream)
            let headerStart = stream.Position
            
            bw.Write(info.Count)
            pickler.Serialize(stream, info.Type)
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
            let cloudSeqTy = typedefof<CloudSeq<_>>.MakeGenericType [| ty |]
            let cloudSeq = Activator.CreateInstance(cloudSeqTy,[| id :> obj ; container :> obj; this :> obj |])
            cloudSeq :?> ICloudSeq

        member __.StoreId = storeInfo.Id

        member this.GetCloudSeqInfo (cseq : ICloudSeq) : Async<CloudSeqInfo> =
            getCloudSeqInfo cseq.Container cseq.Name

        member this.GetEnumerator<'T> (cseq : ICloudSeq<'T>) : Async<IEnumerator<'T>> =
            async {
                let cont, id, ty = cseq.Container, postfix cseq.Name, cseq.Type
                let! stream = cacheStore.Read(cont, id)

                let info = getInfo stream

                if info.Type <> ty then
                    let msg = sprintf' "CloudSeq type mismatch. Internal type %s, got %s" info.Type.AssemblyQualifiedName ty.AssemblyQualifiedName
                    return raise <| MBraceException(msg)

                return pickler.DeserializeSequence<'T>(stream, info.Count)
            }

            member this.GetIds (container : string) : Async<string []> =
                async {
                    let! files = store.GetFiles(container)
                    return files
                        |> Seq.filter (fun w -> w.EndsWith <| sprintf' ".%s" extension)
                        |> Seq.map (fun w -> w.Substring(0, w.Length - extension.Length - 1))
                        |> Seq.toArray
                }

        interface ICloudSeqProvider with

            member this.CreateNew<'T>(container, id, values : seq<'T>) = async {
                let serializeTo stream = async {
                    let length = pickler.SerializeSequence(typeof<'T>, stream, values, leaveOpen = true)
                    return setInfo stream { Size = -1L; Count = length; Type = typeof<'T> }
                }
                do! cacheStore.Create(container, postfix id, serializeTo)
                do! cacheStore.Commit(container, postfix id)

                return CloudSeq<'T>(id, container, this) :> ICloudSeq<'T>
            }

            member this.CreateNewUntyped (container : string, id : string, values : IEnumerable, ty : Type) : Async<ICloudSeq> =
                async {
                    let serializeTo stream = async {
                        let length = pickler.SerializeSequence(ty, stream, values, leaveOpen = true)
                        return setInfo stream { Size = -1L; Count = length; Type = ty }
                    }
                    do! cacheStore.Create(container, postfix id, serializeTo)
                    do! cacheStore.Commit(container, postfix id)

                    return defineUntyped(ty, container, id)
                }

            member this.CreateExisting (container, id) = async {
                let! cseqInfo = getCloudSeqInfo container id
                return defineUntyped(cseqInfo.Type, container, id)
            }

            member this.GetContainedSeqs(container : string) : Async<ICloudSeq []> =
                async {
                    let! ids = this.GetIds(container)
                    return 
                        ids |> Seq.map (fun id -> (Async.RunSynchronously(getCloudSeqInfo container id)).Type, container, id)
                            |> Seq.map defineUntyped
                            |> Seq.toArray
                }

            member self.Delete(cseq : ICloudSeq) : Async<unit> = 
                store.Delete(cseq.Container, postfix cseq.Name)
