namespace Nessos.MBrace.Core

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic

    open Nessos.FsPickler

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Store
    open Nessos.MBrace.Caching
    
    type CloudSeqStore (store : IStore, cacheStore : LocalCacheStore) = 
        //let cacheStoreLazy = lazy IoC.TryResolve<LocalCacheStore>("cacheStore")

        let pickler = Nessos.MBrace.Runtime.Serializer.Pickler
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
//                    match cacheStoreLazy.Value with
//                    | None       -> store.Read(cont, id)
//                    | Some cache -> cache.Read(cont, id)
                return getInfo stream
            }

        interface ICloudSeqStore with

            // this is wrong: type should not be passed as a parameter: temporary fix
            member this.GetSeq (container, id, ty) = async {
                let cloudSeqTy = typedefof<CloudSeq<_>>.MakeGenericType [| ty |]
                let cloudSeq = Activator.CreateInstance(cloudSeqTy,[| id :> obj ; container :> obj |])
                return cloudSeq :?> ICloudSeq
            }

            member this.GetCloudSeqInfo (cseq) : Async<CloudSeqInfo> =
                getCloudSeqInfo cseq.Container cseq.Name
        
            member this.Create (items : IEnumerable, container : string, id : string, ty : Type) : Async<ICloudSeq> =
                async {
                    let serializeTo stream = async {
                        let length = pickler.SerializeSequence(ty, stream, items, leaveOpen = true)
                        // TODO: move to table storage
                        return setInfo stream { Size = -1L; Count = length; Type = ty }
                    }

//                    match cacheStoreLazy.Value with
//                    | None -> do! store.Create(container, postfix id, serializeTo)
//                    | Some cache -> 
//                        do! cache.Create(container, postfix id, serializeTo)
//                        do! cache.Commit(container, postfix id)
                    do! cacheStore.Create(container, postfix id, serializeTo)
                    do! cacheStore.Commit(container, postfix id)

                    let cloudSeqTy = typedefof<CloudSeq<_>>.MakeGenericType [| ty |]
                    let cloudSeq = Activator.CreateInstance(cloudSeqTy, [| id :> obj; container :> obj |])
                
                    return cloudSeq :?> _
                }
         

            member this.GetEnumerator<'T> (cseq : ICloudSeq<'T>) : Async<IEnumerator<'T>> =
                async {
                    let cont, id, ty = cseq.Container, postfix cseq.Name, cseq.Type
                    let! stream = cacheStore.Read(cont, id)
//                        match cacheStoreLazy.Value with
//                        | None       -> store.Read(cont, id)
//                        | Some cache -> cache.Read(cont, id)

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

            member this.GetSeqs(container : string) : Async<ICloudSeq []> =
                async {
                    let! ids = (this :> ICloudSeqStore).GetIds(container)
                    return 
                        ids |> Seq.map (fun id -> (Async.RunSynchronously(getCloudSeqInfo container id)).Type, container, id)
                            |> Seq.map (fun (t,c,i) ->
                                    let cloudSeqTy = typedefof<CloudSeq<_>>.MakeGenericType [| t |]
                                    let cloudSeq = Activator.CreateInstance(cloudSeqTy, [| i :> obj; c :> obj |])
                                    cloudSeq :?> ICloudSeq)
                            |> Seq.toArray
                }


            member self.Exists(container : string, id : string) : Async<bool> = 
                store.Exists(container, postfix id)

            member self.Exists(container : string) : Async<bool> = 
                store.Exists(container)

            member self.Delete(container : string, id : string) : Async<unit> = 
                store.Delete(container, postfix id)
