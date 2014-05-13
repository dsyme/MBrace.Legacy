namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils

    type internal CloudFile(id : string, container : string, provider : CloudFileProvider) =

        interface ICloudFile with
            member self.Name = id
            member self.Container = container
            member self.Dispose () =
                (provider :> ICloudFileProvider).Delete(self)

        override self.ToString() = sprintf' "%s - %s" container id

        member __.Provider with get () = provider

        new (info : SerializationInfo, context : StreamingContext) = 
            let id = info.GetString("id")
            let container = info.GetString("container")
            let storeId = info.GetValue("storeId", typeof<StoreId>) :?> StoreId
            let provider =
                match StoreRegistry.TryGetCoreConfiguration storeId with
                | None -> raise <| new MBraceException(sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName)
                | Some config -> config.CloudFileProvider :?> CloudFileProvider

            new CloudFile(id, container, provider)
        
        interface ISerializable with 
            member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)
                info.AddValue("storeId", provider.StoreId, typeof<StoreId>)


    and
     [<Serializable>]
     [<StructuredFormatDisplay("{StructuredFormatDisplay}")>] 
     internal CloudFileSeq<'T> (file : ICloudFile, reader:(Stream -> Async<obj>)) =

        let provider = (file :?> CloudFile).Provider

        override this.ToString () = sprintf' "cloudfile:%s/%s" file.Container file.Name

        member private this.StructuredFormatDisplay = this.ToString()

        interface IEnumerable with
            member this.GetEnumerator () = 
                let s = (provider :> ICloudFileProvider).Read(file, reader) |> Async.RunSynchronously :?> IEnumerable
                s.GetEnumerator()

        interface IEnumerable<'T> with
            member this.GetEnumerator () = 
                let s = (provider :> ICloudFileProvider).Read(file, reader) |> Async.RunSynchronously :?> IEnumerable<'T> 
                s.GetEnumerator()
            
        interface ISerializable with
            member this.GetObjectData (info : SerializationInfo , context : StreamingContext) =
                info.AddValue ("file", file)
                info.AddValue ("reader", reader)

        new (info : SerializationInfo , context : StreamingContext) =
            CloudFileSeq(info.GetValue("file", typeof<ICloudFile> ) :?> ICloudFile, 
                         info.GetValue ("reader", typeof<Stream -> Async<obj>>) :?> Stream -> Async<obj>)
    
    and internal CloudFileProvider (storeInfo : StoreInfo, cache : LocalCacheStore) =

        let store = storeInfo.Store

        member this.Exists(container) : Async<bool> =
            store.Exists(container)

        member this.Exists(container, id) : Async<bool> =
            store.Exists(container,id)

        member __.StoreId = storeInfo.Id

        interface ICloudFileProvider with
            override this.CreateNew(container : Container, id : Id, writer : (Stream -> Async<unit>)) : Async<ICloudFile> =
                async {
                    do! cache.Create(container, id, writer)
                    do! cache.Commit(container, id, asFile = true)

                    return CloudFile(id, container, this) :> _
                }

            override this.CreateExisting (container, id) : Async<ICloudFile> =
                async {
                    let! exists = store.Exists(container, id) 
                    if exists then 
                        return CloudFile(id, container, this) :> _
                    else 
                        return failwith "File does not exist"
                }

            override this.Read(file : ICloudFile, deserializer) : Async<'T> =
                async {
                    let! stream = cache.Read(file.Container, file.Name)
                    return! deserializer stream
                }

            override this.ReadAsSequence(file : ICloudFile, deserializer, ty) : Async<IEnumerable> =
                async {
                    let cloudFileSeqTy = typedefof<CloudFileSeq<_>>.MakeGenericType [| ty |]
                    let cloudFileSeq = Activator.CreateInstance(cloudFileSeqTy, [| file :> obj; deserializer :> obj |])
                    return cloudFileSeq :?> _
                }

            override this.GetContainedFiles(container) : Async<ICloudFile []> =
                async {
                    let! files = store.GetFiles(container)
                    return files |> Array.map (fun name -> CloudFile(name, container, this) :> _)
                }
                
            
            override this.Delete(cfile : ICloudFile) : Async<unit> =
                store.Delete(cfile.Container, cfile.Name)

    and private CloudFileReader =
        static member private GetEnumerator<'T> (sr : StreamReader, deserializer : unit -> obj) : IEnumerator<'T> =
            let curr = ref Unchecked.defaultof<'T>
            { new IEnumerator<'T> with
                member this.Current = !curr
                member this.Current = !curr :> obj
                member this.Dispose () = sr.Dispose()
                member this.MoveNext () = 
                    if not sr.EndOfStream
                    then curr := deserializer() :?> 'T; true 
                    else false
                member this.Reset () = 
                    curr := Unchecked.defaultof<'T>
                    sr.BaseStream.Position <- 0L
                    sr.DiscardBufferedData()
            }
        
//        static member ReadLines (stream : Stream) =
//            let sr = new StreamReader(stream)
//            CloudFileReader.GetEnumerator<string>(sr, (fun () -> sr.ReadLine() :> obj))
//
//        static member ReadBytes (stream : Stream) =
//            let curr = ref 0
//            { new IEnumerator<byte> with
//                member this.Current = byte !curr :> obj
//                member this.Current = byte !curr 
//                member this.Dispose () = stream.Dispose()
//                member this.MoveNext () = 
//                    curr := stream.ReadByte()
//                    !curr <> -1
//                member this.Reset () = 
//                    curr := 0
//                    stream.Position <- 0L
//            }
