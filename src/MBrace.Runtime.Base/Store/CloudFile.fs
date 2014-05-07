namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils

    type CloudFile(id : string, container : string) =

        let fileStoreLazy = lazy IoC.Resolve<ICloudFileStore>()

        interface ICloudFile with
            member self.Name = id
            member self.Container = container
            member self.Dispose () =
                fileStoreLazy.Value.Delete(container, id)

        override self.ToString() = sprintf' "%s - %s" container id

        new (info : SerializationInfo, context : StreamingContext) = 
                CloudFile(info.GetValue("id", typeof<string>) :?> string,
                            info.GetValue("container", typeof<string>) :?> string)
        
        interface ISerializable with 
            member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)


    [<Serializable>]
    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>] 
    type internal CloudFileSeq<'T> (file : ICloudFile, reader:(Stream -> Async<obj>)) =
        let factoryLazy = lazy IoC.Resolve<ICloudFileStore>()

        override this.ToString () = sprintf' "%s - %s" file.Container file.Name

        member private this.StructuredFormatDisplay = this.ToString()

        interface IEnumerable with
            member this.GetEnumerator () = 
                let s = factoryLazy.Value.Read(file, reader) |> Async.RunSynchronously :?> IEnumerable
                s.GetEnumerator()

        interface IEnumerable<'T> with
            member this.GetEnumerator () = 
                let s = factoryLazy.Value.Read(file, reader) |> Async.RunSynchronously :?> IEnumerable<'T> 
                s.GetEnumerator()
            
        interface ISerializable with
            member this.GetObjectData (info : SerializationInfo , context : StreamingContext) =
                info.AddValue ("file", file)
                info.AddValue ("reader", reader)

        new (info : SerializationInfo , context : StreamingContext) =
            CloudFileSeq(info.GetValue("file", typeof<ICloudFile> ) :?> ICloudFile, 
                         info.GetValue ("reader", typeof<Stream -> Async<obj>>) :?> Stream -> Async<obj>)

    type CloudFileStore (store : IStore, cache : LocalCacheStore) =
        //let cache = lazy IoC.TryResolve<LocalCacheStore>("cacheStore")

        interface ICloudFileStore with
            override this.Create(container : Container, id : Id, serialize : (Stream -> Async<unit>)) : Async<ICloudFile> =
                async {
//                    match cache.Value with
//                    | None -> 
//                        do! store.Create(container, id, serialize, true)
//                    | Some cache -> 
                    do! cache.Create(container, id, serialize)
                    do! cache.Commit(container, id, asFile = true)

                    return CloudFile(id, container) :> _
                }

            override this.Read(file : ICloudFile, deserialize) : Async<obj> =
                async {
                    let! stream = cache.Read(file.Container, file.Name)
//                        match cache.Value with
//                        | None       -> store.Read(file.Container, file.Name)
//                        | Some cache -> cache.Read(file.Container, file.Name)

                    return! deserialize stream
                }

            override this.ReadAsSeq(file : ICloudFile, deserialize, ty) : Async<obj> =
                async {
                    let cloudFileSeqTy = typedefof<CloudFileSeq<_>>.MakeGenericType [| ty |]
                    let cloudFileSeq = Activator.CreateInstance(cloudFileSeqTy, [| file :> obj; deserialize :> obj |])
                    return cloudFileSeq
                }

            override this.GetFiles(container) : Async<ICloudFile []> =
                async {
                    let! files = store.GetFiles(container)
                    return files |> Array.map (fun name -> CloudFile(name, container) :> _)
                }

            override this.GetFile (container, id) : Async<ICloudFile> =
                async {
                    let! exists = store.Exists(container, id) 
                    if exists then 
                        return CloudFile(id, container) :> _
                    else 
                        return failwith "File does not exist"
                }
                
            override this.Exists(container) : Async<bool> =
                store.Exists(container)

            override this.Exists(container, id) : Async<bool> =
                store.Exists(container,id)
            
            override this.Delete(container, id) : Async<unit> =
                store.Delete(container,id)

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