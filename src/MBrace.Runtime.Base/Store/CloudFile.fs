namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils

    type CloudFile internal (id : string, container : string, provider : CloudFileProvider) =

        member __.Name = id
        member __.Container = container

        interface ICloudFile with
            member self.Name = id
            member self.Size = provider.GetLength self |> Async.RunSynchronously
            member self.Container = container
            member self.Dispose () = provider.Delete self
            member self.Read () = provider.Read self

        override self.ToString() = sprintf' "%s - %s" container id

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
    
    and internal CloudFileProvider (storeInfo : StoreInfo, cache : LocalCacheStore) =

        let store = storeInfo.Store

        member __.StoreId = storeInfo.Id

        member __.Read(file : CloudFile) = cache.Read(file.Container, file.Name)

        member __.Delete(cfile : CloudFile) =
            store.Delete(cfile.Container, cfile.Name)

        member __.GetLength(file : CloudFile) = async {
            use! stream = cache.Read(file.Container, file.Name)
            return stream.Length
        }

        interface ICloudFileProvider with

            override this.Create(container : Container, id : Id, writer : (Stream -> Async<unit>)) : Async<ICloudFile> =
                async {
                    do! cache.Create(container, id, writer, asFile = true)
                    do! cache.Commit(container, id, asFile = true)

                    return CloudFile(id, container, this) :> _
                }

            override this.GetExisting (container, id) : Async<ICloudFile> =
                async {
                    let! exists = store.Exists(container, id) 
                    if exists then 
                        return CloudFile(id, container, this) :> _
                    else 
                        return failwith "File does not exist"
                }

            override this.GetContainedFiles(container) : Async<ICloudFile []> =
                async {
                    let! files = store.GetAllFiles(container)
                    return files |> Array.map (fun name -> CloudFile(name, container, this) :> _)
                }