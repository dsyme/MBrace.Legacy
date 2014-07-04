namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime.Store.Utils

    type CloudFile internal (id : string, container : string, storeId : StoreId) =

        let provider =
            lazy
                match StoreRegistry.TryGetStoreInfo storeId with
                | None -> raise <| new MBraceException(sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName)
                | Some info -> info.Primitives.CloudFileProvider :?> CloudFileProvider

        member __.Name = id
        member __.Container = container

        interface ICloudFile with
            member self.Name = id
            member self.Size = provider.Value.GetLength self |> Async.RunSynchronously
            member self.Container = container
            member self.Dispose () = provider.Value.Delete self
            member self.Read () = provider.Value.Read(self)

        override self.ToString() = sprintf' "cloudfile:%s/%s" container id

        new (info : SerializationInfo, context : StreamingContext) = 
            let id = info.GetString("id")
            let container = info.GetString("container")
            let storeId = info.GetValue("storeId", typeof<StoreId>) :?> StoreId

            new CloudFile(id, container, storeId)
        
        interface ISerializable with 
            member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)
                info.AddValue("storeId", storeId, typeof<StoreId>)
    
    and internal CloudFileProvider (id : StoreId, store : ICloudStore, cache : LocalCache) =

        member __.StoreId = id

        member __.Read(file : CloudFile) = 
            cache.Read(file.Container, file.Name) |> onDereferenceError file

        member __.Delete(cfile : CloudFile) =
            store.Delete(cfile.Container, cfile.Name)
            |> onDeleteError cfile

        member __.GetLength(file : CloudFile) = 
            async {
                use! stream = cache.Read(file.Container, file.Name)
                return stream.Length
            } |> onLengthError file

        interface ICloudFileProvider with

            override this.Create(container : Container, id : Id, writer : (Stream -> Async<unit>)) : Async<ICloudFile> =
                async {
                    do! cache.Create(container, id, writer, asFile = true)
                    return CloudFile(id, container, this.StoreId) :> ICloudFile
                } |> onCreateError container id

            override this.GetExisting (container, id) : Async<ICloudFile> =
                async {
                    let! exists = store.Exists(container, id) 
                    if exists then 
                        return CloudFile(id, container, this.StoreId) :> ICloudFile
                    else 
                        return raise <| NonExistentObjectStoreException(container, id)
                } |> onGetError container id

            override this.GetContainedFiles(container) : Async<ICloudFile []> =
                async {
                    let! files = store.GetAllFiles(container)
                    return files |> Array.map (fun name -> CloudFile(name, container, this.StoreId) :> ICloudFile)
                } |> onListError container