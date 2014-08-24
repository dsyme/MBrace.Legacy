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

    type CloudFile internal (id : string, container : string, storeId : StoreId) =

        let provider : Lazy<CloudFileProvider> = lazy CloudFileProvider.GetById storeId

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
    
    and CloudFileProvider private (storeId : StoreId, store : ICloudStore, cache : CacheStore) =

        static let providers = new System.Collections.Concurrent.ConcurrentDictionary<StoreId, CloudFileProvider> ()

        static member internal Create (storeId : StoreId, store : ICloudStore, cache : CacheStore) =
            providers.GetOrAdd(storeId, fun id -> new CloudFileProvider(id, store, cache))

        static member internal GetById (storeId : StoreId) =
            let ok, provider = providers.TryGetValue storeId
            if ok then provider
            else
                let msg = sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName
                raise <| new StoreException(msg)

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

        member this.Create(container : string, id : string, writer : (Stream -> Async<unit>)) : Async<ICloudFile> =
            async {
                do! cache.Create(container, id, writer, asFile = true)
                return CloudFile(id, container, storeId) :> ICloudFile
            } |> onCreateError container id

        member this.GetExisting (container, id) : Async<ICloudFile> =
            async {
                let! exists = store.Exists(container, id) 
                if exists then 
                    return CloudFile(id, container, storeId) :> ICloudFile
                else 
                    return raise <| NonExistentObjectStoreException(container, id)
            } |> onGetError container id

        member this.GetContainedFiles(container) : Async<ICloudFile []> =
            async {
                let! files = store.EnumerateFiles(container)
                return files |> Array.map (fun name -> CloudFile(name, container, storeId) :> ICloudFile)
            } |> onListError container