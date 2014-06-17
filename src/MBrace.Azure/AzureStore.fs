namespace Nessos.MBrace.Azure

    open System
    open System.IO
    open Nessos.MBrace.Runtime.Store
    open Nessos.MBrace.Azure.Common


    type AzureStore (conn) =
        let immutableStore = ImmutableStore(conn)
        let mutableStore = MutableStore(conn)
        let generalStore = GeneralPurpose(conn)

        interface ICloudStore with
            member this.Name = immutableStore.Name
            member this.UUID = conn

            // Immutable
            member this.CreateImmutable(folder, file, serialize, asFile) =
                //let folder = Helpers.prefix folder
                Validation.checkFolder folder
                Validation.checkFile file
                immutableStore.Create(folder, file, serialize, asFile)

            member this.ReadImmutable(folder, file) : Async<Stream> =
                //let folder = Helpers.prefix folder
                immutableStore.Read(folder, file)

            member this.CopyFrom(folder, file, source, asFile) =
                //let folder = Helpers.prefix folder
                Validation.checkFolder folder
                Validation.checkFile file
                immutableStore.CopyFrom(folder, file, source, asFile)

            member this.CopyTo(folder, file, target) =
                //let folder = Helpers.prefix folder
                immutableStore.CopyTo(folder, file, target)

            // Mutable
            member this.CreateMutable(folder, file, serialize) =
                //let folder = Helpers.prefix folder
                Validation.checkFolder folder
                Validation.checkFile file
                mutableStore.Create(folder, file, serialize)

            member this.ReadMutable(folder, file) =
                //let folder = Helpers.prefix folder
                mutableStore.Read(folder, file)

            member this.TryUpdateMutable(folder, file, serialize, etag) : Async<bool * Tag> =
                //let folder = Helpers.prefix folder
                mutableStore.Update(folder, file, serialize, etag)

            member this.ForceUpdateMutable(folder, file, serialize) : Async<Tag> =
                async {
                    //let folder = Helpers.prefix folder
                    let! success, newTag = mutableStore.Update(folder, file, serialize)
                    if success then return newTag
                    else return failwithf "Cannot force update %s - %s" folder file
                }

            // General purpose
            member this.Exists(folder, file) =
                //let folder = Helpers.prefix folder
                generalStore.Exists(folder, file)

            member this.ContainerExists(folder) =
                //let folder = Helpers.prefix folder
                generalStore.Exists(folder)

            member this.GetAllFiles(folder) =
                //let folder = Helpers.prefix folder
                generalStore.GetFiles(folder)

            member this.GetAllContainers () =
                generalStore.GetFolders ()
                //|> Array.map Helpers.removePrefix

            member this.DeleteContainer(folder) =
                //let folder = Helpers.prefix folder
                generalStore.Delete(folder)

            member this.Delete(folder, file) =
                //let folder = Helpers.prefix folder
                async {
                    try do! 
                        mutableStore.Delete(folder, file)
                    with _ -> 
                        do! generalStore.Delete(folder, file)
                }

    type AzureStoreFactory () =
        interface ICloudStoreFactory with
            member this.CreateStoreFromConnectionString (objectStore : string) = 
                AzureStore(objectStore) :> ICloudStore


    [<AutoOpen>]
    module StoreProvider =
        type Nessos.MBrace.Client.StoreProvider with
            static member AzureStore (connectionString : string) =
                Nessos.MBrace.Client.StoreProvider.Define<AzureStoreFactory>(connectionString)