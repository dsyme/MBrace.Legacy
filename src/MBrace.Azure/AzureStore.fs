namespace Nessos.MBrace.Azure

    open System
    open System.IO

    open Nessos.MBrace.Store
    open Nessos.MBrace.Azure.Common

    type AzureStore (conn) =
        
        // Check connection string and connectivity
        do  
            try
                Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(conn) |> ignore
                //(Clients.getBlobClient  conn).GetContainerReference("azurestorecheck").Exists() |> ignore
                //(Clients.getTableClient conn).GetTableReference("azurestorecheck").Exists() |> ignore
            with ex ->
                raise <| new Exception("Failed to create AzureStore", ex)
        
        let immutableStore = ImmutableStore(conn)
        let mutableStore = MutableStore(conn)
        let generalStore = GeneralPurpose(conn)

        interface ICloudStore with
            member this.Name = immutableStore.Name
            member this.UUID = conn

            // Immutable
            member this.CreateImmutable(folder, file, serialize, asFile) =
                Validation.checkFolder folder
                Validation.checkFile file
                immutableStore.Create(folder, file, serialize, asFile)

            member this.ReadImmutable(folder, file) : Async<Stream> =
                immutableStore.Read(folder, file)

            member this.CopyFrom(folder, file, source, asFile) =
                Validation.checkFolder folder
                Validation.checkFile file
                immutableStore.CopyFrom(folder, file, source, asFile)

            member this.CopyTo(folder, file, target) =
                immutableStore.CopyTo(folder, file, target)

            // Mutable
            member this.CreateMutable(folder, file, serialize) =
                Validation.checkFolder folder
                Validation.checkFile file
                mutableStore.Create(folder, file, serialize)

            member this.ReadMutable(folder, file) =
                mutableStore.Read(folder, file)

            member this.TryUpdateMutable(folder, file, serialize, etag) : Async<bool * Tag> =
                mutableStore.Update(folder, file, serialize, etag)

            member this.ForceUpdateMutable(folder, file, serialize) : Async<Tag> =
                async {
                    let! success, newTag = mutableStore.Update(folder, file, serialize)
                    if success then return newTag
                    else return failwithf "Cannot force update %s - %s" folder file
                }

            // General purpose
            member this.Exists(folder, file) =
                generalStore.Exists(folder, file)

            member this.ContainerExists(folder) =
                generalStore.Exists(folder)

            member this.GetAllFiles(folder) =
                generalStore.GetFiles(folder)

            member this.GetAllContainers () =
                generalStore.GetFolders ()

            member this.DeleteContainer(folder) =
                async {
                    try
                        do! generalStore.Delete(folder)
                    with ex ->
                        raise <| Exception(sprintf "Cannot delete container %s" folder, ex)
                }

            member this.Delete(folder, file) =
                async {
                    try
                        do! generalStore.Delete(folder, file)
                    with ex ->
                        raise <| Exception(sprintf "Cannot delete %s - %s" folder file, ex)
                }

    type AzureStoreFactory () =
        interface ICloudStoreFactory with
            member this.CreateStoreFromConnectionString (objectStore : string) = 
                AzureStore(objectStore) :> ICloudStore


    [<AutoOpen>]
    module StoreProvider =
        type StoreDefinition with
            static member AzureStore (connectionString : string) =
                StoreDefinition.Create<AzureStoreFactory>(connectionString)