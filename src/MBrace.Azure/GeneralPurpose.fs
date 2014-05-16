namespace Nessos.MBrace.Azure

    open System
    open System.IO
    open Nessos.MBrace.Runtime.Store
    open Nessos.MBrace.Azure.Common
    open Microsoft.WindowsAzure.Storage
    open Microsoft.WindowsAzure.Storage.Blob
    open Microsoft.WindowsAzure.Storage.Table

    type GeneralPurpose (conn) =

        let blobClient () = Clients.getBlobClient conn
        let tableClient () = Clients.getTableClient conn
        let getTable name = tableClient().GetTableReference(name)
        let immblob  = ImmutableBlobStoreProvider (conn)
        let immtable = ImmutableTableStoreProvider(conn)

        let getReadBlob (folder, file)  =
            let container = (blobClient()).GetContainerReference(folder)

            if container.Exists() |> not 
            then failwith "Trying to read from non-existent container"
            
            let blob = container.GetBlockBlobReference(file)

            if blob.Exists() |> not
            then failwith "Trying to read from non-existent blob"

            blob

        let getWriteBlob(folder, file) =
            let container = (blobClient()).GetContainerReference(folder)
            container.CreateIfNotExists() |> ignore
                
            let blob = container.GetBlockBlobReference(file)
            blob

        let readEntity (folder, file) =
            let table = getTable folder
            let retrieveOp = TableOperation.Retrieve<MutableFatEntity>(file, String.Empty)
            let result = (getTable folder).Execute(retrieveOp)
            result.Result, result.Etag  
        
        member this.Exists(folder, file) =
            async {
                let! b1 = immtable.Exists(folder, file)
                if b1 then return true 
                else return! immblob.Exists(folder, file)   
            }

        member this.Exists(folder) =
            async {
                let! b1 = immtable.Exists(folder)
                if b1 then return true 
                else return! immblob.Exists(folder)
            }

        member this.GetFiles(folder) =
            async {
                let! a1 = immtable.GetFiles(folder)
                let! a2 = immblob.GetFiles(folder)
                return Array.append a1 a2
                       |> (Set.ofSeq >> Array.ofSeq)
            }

        member this.GetFolders () =
            let containers = blobClient().ListContainers() |> Seq.map (fun s -> s.Name)
            //ListContainers(Helpers.containerPrefix, ContainerListingDetails.Metadata) 
            let tables = tableClient().ListTables() |> Seq.map (fun s -> s.Name)
            Seq.append tables containers
            |> (Set.ofSeq >> Array.ofSeq)
            |> async.Return

        member this.Delete(folder) =
            async {                
                do! immblob.Delete(folder)
                do! immtable.Delete(folder)
            }

        member this.Delete(folder, file) =
            async {
                let! b1 = immtable.Exists(folder, file)
                if b1 then do! immtable.Delete(folder, file)
                else
                    let! b2 = immblob.Exists(folder, file) 
                    if b2 then do! immblob.Delete(folder, file)
                    else raise <| ArgumentException(sprintf "Non-existent %s - %s" folder file)
            }