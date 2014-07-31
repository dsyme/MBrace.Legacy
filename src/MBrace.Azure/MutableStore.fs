namespace Nessos.MBrace.Azure

    open System
    open System.IO

    open Microsoft.WindowsAzure.Storage
    open Microsoft.WindowsAzure.Storage.Blob
    open Microsoft.WindowsAzure.Storage.Table

    open Nessos.MBrace.Store
    open Nessos.MBrace.Azure.Common

    type internal MutableStore (account : CloudStorageAccount) =

        let blobClient () = Clients.getBlobClient account
        let tableClient () = Clients.getTableClient account
        let getTable name = tableClient().GetTableReference(name)

        let getReadBlob (folder, file)  = async {
                let container = (blobClient()).GetContainerReference(folder)

                let! exists = Async.AwaitTask(container.ExistsAsync())
                if not exists
                then failwith "Trying to read from non-existent container"
            
                let blob = container.GetBlockBlobReference(file)

                let! exists = Async.AwaitTask(blob.ExistsAsync())

                if not exists
                then failwith "Trying to read from non-existent blob"

                return blob
            }

        let getWriteBlob(folder, file) = async {
                let container = (blobClient()).GetContainerReference(folder)

                do! Async.AwaitTask(container.CreateIfNotExistsAsync())
                    |> Async.Ignore

                return container.GetBlockBlobReference(file)
            }

        let readEntity (folder, file) = async {
                let table = getTable folder
                let retrieveOp = TableOperation.Retrieve<MutableFatEntity>(file, String.Empty)
                let! result = Async.AwaitTask(table.ExecuteAsync(retrieveOp))
                return result.Result, result.Etag  
            }

        let deleteBlob(folder, file) = async {
                try
                    let! blob = getReadBlob(folder, file)
                    do! Async.AwaitTask(blob.DeleteIfExistsAsync())
                        |> Async.Ignore
                with _ -> ()
            }

        member this.Create(folder, file, serialize : Stream -> Async<unit>) : Async<Tag> =
            async {
                let table = getTable folder

                do! Async.AwaitTask(table.CreateIfNotExistsAsync())
                    |> Async.Ignore

                let! isFat = Helpers.isFatEntity serialize

                match isFat with
                | true -> 
                    let! bin = Helpers.toBinary serialize
                    let entity = MutableFatEntity(file, false, null, bin)

                    let insertOp = TableOperation.Insert(entity)
                    let! result = Async.AwaitTask(table.ExecuteAsync(insertOp))
                    return result.Etag
                | false -> 
                    let refName = sprintf "%s.version.%s" file <| Guid.NewGuid().ToString()
                    let! blob = getWriteBlob(folder, refName)
                    use! stream = Async.AwaitTask(blob.OpenWriteAsync())
                    do! serialize stream
                    stream.Dispose()

                    let entity = MutableFatEntity(file, true, refName, Array.empty)
                    let insertOp = TableOperation.Insert(entity)
                    let! result = Async.AwaitTask(table.ExecuteAsync(insertOp))
                    return result.Etag
            }

        member this.Read(folder, file) : Async<Stream * Tag> =
            async {
                let! result, opTag = readEntity(folder, file)
                let result = result :?> MutableFatEntity

                match result.IsReference with
                | false -> 
                    let bin = result.GetPayload()
                    let stream = Helpers.ofBinary bin
                    return stream, opTag
                | true ->
                    let refName = result.Reference
                    let! blob = getReadBlob(folder, refName)
                    let stream = blob.OpenRead()
                    return stream, opTag
            }

        member this.Delete(folder, file) : Async<unit> =
            async {
                let retrieveOp = TableOperation.Retrieve<MutableFatEntity>(file, String.Empty)
                let table = getTable folder
                let! result = Async.AwaitTask(table.ExecuteAsync(retrieveOp))
                let oldRef = (result.Result :?> MutableFatEntity).Reference
                let wasRef = (result.Result :?> MutableFatEntity).IsReference
                (result.Result :?> MutableFatEntity).ETag <- "*"
                let op = TableOperation.Delete(result.Result :?> MutableFatEntity)
                do! Async.AwaitTask(table.ExecuteAsync(op))
                    |> Async.Ignore
                if wasRef then do! deleteBlob(folder, oldRef)                
            }

        member this.Update(folder, file, serialize : Stream -> Async<unit>, ?etag) : Async<bool * Tag> = 
            async {
                let etag = defaultArg etag "*"
                let table = getTable folder

                let retrieveOp = TableOperation.Retrieve<MutableFatEntity>(file, String.Empty)
                let! result = Async.AwaitTask(table.ExecuteAsync(retrieveOp))
            
                if result.Result = null then raise <| Exception(sprintf "Non-existent %A - %A" folder file)
            
                let oldEntity = result.Result :?> MutableFatEntity

                let! isFat = Helpers.isFatEntity serialize

                match isFat with
                | true ->
                    let! bin = Helpers.toBinary serialize
                    let entity = MutableFatEntity(file, false, String.Empty, bin, ETag = etag)

                    let mergeOp = TableOperation.Merge(entity)
                
                    try
                        let! result = Async.AwaitTask(table.ExecuteAsync(mergeOp))
                        if oldEntity.IsReference then
                            do! deleteBlob(folder, oldEntity.Reference)
                        return true, result.Etag
                    with :? AggregateException as e ->
                        match e.InnerException with 
                        | UpdateConditionNotSatisfied _ -> return false, etag
                        | _ -> return raise e

                | false ->

                    if result.Etag <> etag && etag <> "*" then return false, etag
                    else
                        let refName = sprintf "%s.version.%s" file <| Guid.NewGuid().ToString()
                        let! blob = getWriteBlob(folder, refName)
                        use! stream = Async.AwaitTask(blob.OpenWriteAsync())
                        do! serialize stream
                        stream.Dispose()

                        let entity = MutableFatEntity(file, true, refName, Array.empty, ETag = etag)
                        let insertOp = TableOperation.Merge(entity)
                        try
                            let! result = Async.AwaitTask(table.ExecuteAsync(insertOp))
                            if oldEntity.IsReference then 
                                do! deleteBlob(folder, oldEntity.Reference)
                            return true, result.Etag
                        with :? AggregateException as e ->
                            match e.InnerException with
                            | UpdateConditionNotSatisfied _ -> 
                                do! deleteBlob(folder, refName)
                                return false, etag
                            | _ -> return raise e
            }
            