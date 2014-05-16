namespace Nessos.MBrace.Azure

    open System
    open System.IO
    open Nessos.MBrace.Azure
    open Nessos.MBrace.Azure.Common
    open Nessos.MBrace.Runtime.Store
    open Microsoft.WindowsAzure.Storage
    open Microsoft.WindowsAzure.Storage.Blob
    open Microsoft.WindowsAzure.Storage.Table

    type MutableStore (conn) =

        let blobClient () = Clients.getBlobClient conn
        let tableClient () = Clients.getTableClient conn
        let getTable name = tableClient().GetTableReference(name)

        let getReadBlob (folder, file)  = async {
                let container = (blobClient()).GetContainerReference(folder)

                let! exists = Async.FromBeginEndCancellable(container.BeginExists, container.EndExists)
                if not exists
                then failwith "Trying to read from non-existent container"
            
                let blob = container.GetBlockBlobReference(file)

                let! exists = Async.FromBeginEndCancellable(blob.BeginExists, blob.EndExists)

                if not exists
                then failwith "Trying to read from non-existent blob"

                return blob
            }

        let getWriteBlob(folder, file) = async {
                let container = (blobClient()).GetContainerReference(folder)

                do! Async.FromBeginEndCancellable(container.BeginCreateIfNotExists, container.EndCreateIfNotExists)
                    |> Async.Ignore

                return container.GetBlockBlobReference(file)
            }

        let readEntity (folder, file) = async {
                let table = getTable folder
                let retrieveOp = TableOperation.Retrieve<MutableFatEntity>(file, String.Empty)
                let! result = Async.FromBeginEndCancellable(table.BeginExecute, table.EndExecute, retrieveOp)
                return result.Result, result.Etag  
            }

        let deleteBlob(folder, file) = async {
                try
                    let! blob = getReadBlob(folder, file)
                    do! Async.FromBeginEndCancellable(blob.BeginDeleteIfExists, blob.EndDeleteIfExists)
                        |> Async.Ignore
                with _ -> ()
            }

        member this.Create(folder, file, serialize : Stream -> Async<unit>) : Async<Tag> =
            async {
                let table = getTable folder

                do! Async.FromBeginEndCancellable(table.BeginCreateIfNotExists, table.EndCreateIfNotExists)
                    |> Async.Ignore

                let! isFat = Helpers.isFatEntity serialize

                match isFat with
                | true -> 
                    let! bin = Helpers.toBinary serialize
                    let entity = MutableFatEntity(file, false, null, bin)

                    let insertOp = TableOperation.Insert(entity)
                    let! result = Async.FromBeginEndCancellable(table.BeginExecute, table.EndExecute, insertOp)
                    return result.Etag
                | false -> 
                    let refName = sprintf "%s.version.%s" file <| Guid.NewGuid().ToString()
                    let! blob = getWriteBlob(folder, refName)
                    use! stream = Async.FromBeginEndCancellable(blob.BeginOpenWrite, blob.EndOpenWrite)
                    do! serialize stream
                    stream.Dispose()

                    let entity = MutableFatEntity(file, true, refName, Array.empty)
                    let insertOp = TableOperation.Insert(entity)
                    let! result = Async.FromBeginEndCancellable(table.BeginExecute, table.EndExecute, insertOp)
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
                let! result = Async.FromBeginEndCancellable(table.BeginExecute, table.EndExecute, retrieveOp)
                let oldRef = (result.Result :?> MutableFatEntity).Reference
                let wasRef = (result.Result :?> MutableFatEntity).IsReference
                (result.Result :?> MutableFatEntity).ETag <- "*"
                let op = TableOperation.Delete(result.Result :?> MutableFatEntity)
                do! Async.FromBeginEndCancellable(table.BeginExecute, table.EndExecute, op)
                    |> Async.Ignore
                if wasRef then do! deleteBlob(folder, oldRef)                
            }

        member this.Update(folder, file, serialize : Stream -> Async<unit>, ?etag) : Async<bool * Tag> = 
            async {
                let etag = defaultArg etag "*"
                let table = getTable folder

                let retrieveOp = TableOperation.Retrieve<MutableFatEntity>(file, String.Empty)
                let! result = Async.FromBeginEndCancellable(table.BeginExecute, table.EndExecute, retrieveOp)
            
                if result.Result = null then raise <| Exception(sprintf "Non-existent %A - %A" folder file)
            
                let oldEntity = result.Result :?> MutableFatEntity

                let! isFat = Helpers.isFatEntity serialize

                match isFat with
                | true ->
                    let! bin = Helpers.toBinary serialize
                    let entity = MutableFatEntity(file, false, String.Empty, bin, ETag = etag)

                    let mergeOp = TableOperation.Merge(entity)
                
                    try
                        let! result = Async.FromBeginEndCancellable(table.BeginExecute, table.EndExecute, mergeOp)
                        if oldEntity.IsReference then
                            do! deleteBlob(folder, oldEntity.Reference)
                        return true, result.Etag
                    with :? StorageException as e ->
                        match e with
                        | UpdateConditionNotSatisfied _ -> return false, etag
                        | _ -> return raise e

                | false ->

                    if result.Etag <> etag && etag <> "*" then return false, etag
                    else
                        let refName = sprintf "%s.version.%s" file <| Guid.NewGuid().ToString()
                        let! blob = getWriteBlob(folder, refName)
                        use! stream = Async.FromBeginEndCancellable(blob.BeginOpenWrite, blob.EndOpenWrite)
                        do! serialize stream
                        stream.Dispose()

                        let entity = MutableFatEntity(file, true, refName, Array.empty, ETag = etag)
                        let insertOp = TableOperation.Merge(entity)
                        try
                            let! result = Async.FromBeginEndCancellable(table.BeginExecute, table.EndExecute, insertOp)
                            if oldEntity.IsReference then 
                                do! deleteBlob(folder, oldEntity.Reference)
                            return true, result.Etag
                        with :? StorageException as e ->
                            match e with
                            | UpdateConditionNotSatisfied _ -> 
                                do! deleteBlob(folder, refName)
                                return false, etag
                            | _ -> return raise e
            }
            