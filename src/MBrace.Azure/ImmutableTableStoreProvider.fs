namespace Nessos.MBrace.Azure

    open System
    open System.IO
    open System.Threading.Tasks
    //open Nessos.MBrace.Store
    open Nessos.MBrace.Azure.Common
    open Microsoft.WindowsAzure
    open Microsoft.WindowsAzure.Storage
    open Microsoft.WindowsAzure.Storage.Table

    type ImmutableTableStoreProvider(connectionString : string) =
       
        // get a new client every time because the client obj is not thread safe
        let getClient () = Clients.getTableClient connectionString
        let getTable name = getClient().GetTableReference(name)

        let read (folder, file) =
            async {
                let table = getTable folder
                let retrieveOp = TableOperation.Retrieve<FatEntity>(file, String.Empty)
                let! result = Async.FromBeginEndCancellable(table.BeginExecute, table.EndExecute, retrieveOp)
                return result.Result
            }

        member this.Name = getClient().BaseUri.AbsoluteUri

        member self.Create(folder, file, serialize : Stream -> Async<unit>) : Async<unit> =
            async {
                use ms = new MemoryStream()
                do! serialize ms
                ms.Position <- 0L
                let bin = ms.ToArray()
            
                let table = getTable folder
                do! Async.FromBeginEndCancellable(table.BeginCreateIfNotExists, table.EndCreateIfNotExists)
                    |> Async.Ignore
                let insertOp = TableOperation.Insert(FatEntity(file, bin))

                do! Async.FromBeginEndCancellable(table.BeginExecute, table.EndExecute, insertOp)
                    |> Async.Ignore
            }

        member self.Read (folder, file) : Async<Stream> =
            async {
                let! result = read(folder, file)
                if result <> null then
                    let bin = (result :?> FatEntity).GetPayload()
                    return new MemoryStream(bin) :> Stream
                else 
                    return failwith "Trying to read from non-existent record"
            }

        member self.Exists(folder, file) : Async<bool> = 
            async {
                let! result = read(folder, file)
                return result <> null   
            }

        member this.Exists(folder) =
            async {
                let table = getClient().GetTableReference(folder)
                return! Async.FromBeginEndCancellable(table.BeginExists, table.EndExists)
            }


        member this.GetFiles(folder) =
            async {
                let table = getTable folder
                let! exists = Async.FromBeginEndCancellable(table.BeginExists, table.EndExists)
                if exists then
                    let rangeQuery = TableQuery<DynamicTableEntity>().Select([|"PartitionKey"|])
                    let resolver = EntityResolver(fun pk rk ts props etag -> pk)
                    return table.ExecuteQuery(rangeQuery, resolver, null, null)
                           |> Seq.toArray
                else 
                    return Array.empty
            }

        member self.Delete(folder, file) =
            async {
                let! entity = read(folder, file)
                let deleteOp = TableOperation.Delete(entity :?> FatEntity)
                let table = getTable folder
                let! r = Async.FromBeginEndCancellable(table.BeginExecute, table.EndExecute, deleteOp)
                r.Result |> ignore
            }

        member self.Delete(folder) =
            async {
                let table = getTable folder
                do! Async.FromBeginEndCancellable(table.BeginDeleteIfExists, table.EndDeleteIfExists)
                    |> Async.Ignore
            }

        member self.CopyFrom(folder, file, source : Stream) =
            self.Create(folder, file, fun s -> async { source.CopyTo(s); source.Dispose() })

        member self.CopyTo(folder, file, target : Stream) = async {
                let! s = self.Read(folder, file)
                s.CopyTo(target)
                do! Async.AwaitTask(s.CopyToAsync(target).ContinueWith<unit>(fun _ -> ()))

                target.Dispose()
            }