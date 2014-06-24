namespace Nessos.MBrace.Azure

    open System.IO
    //open Nessos.MBrace.Store
    open Nessos.MBrace.Azure.Common
    open Microsoft.WindowsAzure
    open Microsoft.WindowsAzure.Storage

    type ImmutableBlobStoreProvider (connectionString : string) =
    
        let getClient () = Clients.getBlobClient connectionString

        let getReadBlob (folder, file) = async { 
            let container = (getClient()).GetContainerReference(folder)

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
                let container = (getClient()).GetContainerReference(folder)

                do! Async.AwaitTask(container.CreateIfNotExistsAsync())
                    |> Async.Ignore

                return container.GetBlockBlobReference(file)
            }

        member this.Name = getClient().BaseUri.AbsoluteUri

        member this.Create (folder, file, serialize : Stream -> Async<unit>) : Async<unit> =
            async {
                let! blob = getWriteBlob(folder, file)
                use! s = Async.AwaitTask(blob.OpenWriteAsync())
                return! serialize(s)
            }

        member this.Read (folder, file) : Async<Stream> =
            async {
                let! blob = getReadBlob(folder, file)
                return blob.OpenRead()
            }

        member self.Exists(folder, file) = 
            async {
                let container = (getClient()).GetContainerReference(folder)
                
                let b1 = Async.AwaitTask(container.ExistsAsync())
                let blob = container.GetBlockBlobReference(file)
                let b2 = Async.AwaitTask(blob.ExistsAsync())
                
                return! Async.And(b1, b2)
            }

        member this.Exists(folder) =
            async { 
                let container = (getClient()).GetContainerReference(folder)
                return! Async.AwaitTask<_>(container.ExistsAsync()) 
            }
        
        member this.GetFiles(folder) =
            async {
                let container = (getClient()).GetContainerReference(folder)
                let! exists = Async.AwaitTask(container.ExistsAsync())
                if exists then
                    return 
                        container.ListBlobs()
                        |> Seq.map (fun blob -> blob.Uri.Segments |> Seq.last)
                        |> Seq.toArray
                else 
                    return Array.empty
            }

        member self.CopyTo(folder : string, file : string, target : Stream) = 
            async {
                let! blob = getReadBlob(folder,file)
                do! Async.AwaitTask(blob.DownloadToStreamAsync(target).ContinueWith(ignore))
            }

        member self.CopyFrom(folder : string, file : string, source : Stream) =
            async {
                let! blob = getWriteBlob(folder, file)
                do! Async.AwaitTask(blob.UploadFromStreamAsync(source).ContinueWith(ignore))
            }


        member self.Delete(folder : string, file : string) =
            async {
                let! blob = getReadBlob(folder, file)
                do! Async.AwaitTask(blob.DeleteIfExistsAsync())
                    |> Async.Ignore
            }


        member self.Delete(folder : string) =
            async {
                let cont = (getClient()).GetContainerReference(folder)
                do! Async.AwaitTask<_>(cont.DeleteIfExistsAsync())
                    |> Async.Ignore
            }