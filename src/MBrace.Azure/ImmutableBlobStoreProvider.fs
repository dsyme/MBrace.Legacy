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
                let container = (getClient()).GetContainerReference(folder)

                do! Async.FromBeginEndCancellable(container.BeginCreateIfNotExists, container.EndCreateIfNotExists)
                    |> Async.Ignore

                return container.GetBlockBlobReference(file)
            }

        member this.Name = getClient().BaseUri.AbsoluteUri

        member this.Create (folder, file, serialize : Stream -> Async<unit>) : Async<unit> =
            async {
                let! blob = getWriteBlob(folder, file)
                use! s = Async.FromBeginEndCancellable(blob.BeginOpenWrite, blob.EndOpenWrite)
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
                
                let b1 = Async.FromBeginEndCancellable(container.BeginExists, container.EndExists)
                let blob = container.GetBlockBlobReference(file)
                let b2 = Async.FromBeginEndCancellable(blob.BeginExists, blob.EndExists)
                
                return! Async.And(b1, b2)
            }

        member this.Exists(folder) =
            async { 
                let container = (getClient()).GetContainerReference(folder)
                return! Async.FromBeginEndCancellable(container.BeginExists, container.EndExists)
            }
        
        member this.GetFiles(folder) =
            async {
                let container = (getClient()).GetContainerReference(folder)
                let! exists = Async.FromBeginEndCancellable(container.BeginExists, container.EndExists)
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
                do! Async.FromBeginEndCancellable(blob.BeginDownloadToStream, blob.EndDownloadToStream, target)
            }

        member self.CopyFrom(folder : string, file : string, source : Stream) =
            async {
                let! blob = getWriteBlob(folder, file)
                do! Async.FromBeginEndCancellable(blob.BeginUploadFromStream, blob.EndUploadFromStream, source)
            }


        member self.Delete(folder : string, file : string) =
            async {
                let! blob = getReadBlob(folder, file)
                do! Async.FromBeginEndCancellable(blob.BeginDeleteIfExists, blob.EndDeleteIfExists)
                    |> Async.Ignore
            }


        member self.Delete(folder : string) =
            async {
                let cont = (getClient()).GetContainerReference(folder)
                do! Async.FromBeginEndCancellable(cont.BeginDeleteIfExists, cont.EndDeleteIfExists)
                    |> Async.Ignore
            }