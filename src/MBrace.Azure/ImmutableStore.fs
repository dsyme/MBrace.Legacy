namespace Nessos.MBrace.Azure

    open System
    open System.IO

    open Microsoft.WindowsAzure.Storage

    open Nessos.MBrace.Azure.Common

    type internal ImmutableStore (account : CloudStorageAccount) =
        let blob  = ImmutableBlobStoreProvider (account)
        let table = ImmutableTableStoreProvider(account)
        
        member this.Name = sprintf "Blob/Table store : %s/%s" blob.Name table.Name

        member this.Create(folder, file, serialize : Stream -> Async<unit>, asFile : bool) : Async<unit> =
            async {
                if asFile then 
                    return! blob.Create(folder, file, serialize)
                else
                    let! isFat = Helpers.isFatEntity serialize
                    match isFat with
                    | true ->  return! table.Create(folder, file, serialize)
                    | false -> return! blob.Create(folder, file, serialize)
            }

        member this.Read(folder, file) : Async<Stream> = async {
                let! inTable = table.Exists(folder, file)
                let! inBlob = blob.Exists(folder, file)

                match inTable, inBlob with
                | false, false -> 
                    return raise <| ArgumentException(sprintf "Non-existent %s - %s" folder file)
                | true, true ->
                    return raise <| Exception(sprintf "Duplicate %s - %s" folder file)
                | false, true ->
                    return! blob.Read(folder, file)
                | true, false ->
                    return! table.Read(folder, file)
            }

        member this.CopyFrom(folder, file, source : Stream, asFile : bool) : Async<unit> =
            async {
                if asFile || source.Length > Limits.MaxPayloadSize 
                then do! blob.CopyFrom(folder, file, source)
                else do! table.CopyFrom(folder, file, source)
            }

        member this.CopyTo(folder, file, target) = async {
                let! blobExists = blob.Exists(folder, file) 
                if blobExists then 
                    do! blob.CopyTo(folder, file, target)
                else
                    let! tableExists = table.Exists(folder, file)
                    if tableExists then do! table.CopyTo(folder, file, target)
                    else raise <| ArgumentException(sprintf "Non-existent %s - %s" folder file)
            }