namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Runtime.Serialization

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry

    type LocalCacheStore(localCacheStore : IStore, underlyingStore : IStore) = 
        
        //do if Directory.Exists path then try Directory.Delete(path, true) with _ -> ()
        //let cache = new FileSystemStore(path, name = id) :> IStore
        //let underlyingLazy = lazy StoreRegistry.DefaultStore.Store

        let base32 (s : string) =
            let s' = String.Convert.StringToBase32 s
            if Path.HasExtension(s) then s' + Path.GetExtension(s) else s'
            
        member this.Name = localCacheStore.Name

        member this.Create(folder, file, serializeTo) = 
            localCacheStore.Create(base32 folder, base32 file, serializeTo)

        member this.Commit(folder, file, ?asFile : bool) = 
            async {
                let asFile = defaultArg asFile false
                use! stream = localCacheStore.Read(base32 folder, base32 file)
                return! underlyingStore.CopyFrom(folder, file, stream, asFile)
            }

        member this.Read(folder, file) = this.Read(folder, file, true)

        member private this.Read(folder, file, ?checkCoherence : bool) = 
            async {
                let checkCoherence = defaultArg checkCoherence true
                let! cacheExists = localCacheStore.Exists(base32 folder, base32 file)
                let! storeExists = underlyingStore.Exists(folder, file) // TODO: parallel?
                match cacheExists, storeExists with
                | true, true -> 
                    try return! localCacheStore.Read(base32 folder, base32 file) 
                    with 
                    | :? IOException ->
                        return! retryAsync (RetryPolicy.Infinite(0.5<sec>))
                                (this.Read(folder, file, checkCoherence))
                | false, true -> 
                    try
                        use! stream = underlyingStore.Read(folder, file)
                        do! localCacheStore.CopyFrom(base32 folder, base32 file, stream)
                        stream.Dispose()
                        return! localCacheStore.Read(base32 folder, base32 file)
                    with 
                    | :? IOException -> 
                        return! retryAsync (RetryPolicy.Infinite(0.5<sec>))
                                (this.Read(folder, file, checkCoherence))
                | true, false -> 
                    if checkCoherence then 
                        return raise <| Nessos.MBrace.MBraceException(sprintf' "Incoherent cache : Item %s - %s found in cache but not in the main store" folder file)
                    else 
                        return! localCacheStore.Read(folder, file)
                | false, false -> 
                    return raise <| Nessos.MBrace.NonExistentObjectStoreException(folder,file)
            }