namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Runtime.Serialization

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry

    type LocalCacheStore(id : string, localCacheStore : ICloudStore, targetStore : ICloudStore) = 
        
        //do if Directory.Exists path then try Directory.Delete(path, true) with _ -> ()
        //let cache = new FileSystemStore(path, name = id) :> ICloudStore
        //let underlyingLazy = lazy StoreRegistry.DefaultStore.Store

        let getCachedFileName (container : string) (name : string) =
            let name' = String.Convert.StringToBase32(container + name)
            if Path.HasExtension name then
                name' + Path.GetExtension name
            else
                name'
            
        member this.Name = localCacheStore.Name

        member this.Create(folder, file, serializeTo : Stream -> Async<unit>, asFile) = 
            localCacheStore.CreateImmutable(id, getCachedFileName folder file, serializeTo, asFile)

        member this.Commit(folder, file, asFile : bool) = 
            async {
                use! stream = localCacheStore.ReadImmutable(id, getCachedFileName folder file)
                return! targetStore.CopyFrom(folder, file, stream, asFile)
            }

        member this.Read(folder, file, ?checkCoherence : bool) = 
            let checkCoherence = defaultArg checkCoherence true
            let cachedFileName = getCachedFileName folder file

            let rec attemptRead () = async {
                let! cacheExists = localCacheStore.Exists(id, cachedFileName)
                let! storeExists = targetStore.Exists(folder, file) // TODO: parallel?

                match cacheExists, storeExists with
                | true, true -> 
                    try return! localCacheStore.ReadImmutable(id, cachedFileName) 
                    with 
                    | :? IOException ->
                        return! retryAsync (RetryPolicy.Infinite(0.5<sec>)) (attemptRead())

                | false, true -> 
                    try
                        use! stream = targetStore.ReadImmutable(folder, file)
                        do! localCacheStore.CopyFrom(id, cachedFileName, stream, false) // why false?
                        stream.Dispose()
                        return! localCacheStore.ReadImmutable(id, cachedFileName)
                    with 
                    | :? IOException -> 
                        return! retryAsync (RetryPolicy.Infinite(0.5<sec>)) (attemptRead())

                | true, false -> 
                    if checkCoherence then 
                        return raise <| Nessos.MBrace.MBraceException(sprintf' "Incoherent cache : Item %s - %s found in cache but not in the main store" folder file)
                    else 
                        return! localCacheStore.ReadImmutable(id, cachedFileName)
                | false, false -> 
                    return raise <| Nessos.MBrace.NonExistentObjectStoreException(folder,file)
            }

            attemptRead()