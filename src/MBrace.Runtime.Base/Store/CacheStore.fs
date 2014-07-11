namespace Nessos.MBrace.Store

    open System
    open System.IO
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry

    // TODO : CloudSeq & ClouFile only delete from target store and not cache
    // this might lead to confusing exception messages if attempting to read from
    // explicitly disposed items (cf: checkCoherence parameter in Read)
    // probably need a __.Delete implementation here too.
    // Better still, implement a full ICloudStore interface wrap

    type CacheStore(cacheContainer : string, localCacheStore : ICloudStore, targetStore : ICloudStore) = 

        let getCachedFileName (container : string) (name : string) =
            let name' = String.Convert.StringToBase32(container + name)
            if Path.HasExtension name then
                name' + Path.GetExtension name
            else
                name'
           
        member this.Container = cacheContainer
            
        member this.Name = localCacheStore.Name

        member this.Create(folder, file, serializeTo : Stream -> Async<unit>, asFile) = async {
                do! localCacheStore.CreateImmutable(cacheContainer, getCachedFileName folder file, serializeTo, asFile)
                use! stream = localCacheStore.ReadImmutable(cacheContainer, getCachedFileName folder file)
                return! targetStore.CopyFrom(folder, file, stream, asFile)
            }

        member this.Read(folder, file) = 
            let cachedFileName = getCachedFileName folder file

            let rec attemptRead () = async {
                let! cacheExists = localCacheStore.Exists(cacheContainer, cachedFileName)
                let! storeExists = targetStore.Exists(folder, file) // TODO: parallel?

                match cacheExists, storeExists with
                | true, true -> 
                    try return! localCacheStore.ReadImmutable(cacheContainer, cachedFileName) 
                    with 
                    | :? IOException ->
                        return! retryAsync (RetryPolicy.Infinite(0.5<sec>)) (attemptRead())

                | false, true -> 
                    try
                        use! stream = targetStore.ReadImmutable(folder, file)
                        do! localCacheStore.CopyFrom(cacheContainer, cachedFileName, stream, false) // why false?
                        stream.Dispose()
                        return! localCacheStore.ReadImmutable(cacheContainer, cachedFileName)
                    with 
                    | :? IOException -> 
                        return! retryAsync (RetryPolicy.Infinite(0.5<sec>)) (attemptRead())

                | true, false -> 
                    return raise <| StoreException(sprintf' "Incoherent cache : Item %s - %s found in cache but not in the main store" folder file)
                | false, false -> 
                    return raise <| NonExistentObjectStoreException(folder,file)
            }

            attemptRead()