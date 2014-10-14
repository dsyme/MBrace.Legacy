namespace Nessos.MBrace.Store
    open System

    /// Defines cache behavior.
    [<Flags>]
    type CacheBehavior = 
        /// Do not cache.
        | None = 0uy
        /// Cache only on write.
        | OnWrite = 1uy
        /// Cache only on read.
        | OnRead = 2uy
        /// Cache on read/write.
        | Default = 3uy

namespace Nessos.MBrace.Runtime

    open System.IO
    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Store

    // TODO : CloudSeq & ClouFile only delete from target store and not cache
    // this might lead to confusing exception messages if attempting to read from
    // explicitly disposed items (cf: checkCoherence parameter in Read)
    // probably need a __.Delete implementation here too.
    // Better still, implement a full ICloudStore interface wrap

    type CacheStore(cacheContainer : string, localCacheStore : ICloudStore, targetStore : ICloudStore, ?behavior : CacheBehavior) = 

        let getCachedFileName (container : string) (name : string) =
            let name' = String.Convert.StringToBase32(container + name)
            if Path.HasExtension name then
                name' + Path.GetExtension name
            else
                name'

        let mutable behavior = defaultArg behavior CacheBehavior.Default

        member this.Container = cacheContainer
            
        member this.Name = localCacheStore.Name

        member this.Behavior with get () = behavior and set b = behavior <- b

#if NO_CACHESTORE
        member this.Create(folder, file, serializeTo : Stream -> Async<unit>, asFile) =
            targetStore.CreateImmutable(folder, file, serializeTo, asFile)

        member this.Read(folder, file) = 
            targetStore.ReadImmutable(folder, file)
#else 
        member this.Create(folder, file, serializeTo : Stream -> Async<unit>, asFile) = async {
                if behavior.HasFlag CacheBehavior.OnWrite then
                    do! localCacheStore.CreateImmutable(cacheContainer, getCachedFileName folder file, serializeTo, asFile)
                    use! stream = localCacheStore.ReadImmutable(cacheContainer, getCachedFileName folder file)
                    return! targetStore.CopyFrom(folder, file, stream, asFile)
                else
                    return! targetStore.CreateImmutable(folder, file, serializeTo, asFile)
            }

        member this.Read(folder, file) = 
            if behavior.HasFlag CacheBehavior.OnRead then
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
                            do! localCacheStore.CopyFrom(cacheContainer, cachedFileName, stream, true)
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
            else
                targetStore.ReadImmutable(folder, file)
#endif