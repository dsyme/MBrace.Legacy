namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Text
    open System.Runtime.Caching
    open System.Runtime.Serialization

    open Nessos.FsPickler

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Runtime

    // two-tier object cache

    type LocalObjectCache(cacheStore : ICloudStore, ?id : string, ?physicalMemoryLimitPercentage : int) =

        // arg parsing
        let id = match id with Some i -> i | None -> Guid.NewGuid().ToString()
        let percentage = 
            match defaultArg physicalMemoryLimitPercentage 70 with
            | n when n > 0 && n <= 100 -> n
            | _ -> raise <| new ArgumentException("Invalid percentage")

        // In-Memory Cache
        let config = new System.Collections.Specialized.NameValueCollection()
        do config.Add("PhysicalMemoryLimitPercentage", percentage.ToString())
        let cache = new MemoryCache(id, config)
        
        // Serialize/Deserialize helpers
        let serialize (value : obj) (stream : Stream) = async.Return <| Serialization.DefaultPickler.Serialize<obj>(stream, value)
        let deserialize (stream : Stream) = Serialization.DefaultPickler.Deserialize<obj>(stream)

        // In-Memory Cache Policy
        let policy = new CacheItemPolicy()
        let syncRoot = new obj()
        do policy.RemovedCallback <- 
            new CacheEntryRemovedCallback (
                fun (args : CacheEntryRemovedArguments) ->
                    lock syncRoot (fun () ->
                        async {
                            let! existsInCache = cacheStore.Exists(id, args.CacheItem.Key)
                            if not existsInCache then
                                return!
                                    cacheStore.CreateImmutable(id, args.CacheItem.Key, serialize args.CacheItem.Value, false)
                                    |> retryAsync (RetryPolicy.Retry(10, 1.0<sec>))
                        } |> Async.RunSynchronously))
//                        if not <| (run <| cacheStore.Exists(id, args.CacheItem.Key)) then
//                            retryAsync (RetryPolicy.Retry(10, 1.0<sec>))
//                                (cacheStore.Create(id, args.CacheItem.Key,
//                                        fun stream -> async {
//                                            serialize(stream, args.CacheItem.Value) |> ignore
//                                            stream.Dispose() })) |> ignore )
//            )

        member self.TryFind (key : string) = async {
            if cache.Contains key then return Some cache.[key]
            else
                try
                    use! stream = cacheStore.ReadImmutable(id, key)
                    let result = deserialize stream
                    cache.[key] <- result // update in-memory cache
                    return Some result 
                with _ -> return None // cache failed (io problem or data corruption)... just say no and continue
        }

        member self.Get (key : string) = async {
            let! result = self.TryFind key 
            return
                match result with
                | None -> raise <| new ArgumentException("Key not found in cache.")
                | Some o -> o
        }

        member self.ContainsKey (key : string) = async {
            if cache.Contains key then return true
            else
                return! cacheStore.Exists(id, key)
        }

        member self.Set(key : string, value : obj) =
            if value <> null then
                try
                    cache.Add(key, value, policy) |> ignore
                with :? OutOfMemoryException -> 
                    cache.Trim(20) |> ignore
                    self.Set(key, value)
            else ()

        member self.Delete(key : string) = async {
//            let! containsKey = self.ContainsKey key
//            if not containsKey then 
//                return raise <| new ArgumentException("Key not found in cache.")
            
            if cache.Contains key then
                cache.Remove(key) |> ignore

            let! storeContains = cacheStore.Exists(id, key)
            if storeContains then
                return! cacheStore.Delete(id,key) 
        }