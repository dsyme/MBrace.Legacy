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

    type InMemoryCache(cacheStore : IStore, ?id : string, ?physicalMemoryLimitPercentage : int) =

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
        let serialize (stream : Stream, value : obj) : unit = Serialization.DefaultPickler.Serialize(stream, value)
        let deserialize (stream : Stream) : obj = Serialization.DefaultPickler.Deserialize(stream)

        // temporary solution before making cache async
        let run = Async.RunSynchronously

        // In-Memory Cache Policy
        let policy = new CacheItemPolicy()
        let syncRoot = new obj()
        do policy.RemovedCallback <- 
            new CacheEntryRemovedCallback (
                fun (args : CacheEntryRemovedArguments) ->
                    lock syncRoot (fun () ->
                        if not <| (run <| cacheStore.Exists("", args.CacheItem.Key)) then
                            retryAsync (RetryPolicy.Retry(10, 1.0<sec>))
                                (cacheStore.Create("", args.CacheItem.Key,
                                        fun stream -> async {
                                            serialize(stream, args.CacheItem.Value) |> ignore
                                            stream.Dispose() })) |> ignore )
            )

        member self.TryFind (key : string) =
            if cache.Contains key then Some cache.[key]
            else
                if run <| cacheStore.Exists("", key) then 
                    try
                        use stream = retry (RetryPolicy.Retry(10, 1.0<sec>)) (fun () -> run <| cacheStore.Read("", key))
                        let result = deserialize stream
                        cache.[key] <- result // update in-memory cache
                        Some result 
                    with _ -> None // cache failed (io problem or data corruption)... just say no and continue
                else None

        member self.Get (key : string) =
            match self.TryFind key with
            | None -> raise <| new ArgumentException("Key not found in cache.")
            | Some o -> o

        member self.ContainsKey (key : string) =
            cache.Contains key || run <| cacheStore.Exists("", key)

        member self.Set(key : string, value : obj) =
            if value <> null then
                try
                    cache.Add(key, value, policy) |> ignore
                with :? OutOfMemoryException -> 
                    cache.Trim(20) |> ignore
                    self.Set(key, value)
            else ()

        member self.Delete(key : string) =
            if not (self.ContainsKey key)
            then raise <| new ArgumentException("Key not found in cache.")
            
            if cache.Contains key then
                cache.Remove(key) |> ignore
            if run <| cacheStore.Exists("",key) then
                run <| cacheStore.Delete("",key)