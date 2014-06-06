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

#if NO_INMEM_CACHE
    type InMemCache(?physicalMemoryLimitPercentage : int) =
        member self.TryFind (key : string) = None : obj option
        member self.Get (key : string) = raise <| new ArgumentException("Key not found in cache.")
        member self.ContainsKey (key : string) = false
        member self.Set(key : string, value : obj) = ()
        member self.DeleteIfExists(key : string) = ()
#else
    type InMemCache(?physicalMemoryLimitPercentage : int) =

        // arg parsing
        let percentage = 
            match defaultArg physicalMemoryLimitPercentage 70 with
            | n when n > 0 && n <= 100 -> n
            | _ -> raise <| new ArgumentException("Invalid percentage")

        // In-Memory Cache
        let config = new System.Collections.Specialized.NameValueCollection()
        do config.Add("PhysicalMemoryLimitPercentage", percentage.ToString())
        let cache = new MemoryCache(Guid.NewGuid().ToString(), config)
        
        // In-Memory Cache Policy
        let policy = new CacheItemPolicy()

        member self.TryFind (key : string) =
            if cache.Contains key then Some cache.[key]
            else None // cache failed (io problem or data corruption)... just say no and continue

        member self.Get (key : string) = 
            let result = self.TryFind key 
            match result with
            | None -> raise <| new ArgumentException("Key not found in cache.")
            | Some o -> o

        member self.ContainsKey (key : string) =
            cache.Contains key

        member self.Set(key : string, value : obj) =
            if value <> null then
                try
                    cache.Add(key, value, policy) |> ignore
                with :? OutOfMemoryException -> 
                    cache.Trim(20) |> ignore
                    self.Set(key, value)
            else ()

        member self.DeleteIfExists(key : string) = 
            if cache.Contains key then
                cache.Remove(key) |> ignore
#endif