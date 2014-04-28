namespace Nessos.MBrace.Caching

    open System
    open System.IO
    open System.Text
    open System.Runtime.Caching
    open System.Runtime.Serialization

    open Nessos.FsPickler

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Store

    type Cache(?id : string, ?physicalMemoryLimitPercentage : int, ?location : string) = 
//        let pickler = Nessos.MBrace.Runtime.Serializer.Pickler
        // temporary dependency injection hack
        let pickler = IoC.Resolve<FsPickler>()

        // arg parsing
        let id = match id with Some i -> i | None -> Guid.NewGuid().ToString()
        let percentage = 
            match defaultArg physicalMemoryLimitPercentage 70 with
            | n when n > 0 && n <= 100 -> n
            | _ -> raise <| new ArgumentException("Invalid percentage")

        let location = defaultArg location <| Path.GetTempPath()

        // In-Memory Cache
        let config = new System.Collections.Specialized.NameValueCollection()
        do config.Add("PhysicalMemoryLimitPercentage", percentage.ToString())
        let cache = new MemoryCache(id, config)

        let cacheDir = Path.Combine(location, "cache")
        do if Directory.Exists cacheDir then Async.Start <| async { Directory.Delete(cacheDir, true) }
        let fileSystemStore = new FileSystemStore(cacheDir, name = "cacheStore") :> IStore

//#if APPDOMAIN_ISOLATION
////        let persistentCache = new System.Collections.Generic.Dictionary<string, string>()
//        let fileSystemStore = (new FileSystemStore("cache", cacheDir) :> IStore)
//#else
//        // Persistent Cache
//        let cacheDir = Path.Combine(location, "cache")
//        do if Directory.Exists cacheDir then Directory.Delete(cacheDir, true)
//        let fileSystemStore = (new FileSystemStore("cache", cacheDir) :> IStore)
//#endif
        
        // Serialize/Deserialize helpers
        let serialize (stream : Stream, value : obj) : unit = pickler.Serialize(stream, value)
        let deserialize (stream : Stream) : obj = pickler.Deserialize(stream)

        // temporary solution before making cache async
        let run = Async.RunSynchronously

        // In-Memory Cache Policy
        let policy = new CacheItemPolicy()
        let syncRoot = new obj()
        do policy.RemovedCallback <- 
            new CacheEntryRemovedCallback (
                fun (args : CacheEntryRemovedArguments) ->
                    lock syncRoot (fun () ->
                        if not <| (run <| fileSystemStore.Exists("", args.CacheItem.Key)) then
                            retryAsync (RetryPolicy.Retry(10, 1.0<sec>))
                                (fileSystemStore.Create("", args.CacheItem.Key,
                                        fun stream -> async {
                                            serialize(stream, args.CacheItem.Value) |> ignore
                                            stream.Dispose() })) |> ignore )
            )

        member self.TryFind (key : string) =
            if cache.Contains key then Some cache.[key]
            else
                if run <| fileSystemStore.Exists("", key) then 
                    try
                        use stream = retry (RetryPolicy.Retry(10, 1.0<sec>)) (fun () -> run <| fileSystemStore.Read("", key))
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
            cache.Contains key || run <| fileSystemStore.Exists("", key)

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
            if run <| fileSystemStore.Exists("",key) then
                run <| fileSystemStore.Delete("",key)

//        member internal __.Serializer = serializer

        // this is not meant to be precise 
        member __.Count =  raise <| new NotImplementedException() //cache.GetCount() + int64 persistentCache.Count
        member __.IsEmpty = __.Count = 0L



    type CachedDictionary<'Key,'Value>(?physicalMemoryLimitPercentage : int, ?location : string) =
        let cache = new Cache(?physicalMemoryLimitPercentage = physicalMemoryLimitPercentage, ?location = location)

        // probably not too safe
        let mkHashString (key : obj) : string = key.GetHashCode().ToString()

        member __.Count = cache.Count
        member __.IsEmpty = cache.IsEmpty

        member __.ContainsKey (k : 'Key) = cache.ContainsKey (mkHashString k)
        member __.TryFind (k : 'Key) = cache.TryFind (mkHashString k) |> Option.map (fun v -> v :?> 'Value)
        member __.Set (k : 'Key, v : 'Value) = cache.Set(mkHashString k, v)
        member __.Get (k : 'Key) =
            match __.TryFind k with
            | Some v -> v
            | None -> raise <| new ArgumentException("Key not found in cache.")
        member __.Delete (k : 'Key) =
            match __.ContainsKey k with
            | true -> cache.Delete(mkHashString k)
            | false -> raise <| new ArgumentException("Key not found in cache.")

        member self.Item
            with set (k : 'Key) (v: 'Value) = self.Set(k,v)
            and get (k : 'Key) = self.Get k

    type CachedSet<'T>(?physicalMemoryLimitPercentage : int, ?location : string) =
        let cache = new Cache(?physicalMemoryLimitPercentage = physicalMemoryLimitPercentage, ?location = location)
        let count = ref 0L

        member __.Add (x : 'T) =
            lock count (fun () -> cache.Set((!count).ToString(), x) ; count := !count + 1L)

        member __.ToSeq () =
            { 0L..(!count - 1L) } |> Seq.map (fun entry -> cache.Get (entry.ToString()) :?> 'T)

        member __.Clear () = lock count (fun () -> count := 0L)