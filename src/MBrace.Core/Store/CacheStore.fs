
namespace Nessos.MBrace.Caching
    open System
    open System.IO
    open System.Text
    open System.Threading
    open System.Collections.Concurrent
    open System.Runtime.Serialization
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Store
    open Nessos.MBrace.Store.Registry

    type LocalCacheStore(path : string, ?id : string ) = 
        
        let id = defaultArg id "localCache"

        //do if Directory.Exists path then try Directory.Delete(path, true) with _ -> ()
        let cache = new FileSystemStore(path, name = id) :> IStore
        let underlyingLazy = lazy StoreRegistry.DefaultStore.Store

        let base32 (s : string) =
            let bytes = s |> Seq.map byte |> Seq.toArray
            let s' = String.Convert.toBase32String(bytes)
            if Path.HasExtension(s) then s' + Path.GetExtension(s) else s'
            
        member this.Name = cache.Name

        member this.Create(folder, file, serializeTo) = 
            cache.Create(base32 folder, base32 file, serializeTo)

        member this.Commit(folder, file, ?asFile : bool) = 
            async {
                let asFile = defaultArg asFile false
                use! stream = cache.Read(base32 folder, base32 file)
                return! underlyingLazy.Value.CopyFrom(folder, file, stream, asFile)
            }

        member this.Read(folder, file) = this.Read(folder, file, true)

        member private this.Read(folder, file, ?checkCoherence : bool) = 
            async {
                let checkCoherence = defaultArg checkCoherence true
                let! cacheExists = cache.Exists(base32 folder, base32 file)
                let! storeExists = underlyingLazy.Value.Exists(folder, file) // TODO: parallel?
                match cacheExists, storeExists with
                | true, true -> 
                    try return! cache.Read(base32 folder, base32 file) 
                    with 
                    | :? IOException ->
                        return! retryAsync (RetryPolicy.Infinite(0.5<sec>))
                                (this.Read(folder, file, checkCoherence))
                | false, true -> 
                    try
                        use! stream = underlyingLazy.Value.Read(folder, file)
                        do! cache.CopyFrom(base32 folder, base32 file, stream)
                        stream.Dispose()
                        return! cache.Read(base32 folder, base32 file)
                    with 
                    | :? IOException -> 
                        return! retryAsync (RetryPolicy.Infinite(0.5<sec>))
                                (this.Read(folder, file, checkCoherence))
                | true, false -> 
                    if checkCoherence then 
                        return raise <| Exception(sprintf' "Incoherent cache : Item %s - %s found in cache but not in the main store" folder file)
                    else 
                        return! cache.Read(folder, file)
                | false, false -> 
                    return raise <| Exception(sprintf' "Item %s - %s not found" folder file)
            }