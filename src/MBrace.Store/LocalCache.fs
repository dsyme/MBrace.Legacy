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

    type CacheLoopMessage =
        | Create of Folder * File * (Stream -> unit) * AsyncReplyChannel<unit>
        | Commit of Folder * File * AsyncReplyChannel<unit>
        | Read   of Folder * File * AsyncReplyChannel<Stream>

    type LocalCache(path : string, ?underlying : IStore, ?id : string) = 
        
        let id = defaultArg id "cache"

        let cache = new FileSystemStore(path, name = id) :> IStore
        let underlying = lazy defaultArg underlying StoreRegistry.DefaultStore.Store

        let drive = 
            let root = Directory.GetDirectoryRoot path
            DriveInfo.GetDrives() 
            |> Array.find (fun s -> s.Name.ToUpper() = root.ToUpper())

        let cache_max_perc = 0.7
        let cache_total_size = float drive.TotalSize
        let cache_max_size = cache_max_perc * cache_total_size
        let cache_free_size () = float drive.AvailableFreeSpace
        let cache_free_perc () = cache_free_size () / cache_total_size
        let cache_check_interval = TimeSpan.FromSeconds 30.

        let cache_agent = new MailboxProcessor<unit>(fun inbox ->
            let rec loop () = async {
                
                return! loop ()
            }
            loop ())

        member this.Create(folder, file, serializeTo) = cache.Create(folder,file, serializeTo)

        member this.Commit(folder,file) = 
//            use stream = cache.Read(folder, file)
//            underlying.Value.CopyFrom(folder, file, stream)
            raise <| NotImplementedException()

        member this.Read(folder, file) = //this.Read(folder, file, true)
            raise <| NotImplementedException()

        member private this.Read(folder, file, ?check_coherence : bool) = 
            raise <| NotImplementedException()
//            let check_coherence = defaultArg check_coherence true
//            match cache.Exists(folder, file), underlying.Value.Exists(folder, file) with
//            | true, true -> 
//                try cache.Read(folder, file) 
//                with 
//                | :? IOException ->
//                    retry (RetryPolicy.Infinite(0.5<sec>))
//                        (fun () -> this.Read(folder, file, check_coherence))
//
//            | false, true -> 
//                try
//                    use stream = underlying.Value.Read(folder, file)
//                    cache.CopyFrom(folder, file, stream)
//                    stream.Dispose()
//                    cache.Read(folder, file)
//                with 
//                | :? IOException ->
//                    retry (RetryPolicy.Infinite(0.5<sec>))
//                        (fun () -> this.Read(folder, file, check_coherence))
//
//            | true, false -> 
//                if check_coherence
//                then raise <| Exception(sprintf' "Incoherent cache : Item %s - %s found in cache but not in the main store" folder file)
//                else cache.Read(folder, file)
//            | false, false -> raise <| Exception(sprintf' "Item %s - %s not found" folder file)