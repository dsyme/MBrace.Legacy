namespace Nessos.MBrace.Core

open System.IO

open Nessos.MBrace
open Nessos.MBrace.Runtime
open Nessos.MBrace.Store
open Nessos.MBrace.Utils
open Nessos.MBrace.Utils.Retry

type private Message = 
    | Flush of AsyncReplyChannel<unit>
    | QueueItem of (ProcessId * LogEntry)
    | QueueAndFlush of (ProcessId * LogEntry) * AsyncReplyChannel<unit>

type StoreLogger (?store : IStore, ?batchCount, ?batchTimespan) =
    let store = defaultArg store (IoC.Resolve<IStore>())
//    let serializer = new FsCoreSerializer ()
    let batchCount = defaultArg batchCount 50
    let batchTimespan = defaultArg batchTimespan 500

    let pickler = Nessos.MBrace.Runtime.Serializer.Pickler

    let container = string >> sprintf' "log%s"
    let postfix = sprintf' "%s.log"
    let isLogFile (f : string) = f.EndsWith(".log")
    let isLogDir (d : string) = 
        if d.StartsWith("log") then 
            try Some << int <| d.Substring(3)
            with _ -> None
        else None

    let flush (entries : (ProcessId * LogEntry) seq) =
        let flushToStream (entries : LogEntry []) (stream : Stream) =
            async { 
                do pickler.Serialize(stream, entries)
            }


        let flushToStore pid (entries : LogEntry []) =
            let folder = container pid
            let file = postfix <| System.Guid.NewGuid().ToString()
            retry (RetryPolicy.Retry(10, 0.5<sec>))
                (fun () -> store.Create(folder, file, flushToStream entries))
        try
        entries |> Seq.groupBy fst 
                |> Seq.iter (fun (pid, e) -> Async.RunSynchronously <| flushToStore pid (Seq.map snd e |> Seq.toArray))
        with ex ->
            raise <| Nessos.MBrace.StoreException("StoreLogger : Cannot flush user logs", ex)
           
    let fetch pid = async {
            let folder = container pid
            let! exists = store.Exists folder
            if exists then
                let! files = store.GetFiles(folder)
                return 
                    files 
                    |> Array.filter isLogFile
                    |> Array.map (fun file -> async {
                        try
                            use! stream = store.Read(folder, file)
                            return pickler.Deserialize<LogEntry []>(stream)
                        with _ -> 
                            return Array.empty })
                    |> Async.Parallel
                    |> Async.RunSynchronously
                    |> Array.concat
            else
                return Array.empty
        }
                
    let batch = 
        new MailboxProcessor<Message>(fun inbox ->
            let rec loop (acc : (ProcessId * LogEntry) list) = async {
                let! msg = inbox.Receive()
                match msg with
                | Flush(ch) -> 
                    if (List.isEmpty >> not) acc then flush (acc |> List.rev)
                    ch.Reply ()
                    return! loop []
                | QueueItem(item) when acc.Length >= batchCount - 1 ->
                    flush ((item :: acc) |> List.rev)
                    return! loop []
                | QueueAndFlush(item, ch) ->
                    flush ((item :: acc) |> List.rev)
                    ch.Reply()
                    return! loop []
                | QueueItem item -> 
                    return! loop (item :: acc)
            }
            loop [])

    let timer =
        new MailboxProcessor<unit>(fun inbox ->
            let rec loop () =  async {
                do! Async.Sleep batchTimespan
                do! batch.PostAndAsyncReply(Flush)
                return! loop ()
            }
            loop ())

    do batch.Start()
    do timer.Start()

    member self.LogEntry (pid : ProcessId, entry : LogEntry) =
        batch.Post(QueueItem(pid, entry))

    member self.LogEntryAndFlush = self.LogEntry >> self.Flush

    member self.Flush () =
        batch.PostAndReply(Flush)

    member self.DumpLogs (pid : ProcessId) : Async<LogEntry []> =
        fetch pid

    member self.DumpLogs () : Async<LogEntry []> = 
        async {
            let! pids = store.GetFolders() 
            let pids = pids |> Array.choose isLogDir
            let! logs = pids |> Array.map self.DumpLogs
                             |> Async.Parallel
            return logs |> Array.concat
        }

    member self.DeleteLogs (pid : ProcessId) : Async<unit> =
        store.Delete(container pid)

    member self.DeleteLogs () : Async<unit> =
        async {
            let! pids = store.GetFolders() 
            let pids = pids |> Array.choose isLogDir
            return! pids |> Array.map (fun pid -> store.Delete(container pid))
                         |> Async.Parallel
                         |> Async.Ignore
        }