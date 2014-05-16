namespace Nessos.MBrace.Runtime.Store

    open System.IO

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry


    type private Message = 
        | Flush of AsyncReplyChannel<unit>
        | QueueItem of (ProcessId * LogEntry)
        | QueueAndFlush of (ProcessId * LogEntry) * AsyncReplyChannel<unit>

    type CloudLogStore (store : ICloudStore, ?batchCount, ?batchTimespan) =
        let batchCount = defaultArg batchCount 50
        let batchTimespan = defaultArg batchTimespan 500

        let container = string >> sprintf "log%s"
        let postfix = sprintf "%s.log"
        let isLogFile (f : string) = f.EndsWith(".log")
        let isLogDir (d : string) = 
            if d.StartsWith("log") then 
                try Some << int <| d.Substring(3)
                with _ -> None
            else None

        static let serializeLogs (entries : LogEntry []) (stream : Stream) = async { 
            do Serialization.DefaultPickler.Serialize(stream, entries)
        }

        static let deserializeLogs (stream : Stream) = async {
            return Serialization.DefaultPickler.Deserialize<LogEntry []>(stream)
        }

        let flush (entries : (ProcessId * LogEntry) seq) =

            let flushProcessLogsAsync pid (entries : LogEntry []) =
                let folder = container pid
                let file = postfix <| System.Guid.NewGuid().ToString()
                store.CreateImmutable(folder, file, serializeLogs entries, true) // why true?
                |> retryAsync (RetryPolicy.Retry(10, 0.5<sec>))
                    
            try
                entries 
                |> Seq.groupBy fst 
                |> Seq.map (fun (pid, e) -> flushProcessLogsAsync pid (Seq.map snd e |> Seq.toArray))
                |> Async.Parallel
                |> Async.Ignore
                |> Async.RunSynchronously

            with ex ->
                raise <| Nessos.MBrace.StoreException("StoreLogger : Cannot flush user logs", ex)
           
        let fetch pid = async {
                let folder = container pid
                let! exists = store.ContainerExists folder
                if exists then
                    let! files = store.GetAllFiles(folder)
                    return 
                        files 
                        |> Array.filter isLogFile
                        |> Array.map (fun file -> async {
                            try
                                use! stream = store.ReadImmutable(folder, file)
                                return Serialization.DefaultPickler.Deserialize<LogEntry []>(stream)
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

        member self.LogEntryAndFlush(pid : ProcessId, entry : LogEntry) = 
            self.LogEntry(pid, entry) |> self.Flush

        member self.Flush () =
            batch.PostAndReply(Flush)

        member self.DumpLogs (pid : ProcessId) : Async<LogEntry []> =
            fetch pid

        member self.DumpLogs () : Async<LogEntry []> = 
            async {
                let! pids = store.GetAllContainers() 
                let pids = pids |> Array.choose isLogDir
                let! logs = pids |> Array.map self.DumpLogs
                                    |> Async.Parallel
                return logs |> Array.concat
            }

        member self.DeleteLogs (pid : ProcessId) : Async<unit> =
            store.DeleteContainer(container pid)

        member self.DeleteLogs () : Async<unit> =
            async {
                let! pids = store.GetAllContainers() 
                let pids = pids |> Array.choose isLogDir

                return! pids |> Array.map (fun pid -> store.DeleteContainer(container pid))
                             |> Async.Parallel
                             |> Async.Ignore
            }

        interface ICloudLogger with
            
            override this.LogTraceInfo(pid, entry) =
                this.LogEntry(pid, Trace entry)

            override this.LogUserInfo(pid, entry) =
                this.LogEntry(pid, UserLog entry)