namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Threading

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Json
    open Nessos.MBrace.Utils.Logging

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Runtime

    type private Message<'LogEntry> = 
        | EnQueue of 'LogEntry
        | Flush of AsyncReplyChannel<exn option>

    type JsonStoreLogger<'LogEntry>(store : ICloudStore, container : string, logPrefix : string, ?minInterval : int, ?maxInterval : int, ?minEntries : int) =

        let minInterval = defaultArg minInterval 500
        let maxInterval = defaultArg maxInterval 10000
        let minEntries  = defaultArg minEntries 20

        do if maxInterval < minInterval then invalidArg "interval" "invalid intervals."

        let getNewLogfileName () = sprintf "%s-%s.log" logPrefix <| DateTime.Now.ToString("yyyyMMddHmmss")

        let isLogFile (f : string) = f.EndsWith(".log")

        let serializeLogs (entries : seq<'LogEntry>) (stream : Stream) = async { 
            do
                let jss = JsonSequence.CreateSerializer<'LogEntry>(stream, newLine = true)
                for e in entries do jss.WriteNext e
        }

        let deserializeLogs (stream : Stream) = async {
            let jsd = JsonSequence.CreateDeserializer<'LogEntry>(stream)
            return Seq.toArray jsd
        }


        let flush (entries : seq<'LogEntry>) = async {
            let file = getNewLogfileName()
            do! store.CreateImmutable(container, file, serializeLogs entries, asFile = true)
        }

        let fetch () = async {
            let! files = store.GetAllFiles(container)

            let readEntries (f : string) = async {
                use! stream = store.ReadImmutable(container, f)
                return! deserializeLogs stream
            }

            let! entries =
                files
                |> Array.filter isLogFile
                |> Array.sort
                |> Array.map readEntries
                |> Async.Parallel

            return Array.concat entries
        }

        let cts = new CancellationTokenSource()
        let gatheredLogs = new ResizeArray<'LogEntry> ()

        let rec loop (mbox : MailboxProcessor<Message<'LogEntry>>) = async {
            let! msg = mbox.Receive()

            match msg with
            | EnQueue item -> gatheredLogs.Add item
            | Flush ch -> 
                try
                    if gatheredLogs.Count > 0 then
                        do! flush <| gatheredLogs.ToArray()
                        gatheredLogs.Clear()

                    ch.Reply None

                with e ->
                    ch.Reply <| Some e
                            
            return! loop mbox
        }

        let batch = new MailboxProcessor<_>(loop, cts.Token)

        let rec flusher interval = async {

            let sleepAndRecurseWith i = async {
                do! Async.Sleep minInterval
                return! flusher i
            }

            if interval > maxInterval || gatheredLogs.Count > minEntries then
                let! r = batch.PostAndAsyncReply Flush
                match r with
                | None -> return! sleepAndRecurseWith 0
                | Some exn ->
                    return! sleepAndRecurseWith (interval + minInterval)

            else
                return! sleepAndRecurseWith (interval + minInterval)
        }

        member __.Start() =
            batch.Start()
            do Async.Start(flusher 0, cts.Token)

        member self.LogEntry (entry : 'LogEntry) =
            batch.Post(EnQueue entry)

        member self.Flush () =
            match batch.PostAndReply Flush with
            | None -> ()
            | Some e -> raise e

        member self.FetchLogs () : Async<'LogEntry []> = 
            fetch ()

        member self.DeleteLogs () : Async<unit> =
            store.DeleteContainer(container)

        interface IDisposable with
            member __.Dispose () = cts.Cancel()


    type JsonStoreLogger(store : ICloudStore, container, logPrefix) =
        inherit JsonStoreLogger<LogEntry>(store, container, logPrefix)

        interface ILogger with
            member __.LogEntry (e : LogEntry) = __.LogEntry e


    type CloudLogEntry =
        | UserMessage of string
        | Trace of TraceInfo

    type JsonCloudProcessStoreLogger(store : ICloudStore, processId : ProcessId, taskId : string)
        inherit JsonStoreLogger<CloudLogEntry>(store, sprintf "cloudProc-%d" processId, sprintf "task-%s" taskId)