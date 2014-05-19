namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Threading

    open Newtonsoft.Json

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Logging
    open Nessos.MBrace.Utils.Retry

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Runtime

    type private Message = 
        | EnQueue of LogEntry
        | Flush of AsyncReplyChannel<exn option>

    type CloudStoreLogger (store : ICloudStore, container : string, ?minInterval : int, ?maxInterval : int, ?minEntries : int) =
        let minInterval = defaultArg minInterval 500
        let maxInterval = defaultArg maxInterval 5000
        let minEntries  = defaultArg minEntries 20

        do if maxInterval < minInterval then invalidArg "interval" "invalid intervals."

        static let getNewLogfileName () = 
            let id = Guid.NewGuid()
            sprintf "%O.log" id

        static let isLogFile (f : string) = f.EndsWith(".log")

        let jsonSerializer = Newtonsoft.Json.JsonSerializer.Create()

        let serializeLogs (entries : LogEntry []) (stream : Stream) = async { 
            use sw = new StreamWriter(stream)
            use jw = new JsonTextWriter(sw)
            do jsonSerializer.Serialize(jw, entries)
        }

        let deserializeLogs (stream : Stream) = async {
            use sr = new StreamReader(stream)
            use jr = new JsonTextReader(sr)
            return jsonSerializer.Deserialize<LogEntry []>(jr)
        }


        let flush (entries : LogEntry []) = async {
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
                |> Array.map readEntries
                |> Async.Parallel

            return
                entries
                |> Array.concat
                |> Array.sortBy(fun e -> e.Date)
        }

        let cts = new CancellationTokenSource()
        let gatheredLogs = new ResizeArray<LogEntry> ()

        let rec loop (mbox : MailboxProcessor<Message>) = async {
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

        member __.Start () =
            batch.Start()
            do Async.Start(flusher 0, cts.Token)

        member self.LogEntry (entry : LogEntry) =
            batch.Post(EnQueue entry)

        member self.Flush () =
            match batch.PostAndReply Flush with
            | None -> ()
            | Some e -> raise e

        member self.FetchLogs () : Async<LogEntry []> = 
            fetch ()

        member self.DeleteLogs () : Async<unit> =
            store.DeleteContainer(container)

        interface ILogger with
            member this.LogEntry e = this.LogEntry e

        interface IDisposable with
            member __.Dispose () = cts.Cancel()