namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Threading

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Json

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Runtime

    [<AutoOpen>]
    module private StoreLogUtils =

        type Message<'LogEntry> = 
            | EnQueue of 'LogEntry
            | Flush of AsyncReplyChannel<exn option>

        let getNewLogfileName logPrefix = sprintf "%s_%s.log" logPrefix <| DateTime.Now.ToString("yyyyMMddHmmss")
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


    type LogStore<'LogEntry>(store : ICloudStore, container : string, logPrefix : string, ?minInterval : int, ?maxInterval : int, ?minEntries : int) =

        let minInterval = defaultArg minInterval 100
        let maxInterval = defaultArg maxInterval 1000
        let minEntries  = defaultArg minEntries 5

        do if maxInterval < minInterval then invalidArg "interval" "invalid intervals."

        let flush (entries : seq<'LogEntry>) = async {
            let file = getNewLogfileName logPrefix
            do! store.CreateImmutable(container, file, serializeLogs entries, asFile = true)
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

        do
            batch.Start()
            Async.Start(flusher 0, cts.Token)

        member self.LogEntry (entry : 'LogEntry) =
            batch.Post(EnQueue entry)

        member self.Flush () =
            match batch.PostAndReply Flush with
            | None -> ()
            | Some e -> raise e

        interface IDisposable with
            member __.Dispose () = cts.Cancel()



    type LogStoreReader<'LogEntry>(store : ICloudStore, container) =

        member self.FetchLogs (?filterF : string -> bool) : Async<'LogEntry []> = 
            async {

                let filterF = defaultArg filterF (fun _ -> true)
                let! files = store.GetAllFiles(container)

                let readEntries (f : string) : Async<'LogEntry []> = async {
                    use! stream = store.ReadImmutable(container, f)
                    return! deserializeLogs stream
                }

                let! entries =
                    files
                    |> Array.filter (fun f -> isLogFile f && filterF f)
                    |> Array.sort
                    |> Array.map readEntries
                    |> Async.Parallel

                return Array.concat entries
            }

        member self.DeleteLogs () : Async<unit> =
            store.DeleteContainer(container)