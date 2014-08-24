namespace Nessos.MBrace.Runtime

    open System
    open System.IO
    open System.Threading

    open Nessos.FsPickler

    open Nessos.MBrace.Utils

    open Nessos.MBrace
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.StoreUtils

    [<AutoOpen>]
    module private LogStoreUtils =

        type Message<'LogEntry> = 
            | EnQueue of 'LogEntry
            | Flush of AsyncReplyChannel<exn option>

        let getNewLogfileName logPrefix counter = sprintf "%s_%d.log" logPrefix counter
        let isLogFile (f : string) = f.EndsWith(".log")

        let serializeLogs (entries : seq<'LogEntry>) (stream : Stream) = async {
            use sw = new StreamWriter(stream)
            let length = JsonLogPickler.WriteEntries(sw, entries, leaveOpen = false)
            return ()
        }

        let deserializeLogs<'LogEntry> (stream : Stream) = async {
            use sr = new StreamReader(stream)
            let seq = JsonLogPickler.ReadEntries<'LogEntry>(sr)
            return Seq.toArray seq
        }

    type LogStore<'LogEntry>(store : ICloudStore, container : string, logPrefix : string, ?minInterval : int, ?maxInterval : int, ?minEntries : int) =

        let minInterval = defaultArg minInterval 100
        let maxInterval = defaultArg maxInterval 1000
        let minEntries  = defaultArg minEntries 5

        do if maxInterval < minInterval then invalidArg "interval" "invalid intervals."

        let cnt = ref 0

        let flush (entries : seq<'LogEntry>) = async {
            let file = getNewLogfileName logPrefix cnt.Value
            ThreadSafe.incr cnt
            do! store.CreateImmutable(container, file, serializeLogs entries, asFile = true)
                |> onCreateError container file
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
            member self.Dispose () = self.Flush () ; cts.Cancel()



    type LogStoreReader<'LogEntry>(store : ICloudStore, container) =

        member self.FetchLogs (?filterF : string -> bool) : Async<'LogEntry []> = 
            async {
                let filterF = defaultArg filterF (fun _ -> true)
                let! containerExists = store.ContainerExists container
                                       
                if not containerExists then
                    return [||]
                else   
                    let! files = store.EnumerateFiles container

                    let readEntries (f : string) : Async<'LogEntry []> = async {
                        use! stream = store.ReadImmutable(container, f)
                                      |> onDereferenceError(sprintf "%s %s" container f)
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
            async {
                let! exists = store.ContainerExists(container)
                if exists then
                    do! store.DeleteContainer(container)
            } |> onDeleteError container
            


    type StreamingLogReader<'LogEntry>(store : ICloudStore, container, ct : CancellationToken, ?pollingInterval : int) as this =

        let logsRead = new System.Collections.Generic.HashSet<string>()

        let pollingInterval = defaultArg pollingInterval 500

        let updatedEvent = new Event<_>()

        let rec loop _ = async {
            let! containerExists = store.ContainerExists container

            if containerExists then 
                let! files = store.EnumerateFiles container
                let files = files |> Seq.filter (fun file -> isLogFile file && not <| logsRead.Contains file)

                if not <| Seq.isEmpty files then
                    let readEntries file : Async<'LogEntry []> = async { 
                        use! stream = store.ReadImmutable(container, file)
                                      |> onDereferenceError(sprintf "%s %s" container file)
                        return! deserializeLogs stream
                    }

                    let! entries =
                        files |> Seq.sort
                              |> Seq.map readEntries
                              |> Async.Parallel

                    files |> Seq.iter (logsRead.Add >> ignore)

                    let entries = Seq.concat entries

                    if not <| Seq.isEmpty entries then
                        updatedEvent.Trigger(this, entries)

            do! Async.Sleep pollingInterval
            return! loop ()
        }

        [<CLIEvent>]
        member this.Updated = updatedEvent.Publish

        member this.StartAsync () = async.Return <| Async.Start(loop (), cancellationToken = ct)
