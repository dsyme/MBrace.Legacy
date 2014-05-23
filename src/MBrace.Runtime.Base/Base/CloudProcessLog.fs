namespace Nessos.MBrace.Runtime.Logging

    open System
    open System.Text

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils.String
    open Nessos.MBrace.Runtime.Store

    type CloudLogEntry =
        {
            Date : DateTime
            TaskId : TaskId
            Message : string
            TraceInfo : TraceInfo option
        }
    with
        static member UserInfo msg taskId = { Date = DateTime.Now ; TaskId = taskId ; Message = msg ; TraceInfo = None }
        static member Trace msg taskId traceInfo = { Date = DateTime.Now ; TaskId = taskId ; Message = msg ; TraceInfo = Some traceInfo }

        member e.ToSystemLogEntry (processId : ProcessId) =
            let message =
                match e.TraceInfo with
                | None ->
                    sprintf "[Cloud Process %A][Task %s] User Message: %s" processId e.TaskId e.Message
                | Some tI ->
                    String.build(fun sb ->
                        String.append sb <| sprintf "[Cloud Process %A][Task %s] Trace" processId e.TaskId
                        match tI.File with 
                        | None -> () 
                        | Some f -> String.append sb <| sprintf ", File:%s" f 

                        match tI.Line with
                        | None -> ()
                        | Some l -> String.append sb <| sprintf ", Line:%d" l

                        String.append sb <| sprintf ": %s\n" e.Message
                        String.append sb <| "--- Begin environment dump ---\n"
                        for KeyValue(n,v) in tI.Environment do
                            String.append sb <| sprintf "  val %s = %s\n" n v
                        String.append sb <| "--- End  environment  dump ---\n")

            { Date = e.Date ; Message = message ; Level = Info }

    /// Store interface for cloud process logs

    type StoreCloudLogger(store : ICloudStore, processId : ProcessId, taskId : TaskId) =
        inherit LogStore<CloudLogEntry>(store, container = sprintf "cloudProc%d" processId, logPrefix = sprintf "task_%s" taskId)

        static member GetReader(store : ICloudStore, processId : ProcessId) =
            new LogStoreReader<CloudLogEntry>(store, container = sprintf "cloudProc%d" processId)

        static member GetStreamingReader(store : ICloudStore, processId : ProcessId) =
            new StreamingLogReader<CloudLogEntry>(store, container = sprintf "cloudProc%d" processId)

    /// The runtime ICloudLogger implementation

    type RuntimeCloudProcessLogger(processId : ProcessId, taskId : TaskId, ?sysLog : ISystemLogger, ?store : ICloudStore) =
        let storeLogger = store |> Option.map (fun s -> new StoreCloudLogger(s, processId, taskId))

        let logEntry (e : CloudLogEntry) =
            match sysLog with Some s -> s.LogEntry (e.ToSystemLogEntry processId) | None -> ()
            match storeLogger with Some s -> s.LogEntry e | None -> ()

        interface ICloudLogger with
            member __.LogTraceInfo (msg, taskId, traceInfo) = logEntry <| CloudLogEntry.Trace msg taskId traceInfo
            member __.LogUserInfo (msg, taskId) = logEntry <| CloudLogEntry.UserInfo msg taskId

        interface IDisposable with
            member __.Dispose () = storeLogger |> Option.iter (fun s -> (s :> IDisposable).Dispose())


    type InMemoryCloudProcessLogger(sysLog : ISystemLogger, processId : ProcessId) =

        let logEntry (e : CloudLogEntry) = sysLog.LogEntry (e.ToSystemLogEntry processId)

        interface ICloudLogger with
            member __.LogTraceInfo (msg, taskId, traceInfo) = logEntry <| CloudLogEntry.Trace msg taskId traceInfo
            member __.LogUserInfo (msg, taskId) = logEntry <| CloudLogEntry.UserInfo msg taskId 


    type NullCloudProcessLogger () =
        interface ICloudLogger with
            member __.LogTraceInfo (_,_,_) = ()
            member __.LogUserInfo (_,_) = ()