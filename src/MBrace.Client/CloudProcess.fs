namespace Nessos.MBrace.Client

    open System
    open System.Reflection
    open System.Runtime.Serialization
    open System.Text

    open Nessos.Thespian
    open Nessos.Thespian.ConcurrencyTools

    open Nessos.Vagrant

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.PrettyPrinters
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Store
    open Nessos.MBrace.Runtime.Logging
    open Nessos.MBrace.Runtime.Utils

    open Nessos.MBrace.Client.Reporting
    
    open Microsoft.FSharp.Quotations

    type internal RuntimeMsg = MBraceNode
    type internal ProcessManagerMsg = ProcessManager
    type internal OSProcess = System.Diagnostics.Process

    type ProcessResult<'T> =
        | Pending
        | InitError of exn
        | Success of 'T
        | UserException of exn
        | Fault of exn
        | Killed
    with
        member r.TryGetValue () =
            match r with
            | Pending -> None
            | Success t -> Some t
            | InitError e -> mfailwithInner e "process initialization error."
            | UserException e -> reraise' e
            | Fault e -> reraise' e
            | Killed -> mfailwith "process has been user terminated."

        static member internal OfProcessInfo(info : ProcessInfo) =
            match info.ResultRaw with
            | ProcessResultImage.Pending -> Pending
            | ProcessResultImage.InitError e -> InitError e
            | ProcessResultImage.Success _ as result -> Success (ProcessResultImage.GetUserValue result :?> 'T)
            // CloudDump is discarded here; maybe use somehow?
            | ProcessResultImage.UserException _ as result -> UserException(ProcessResultImage.GetUserExceptionInfo result |> fst)
            | ProcessResultImage.Fault e -> Fault e
            | ProcessResultImage.Killed -> Killed

//    module internal ProcessInfo =
//
//        let getResult<'T> (info : ProcessInfo) =
//            match info.Result with
//            | None -> Pending
//            | Some(ProcessSuccess bytes) ->
//                match Serialization.Deserialize<Result<obj>> bytes with
//                | ValueResult v -> Success (v :?> 'T)
//                | ExceptionResult (e, ctx) -> UserException e
//            | Some(ProcessFault e) -> Fault e
//            | Some(ProcessInitError e) -> InitError e
//            | Some(ProcessKilled) -> Killed
//
//        let getReturnType (info : ProcessInfo) = Serialization.Deserialize<Type> info.Type
//
//        let prettyPrint =
//
//            let template : Field<ProcessInfo> list =
//                [
//                    Field.create "Name" Left (fun p -> p.Name)
//                    Field.create "Process Id" Right (fun p -> p.ProcessId)
//                    Field.create "Status" Left (fun p -> p.ProcessState)
//                    Field.create "#Workers" Right (fun p -> p.Workers)
//                    Field.create "#Tasks" Right (fun p -> p.Workers)
//                    Field.create "Start Time" Left (fun p -> p.InitTime)
//                    Field.create "Execution Time" Left (fun p -> p.ExecutionTime)
//                    Field.create "Result Type" Left (fun p -> p.TypeName)
//                ]
//
//            Record.prettyPrint3 template None

    [<AbstractClass>]
    type Process internal (processId : ProcessId, returnType : Type, processManager : ProcessManager) =

        let processInfo = CacheAtom.Create(fun () -> processManager.GetProcessInfo(processId) |> Async.RunSynchronously)

        abstract AwaitBoxedResultAsync : ?pollingInterval:int -> Async<obj>
        abstract AwaitBoxedResult : ?pollingInterval:int -> obj
        abstract TryGetBoxedResult : unit -> obj option

        member p.Name : string = processInfo.Value.Name
        member p.ProcessId : ProcessId = processId
        member p.ReturnType : Type = returnType
        member internal p.ProcessInfo : ProcessInfo = processInfo.Value
        member p.ExecutionTime : TimeSpan = processInfo.Value.ExecutionTime
        member p.Complete : bool = processInfo.Value.ResultRaw <> ProcessResultImage.Pending
        member p.InitTime : DateTime = processInfo.Value.InitTime
        member p.Workers : int = processInfo.Value.Workers
        member p.Tasks : int = processInfo.Value.Tasks
        member p.ClientId : Guid = processInfo.Value.ClientId

        static member internal CreateUntyped(t : Type, processId : ProcessId, processManager : ProcessManager) =
            let existential = Existential.Create t
            let ctor =
                {
                    new IFunc<Process> with
                        member __.Invoke<'T> () = new Process<'T>(processId, processManager) :> Process
                }

            existential.Apply ctor

        // TODO : should only be printable in shell mode
        member p.ShowInfo (?useBorders) = MBraceProcessReporter.Report [processInfo.Value]

        member p.Kill () : unit = processManager.Kill processId |> Async.RunSynchronously
        member p.GetLogs () : CloudLogEntry [] = p.GetLogsAsync () |> Async.RunSynchronously
        member p.GetLogsAsync () : Async<CloudLogEntry []> = async {
            let reader = StoreCloudLogger.GetReader(processManager.RuntimeStore, processId)
            let! logs = reader.FetchLogs()
            return logs |> Array.sortBy (fun e -> e.Date)
        }

        member p.DeleteLogs () : unit = Async.RunSynchronously <| p.DeleteLogsAsync()
        member p.DeleteLogsAsync () : Async<unit> = async {
            let reader = StoreCloudLogger.GetReader(processManager.RuntimeStore, processId)
            do! reader.DeleteLogs()
        }

        member p.ShowLogs () : unit =
            p.GetLogs () 
            |> Array.map (fun l -> l.ToSystemLogEntry(processId))
            |> Logs.show

        member p.StreamLogs () : unit = Async.RunSynchronously <| p.StreamLogsAsync()
        member p.StreamLogsAsync () : Async<unit> = 
            async {
                use cts = new Threading.CancellationTokenSource()
                let interval = 100
                let rec pollingLoop () = async {
                    if not p.Complete then
                        do! Async.Sleep interval
                        return! pollingLoop ()
                    else 
                        cts.Cancel()
                } 
                
                let reader = StoreCloudLogger.GetStreamingReader(processManager.RuntimeStore, processId, cts.Token)
                
                reader.Updated.Add(fun (_, logs) -> logs |> Seq.map (fun l -> l.ToSystemLogEntry processId) |> Logs.show) 

                do! reader.StartAsync()
                do! pollingLoop () 

            }

        member p.DeleteContainer() : unit =
            p.DeleteContainerAsync()
            |> Async.RunSynchronously

        member p.DeleteContainerAsync() : Async<unit> =
            async {
                let store = MBraceSettings.StoreInfo.Store
                return! store.DeleteContainer(sprintf' "process%d" p.ProcessId)
            }

    and [<Sealed; NoEquality ; NoComparison ; AutoSerializable(false)>] 
      Process<'T> internal (id : ProcessId, processManager : ProcessManager) =
        inherit Process(id, typeof<'T>, processManager)

        member p.Result : ProcessResult<'T> =
            let info = base.ProcessInfo
            ProcessResult<'T>.OfProcessInfo info

        member p.TryGetResult () : 'T option = p.Result.TryGetValue()
        member p.AwaitResultAsync(?pollingInterval) : Async<'T> = async {
            let pollingInterval = defaultArg pollingInterval 200
            let rec retriable () = async {
                match p.Result.TryGetValue() with
                | None ->
                    do! Async.Sleep pollingInterval
                    return! retriable ()
                | Some v -> return v
            }

            return! retriable ()
        }

        member p.AwaitResult(?pollingInterval) : 'T = 
            p.AwaitResultAsync(?pollingInterval = pollingInterval)
            |> Async.RunSynchronously

        override p.TryGetBoxedResult () = p.TryGetResult() |> Option.map (fun r -> r :> obj)
        override p.AwaitBoxedResultAsync (?pollingInterval) = async {
            let! r = p.AwaitResultAsync(?pollingInterval = pollingInterval)
            return r :> obj
        }
        override p.AwaitBoxedResult (?pollingInterval) =
            p.AwaitBoxedResultAsync(?pollingInterval = pollingInterval)
            |> Async.RunSynchronously


    and internal ProcessManager (processManagerF : unit -> ActorRef<ProcessManagerMsg>, storeInfo : StoreInfo) =
        
        // keep track of process id's handled by process manager
        let processes = Atom.atom Set.empty<ProcessId>

        let post (msg : ProcessManagerMsg) =
            try
                do processManagerF().Post msg
            with
            | MBraceExn e -> reraise' e
            | :? CommunicationException as e -> mfailwithInner e "Cannot communicate with runtime."
            | MessageHandlingExceptionRec e -> mfailwithInner e "Runtime replied with exception."

        let postWithReply (msgB : IReplyChannel<'R> -> ProcessManagerMsg) = async {
            try
                return! processManagerF() <!- msgB
            with
            | MBraceExn e -> return reraise' e
            | :? CommunicationException as e -> return mfailwithInner e "Cannot communicate with runtime."
            | MessageHandlingExceptionRec e ->
                return! mfailwithInner e "Runtime replied with exception."

        }

        member pm.RuntimeStore = storeInfo.Store

        member pm.GetProcessInfo (pid : ProcessId) : Async<ProcessInfo> = 
            postWithReply(fun ch -> GetProcessInfo(ch, pid))

        member pm.DownloadProcessDependencies (pid : ProcessId) = async {
            
            if processes.Value.Contains pid then return () else

            // vagrant components for the assembly fetch protocol
            let dependencyDownloader =
                {
                    new IRemoteAssemblyPublisher with
                        member __.GetRequiredAssemblyInfo () = async {
                            let! processInfo = postWithReply <| fun ch -> GetProcessInfo(ch, pid)

                            return 
                                if processInfo.ClientId = MBraceSettings.ClientId then []
                                else
                                    processInfo.Dependencies
                        }

                        member __.PullAssemblies (ids : AssemblyId list) = 
                            postWithReply <| fun ch -> RequestDependencies(ch, ids)
                }

            do! MBraceSettings.Vagrant.Client.ReceiveDependencies dependencyDownloader

            processes.Swap(fun ps -> ps.Add pid)
        }

        member pm.GetProcess (pid : ProcessId) = async {
            do! pm.DownloadProcessDependencies pid
            let! info = postWithReply <| fun ch -> GetProcessInfo(ch, pid)
            let returnType = info.Type

            return Process.CreateUntyped(returnType, pid, pm)
        }

        member pm.GetAllProcesses () = async {
            let! infos = postWithReply GetAllProcessInfo

            return!
                infos
                |> Array.map (fun info -> pm.GetProcess info.ProcessId)
                |> Async.Parallel
        }            

        member pm.ClearProcessInfo (pid : ProcessId) = postWithReply <| fun ch -> ClearProcessInfo(ch, pid)
        member pm.ClearAllProcessInfo () = postWithReply ClearAllProcessInfo

        member pm.CreateProcess<'T> (comp : CloudComputation<'T>) : Async<Process<'T>> = async {
            let requestId = Guid.NewGuid()

            let dependencyUploader =
                {
                    new IRemoteAssemblyReceiver with
                        member __.GetLoadedAssemblyInfo (ids : AssemblyId list) =
                            postWithReply <| fun ch -> GetAssemblyLoadInfo(ch, requestId, ids)

                        member __.PushAssemblies (pas : PortableAssembly list) =
                            postWithReply <| fun ch -> LoadAssemblies(ch, requestId, pas)
                }

            // serialization errors for dynamic assemblies
            let! errors = MBraceSettings.Vagrant.SubmitAssemblies(dependencyUploader, comp.Dependencies)

            let rawImage = MBraceSettings.CloudCompiler.GetRawImage comp

            let! info = postWithReply <| fun ch -> CreateDynamicProcess(ch, requestId, rawImage)

            processes.Swap(fun ps -> ps.Add info.ProcessId)

            return Process<'T>(info.ProcessId, pm)
        }

        member pm.Kill (pid : ProcessId) = postWithReply <| fun ch -> KillProcess(ch, pid)

        // TODO : should only be printable in shell mode
        member pm.ShowInfo (?useBorders) =
            let info = postWithReply GetAllProcessInfo |> Async.RunSynchronously |> Array.toList
            MBraceProcessReporter.Report(info, ?showBorder = useBorders)
            |> Console.WriteLine