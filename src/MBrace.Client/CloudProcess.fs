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

    module internal ProcessInfo =

        let getResult<'T> (info : ProcessInfo) =
            match info.Result with
            | None -> Pending
            | Some(ProcessSuccess bytes) ->
                match Serialization.Deserialize<Result<obj>> bytes with
                | ValueResult v -> Success (v :?> 'T)
                | ExceptionResult (e, ctx) -> UserException e
            | Some(ProcessFault e) -> Fault e
            | Some(ProcessInitError e) -> InitError e
            | Some(ProcessKilled) -> Killed

        let getReturnType (info : ProcessInfo) = Serialization.Deserialize<Type> info.Type

        let prettyPrint =

            let template : Field<ProcessInfo> list =
                [
                    Field.create "Name" Left (fun p -> p.Name)
                    Field.create "Process Id" Right (fun p -> p.ProcessId)
                    Field.create "Status" Left (fun p -> p.ProcessState)
                    Field.create "#Workers" Right (fun p -> p.Workers)
                    Field.create "#Tasks" Right (fun p -> p.Workers)
                    Field.create "Start Time" Left (fun p -> p.InitTime)
                    Field.create "Execution Time" Left (fun p -> p.ExecutionTime)
                    Field.create "Result Type" Left (fun p -> p.TypeName)
                ]

            Record.prettyPrint3 template None

    [<AbstractClass>]
    type Process internal (processId : ProcessId, returnType : Type, processManager : ProcessManager) =

        let processInfo = CacheAtom.Create(fun () -> processManager.GetProcessInfo(processId) |> Async.RunSynchronously)

        abstract AwaitBoxedResultAsync : ?pollingInterval:int -> Async<obj>
        abstract AwaitBoxedResult : ?pollingInterval:int -> obj
        abstract TryGetBoxedResult : unit -> obj option

        member p.Name = processInfo.Value.Name
        member p.ProcessId = processId
        member p.ReturnType = returnType
        member internal p.ProcessInfo = processInfo.Value
        member p.ExecutionTime = processInfo.Value.ExecutionTime
        member p.Complete = processInfo.Value.Result.IsSome
        member p.InitTime = processInfo.Value.InitTime
        member p.Workers = processInfo.Value.Workers
        member p.Tasks = processInfo.Value.Tasks
        member p.ClientId = processInfo.Value.ClientId

        static member internal CreateUntyped(t : Type, processId : ProcessId, processManager : ProcessManager) =
            let existential = Existential.Create t
            let ctor =
                {
                    new IFunc<Process> with
                        member __.Invoke<'T> () = new Process<'T>(processId, processManager) :> Process
                }

            existential.Apply ctor

        // TODO : only printable in shell mode
        member p.ShowInfo (?useBorders) =
            let useBorders = defaultArg useBorders false
            [processInfo.Value] |> ProcessInfo.prettyPrint useBorders |> printfn "%s"

        member p.Kill () = processManager.Kill processId |> Async.RunSynchronously
        member p.GetLogs () = p.GetLogsAsync () |> Async.RunSynchronously
        member p.GetLogsAsync () = async {
            let reader = StoreCloudLogger.GetReader(processManager.RuntimeStore, processId)
            let! logs = reader.FetchLogs()
            return logs |> Array.sortBy (fun e -> e.Date)
        }

        member p.DeleteLogs () = Async.RunSynchronously <| p.DeleteLogsAsync()
        member p.DeleteLogsAsync () = async {
            let reader = StoreCloudLogger.GetReader(processManager.RuntimeStore, processId)
            do! reader.DeleteLogs()
        }

        member p.ShowLogs () = 
            p.GetLogs () 
            |> Array.map (fun l -> l.ToSystemLogEntry(processId).Print(showDate = false))
            |> String.concat "\n"
            |> printfn "%s"

        member p.StreamLogs () = Async.RunSynchronously <| p.StreamLogsAsync()
        member p.StreamLogsAsync () = 
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
                
                reader.Updated.Add(fun (_, logs) -> 
                    logs |> Seq.map (fun l -> l.ToSystemLogEntry(processId).Print(showDate = true))
                         |> String.concat "\n"
                         |> printfn "%s" ) 

                do! reader.StartAsync()
                do! pollingLoop () 

            }

        member p.DeleteContainer() =
            p.DeleteContainerAsync()
            |> Async.RunSynchronously

        member p.DeleteContainerAsync() =
            async {
                let store = MBraceSettings.StoreInfo.Store
                return! store.DeleteContainer(sprintf' "process%d" p.ProcessId)
            }

    and [<Sealed>][<AutoSerializable(false)>] Process<'T> internal (id : ProcessId, processManager : ProcessManager) =
        inherit Process(id, typeof<'T>, processManager)

        member p.Result =
            let info = base.ProcessInfo
            ProcessInfo.getResult<'T> info

        member p.TryGetResult () = p.Result.TryGetValue()
        member p.AwaitResultAsync(?pollingInterval) = async {
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

        member p.AwaitResult(?pollingInterval) = 
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


    and internal ProcessManager (runtime: ActorRef<ClientRuntimeProxy>, storeInfo : StoreInfo) =
        
        // keep track of process id's handled by process manager
        let processes = Atom.atom Set.empty<ProcessId>

        let processManagerActor = CacheAtom.Create(fun () -> runtime <!= (RemoteMsg << GetProcessManager))

        let post (msg : ProcessManagerMsg) =
            try
                do processManagerActor.Value.Post msg
            with
            | MBraceExn e -> reraise' e
            | :? CommunicationException as e -> mfailwithInner e "Cannot communicate with runtime."
            | MessageHandlingExceptionRec e -> mfailwithInner e "Runtime replied with exception."

        let postWithReply (msgB : IReplyChannel<'R> -> ProcessManagerMsg) = async {
            try
                return! processManagerActor.Value <!- msgB
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
            let! info = processManagerActor.Value <!- fun ch -> GetProcessInfo(ch, pid)
            let returnType = ProcessInfo.getReturnType info

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
            let requestId = RequestId.NewGuid()

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

            let rawImage = comp.GetRawImage()

            let! info = postWithReply <| fun ch -> CreateDynamicProcess(ch, requestId, rawImage)

            processes.Swap(fun ps -> ps.Add info.ProcessId)

            return Process<'T>(info.ProcessId, pm)
        }

        member pm.Kill (pid : ProcessId) = postWithReply <| fun ch -> KillProcess(ch, pid)

        // TODO : only printable in shell mode!!
        member pm.ShowInfo (?useBorders) =
            let useBorders = defaultArg useBorders false
            postWithReply GetAllProcessInfo
            |> Async.RunSynchronously
            |> Array.toList
            |> ProcessInfo.prettyPrint useBorders 
            |> printfn "%s"