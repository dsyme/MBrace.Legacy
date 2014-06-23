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

    /// Represents a process' result.
    type ProcessResult<'T> =
        /// Process is still running.
        | Pending
        /// Process could not be created.
        | InitError of exn
        /// Process completed successfully.
        | Success of 'T
        /// Process completed with an exception thrown by user code.
        | UserException of exn
        /// Runtime fault.
        | Fault of exn
        /// Process killed by user.
        | Killed
    with
        /// Try retrieve the process' result.
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

    [<AbstractClass>]
    /// The type representing a process submitted to the runtime.
    type Process internal (processId : ProcessId, returnType : Type, processManager : ProcessManager) =

        let processInfo = CacheAtom.Create(fun () -> processManager.GetProcessInfo(processId) |> Async.RunSynchronously)

        ///<summary>Wait for the process's result.</summary>
        ///<param name="pollingInterval">The number of milliseconds to poll for a result.</param>
        abstract AwaitBoxedResultAsync : ?pollingInterval:int -> Async<obj>
        
        ///<summary>Wait for the process's result.</summary>
        ///<param name="pollingInterval">The number of milliseconds to poll for a result.</param>
        abstract AwaitBoxedResult : ?pollingInterval:int -> obj
        
        /// Try retreive the process's result. Returns None if the process is not completed.
        abstract TryGetBoxedResult : unit -> obj option

        /// Process's name.
        member p.Name : string = processInfo.Value.Name

        /// Process's identifier.
        member p.ProcessId : ProcessId = processId

        /// The type of process's result.
        member p.ReturnType : Type = returnType
        member internal p.ProcessInfo : ProcessInfo = processInfo.Value

        /// The amount of time this process is running.
        member p.ExecutionTime : TimeSpan = processInfo.Value.ExecutionTime

        /// Gets whether the process is completed.
        member p.Complete : bool = processInfo.Value.ResultRaw <> ProcessResultImage.Pending
        
        /// The date the process was created.
        member p.InitTime : DateTime = processInfo.Value.InitTime
        
        /// The number of workers used by this process.
        member p.Workers : int = processInfo.Value.Workers

        /// The number of tasks created by the process.
        member p.Tasks : int = processInfo.Value.Tasks

        /// The identifier of the client that created this process.
        member p.ClientId : Guid = processInfo.Value.ClientId

        static member internal CreateUntyped(t : Type, processId : ProcessId, processManager : ProcessManager) =
            let existential = Existential.Create t
            let ctor =
                {
                    new IFunc<Process> with
                        member __.Invoke<'T> () = new Process<'T>(processId, processManager) :> Process
                }

            existential.Apply ctor

        /// Returns information about this cloud process.
        member p.GetInfo () : string = MBraceProcessReporter.Report [processInfo.Value]
        
        /// Prints information about this cloud process.
        member p.ShowInfo () : unit = p.GetInfo() |> Console.WriteLine

        /// Kill the process.
        member p.Kill () : unit = processManager.Kill processId |> Async.RunSynchronously
        
        /// Gets all user logs created by this process.
        member p.GetLogs () : CloudLogEntry [] = p.GetLogsAsync () |> Async.RunSynchronously
        /// Gets all user logs created by this process.
        member p.GetLogsAsync () : Async<CloudLogEntry []> = async {
            let reader = StoreCloudLogger.GetReader(processManager.RuntimeStore, processId)
            let! logs = reader.FetchLogs()
            return logs |> Array.sortBy (fun e -> e.Date)
        }

        /// Deletes any user logs created by this process.
        member p.DeleteLogs () : unit = Async.RunSynchronously <| p.DeleteLogsAsync()
        /// Deletes any user logs created by this process.
        member p.DeleteLogsAsync () : Async<unit> = async {
            let reader = StoreCloudLogger.GetReader(processManager.RuntimeStore, processId)
            do! reader.DeleteLogs()
        }

        /// Prints all user logs created by this process.
        member p.ShowLogs () : unit =
            p.GetLogs () 
            |> Array.map (fun l -> l.ToSystemLogEntry(processId))
            |> Logs.show

        /// Prints the stream of user logs as they are being created by the process.
        member p.StreamLogs () : unit = Async.RunSynchronously <| p.StreamLogsAsync()
        /// Prints the stream of user logs as they are being created by the process.
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

        /// Deletes the container used by this process in the store.
        member p.DeleteContainer() : unit = p.DeleteContainerAsync() |> Async.RunSynchronously
        /// Deletes the container used by this process in the store.
        member p.DeleteContainerAsync() : Async<unit> =
            async {
                return! processManager.RuntimeStore.DeleteContainer(sprintf' "process%d" p.ProcessId)
            }

    /// The type representing a process submitted to the runtime.
    and [<Sealed; NoEquality ; NoComparison ; AutoSerializable(false)>] 
      Process<'T> internal (id : ProcessId, processManager : ProcessManager) =
        inherit Process(id, typeof<'T>, processManager)

        /// Gets the process' result.
        member p.Result : ProcessResult<'T> =
            let info = base.ProcessInfo
            ProcessResult<'T>.OfProcessInfo info

        /// Try retreive the process's result. Returns None if the process is not completed.
        member p.TryGetResult () : 'T option = p.Result.TryGetValue()
        
        ///<summary>Waits for the process's result.</summary>
        ///<param name="pollingInterval">The number of milliseconds to poll for a result.</param>
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

        ///<summary>Waits for the process's result.</summary>
        ///<param name="pollingInterval">The number of milliseconds to poll for a result.</param>
        member p.AwaitResult(?pollingInterval) : 'T = 
            p.AwaitResultAsync(?pollingInterval = pollingInterval)
            |> Async.RunSynchronously

        /// Try retreive the process's result. Returns None if the process is not completed.
        override p.TryGetBoxedResult () = p.TryGetResult() |> Option.map (fun r -> r :> obj)
        
        ///<summary>Waits for the process's result.</summary>
        ///<param name="pollingInterval">The number of milliseconds to poll for a result.</param>        
        override p.AwaitBoxedResultAsync (?pollingInterval) = async {
            let! r = p.AwaitResultAsync(?pollingInterval = pollingInterval)
            return r :> obj
        }

        ///<summary>Waits for the process's result.</summary>
        ///<param name="pollingInterval">The number of milliseconds to poll for a result.</param>
        override p.AwaitBoxedResult (?pollingInterval) =
            p.AwaitBoxedResultAsync(?pollingInterval = pollingInterval)
            |> Async.RunSynchronously


    and internal ProcessManager (processManagerF : unit -> ActorRef<ProcessManagerMsg>, storeInfoF : unit -> StoreInfo) =
        
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

        member pm.RuntimeStore = storeInfoF().Store

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

        member pm.GetInfo () : string =
            let info = postWithReply GetAllProcessInfo |> Async.RunSynchronously |> Array.toList
            MBraceProcessReporter.Report(info, showBorder = false)