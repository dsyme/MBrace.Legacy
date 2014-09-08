namespace Nessos.MBrace.Client

    open System
    open System.Reflection
    open System.Runtime.Serialization
    open System.Text

    open Nessos.Thespian
    open Nessos.Thespian.ConcurrencyTools

    open Nessos.Vagrant

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.PrettyPrinters
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Compiler
    open Nessos.MBrace.Runtime.Logging
    open Nessos.MBrace.Runtime.Utils

    open Nessos.MBrace.Client.Reporting
    
    open Microsoft.FSharp.Quotations

    type internal ProcessManagerMsg = ProcessManager

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
    /// Type representing a cloud process submitted to the runtime.
    type Process internal (processId : ProcessId, returnType : Type, processManager : ProcessManager) =

        let processInfo = CacheAtom.Create(fun () -> processManager.GetProcessInfo(processId) |> Async.RunSynchronously)

        ///<summary>Wait for the process result.</summary>
        ///<param name="pollingInterval">The number of milliseconds to poll for a result.</param>
        abstract AwaitBoxedResultAsync : ?pollingInterval:int -> Async<obj>
        
        ///<summary>Wait for the process result.</summary>
        ///<param name="pollingInterval">The number of milliseconds to poll for a result.</param>
        abstract AwaitBoxedResult : ?pollingInterval:int -> obj
        
        /// Try retrieve the process result. Returns None if the process is not completed.
        abstract TryGetBoxedResult : unit -> obj option

        /// Process name.
        member p.Name : string = processInfo.Value.Name

        /// Runtime process identifier.
        member p.ProcessId : ProcessId = processId

        /// Process result type.
        member p.ReturnType : Type = returnType
        member internal p.ProcessInfo : ProcessInfo = processInfo.Value

        /// Amount of time current process has been running.
        member p.ExecutionTime : TimeSpan = processInfo.Value.ExecutionTime

        /// Gets whether the process is completed.
        member p.Complete : bool = processInfo.Value.ResultRaw <> ProcessResultImage.Pending
        
        /// Process creation date/time.
        member p.InitTime : DateTime = processInfo.Value.InitTime
        
        /// Number of workers currently occupied with this process.
        member p.Workers : int = processInfo.Value.Workers

        /// Number of tasks currently occupied with this process.
        member p.Tasks : int = processInfo.Value.Tasks

        /// UUID identifying the client that created this process.
        member p.ClientId : Guid = processInfo.Value.ClientId

        static member internal CreateUntyped(t : Type, processId : ProcessId, processManager : ProcessManager) =
            let existential = Existential.Create t
            let ctor =
                {
                    new IFunc<Process> with
                        member __.Invoke<'T> () = new Process<'T>(processId, processManager) :> Process
                }

            existential.Apply ctor

        /// Returns printed information on the cloud process.
        member p.GetInfo () : string = ProcessReporter.Report [processInfo.Value]
        
        /// Prints information on the cloud process to StdOut.
        member p.ShowInfo () : unit = p.GetInfo() |> Console.WriteLine

        /// Asynchronously kill the process.
        member p.KillAsync () : Async<unit> = processManager.Kill processId

        /// Kill the process.
        member p.Kill () : unit = p.KillAsync() |> Async.RunSynchronously
        
        /// Asynchronously gets all user generated logs by the process.
        member p.GetLogsAsync () : Async<CloudLogEntry []> = async {
            let reader = StoreCloudLogger.GetReader(processManager.RuntimeStore, processId)
            let! logs = reader.FetchLogs()
            return logs |> Array.sortBy (fun e -> e.Date)
        }

        /// Gets all user generated logs by the process.
        member p.GetLogs () : CloudLogEntry [] = p.GetLogsAsync () |> Async.RunSynchronously

        /// Asynchronously deletes any user logs created by the process.
        member p.DeleteLogsAsync () : Async<unit> = async {
            let reader = StoreCloudLogger.GetReader(processManager.RuntimeStore, processId)
            do! reader.DeleteLogs()
        }

        /// Deletes any user logs created by the process.
        member p.DeleteLogs () : unit = p.DeleteLogsAsync() |> Async.RunSynchronously 

        /// Asynchronously prints all user logs created by the process.
        member p.ShowLogsAsync () : Async<unit> =
            async { 
                let! logs = p.GetLogsAsync()
                do logs |> Array.map (fun l -> l.ToSystemLogEntry(processId))
                        |> Logs.show
            }

        /// Prints all user logs created by the process.
        member p.ShowLogs () : unit = p.ShowLogsAsync() |> Async.RunSynchronously

        /// Blocking operation that prints user logs to StdOut as generated by the process.
        member p.StreamLogs () : unit = p.StreamLogsAsync() |> Async.RunSynchronously

        /// Asynchronous operation that prints user logs to StdOut as generated by the process.
        member p.StreamLogsAsync () : Async<unit> = 
            async {
                use cts = new Threading.CancellationTokenSource()
                let interval = 100

                let reader = StoreCloudLogger.GetStreamingReader(processManager.RuntimeStore, processId, cts.Token)

                let rec pollingLoop () = async {
                    if not p.Complete then
                        do! Async.Sleep interval
                        return! pollingLoop ()
                    else 
                        cts.Cancel()
                        do! reader.GetLogsAsync()
                } 
                
                reader.Updated.Add(fun (_,logs) -> logs |> Seq.map (fun (l : CloudLogEntry) -> l.ToSystemLogEntry processId) |> Logs.show)  

                do! reader.StartAsync()
                do! pollingLoop () 

            }

        /// Deletes the container used by this process in the store.
        member p.DeleteContainer() : unit = p.DeleteContainerAsync() |> Async.RunSynchronously

        /// Asynchronously deletes the container used by this process in the store.
        member p.DeleteContainerAsync() : Async<unit> =
            async {
                return! processManager.RuntimeStore.DeleteContainer(sprintf' "process%d" p.ProcessId)
            }

    /// Type representing a cloud process submitted to the runtime.
    and [<Sealed; NoEquality ; NoComparison ; AutoSerializable(false)>] 
      Process<'T> internal (id : ProcessId, processManager : ProcessManager) =
        inherit Process(id, typeof<'T>, processManager)

        /// Gets the process result.
        member p.Result : ProcessResult<'T> =
            let info = base.ProcessInfo
            ProcessResult<'T>.OfProcessInfo info

        /// Try retreiving the process result. Returns None if the process is not completed.
        member p.TryGetResult () : 'T option = p.Result.TryGetValue()
        
        /// <summary>
        ///      Asynchronously awaits the process result.
        /// </summary>
        /// <param name="pollingInterval">Result polling interval in milliseconds.</param>
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

        /// <summary>
        ///     Synchronously awaits the process result.
        /// </summary>
        /// <param name="pollingInterval">The number of milliseconds to poll for a result.</param>
        member p.AwaitResult(?pollingInterval) : 'T = 
            p.AwaitResultAsync(?pollingInterval = pollingInterval)
            |> Async.RunSynchronously

        /// Try retrieving the process result. Returns None if the process is not completed.
        override p.TryGetBoxedResult () = p.TryGetResult() |> Option.map (fun r -> r :> obj)
        
        /// <summary>
        ///      Asynchronously awaits the process result.
        /// </summary>
        /// <param name="pollingInterval">Result polling interval in milliseconds.</param> 
        override p.AwaitBoxedResultAsync (?pollingInterval) = async {
            let! r = p.AwaitResultAsync(?pollingInterval = pollingInterval)
            return r :> obj
        }

        /// <summary>
        ///     Synchronously awaits the process result.
        /// </summary>
        /// <param name="pollingInterval">The number of milliseconds to poll for a result.</param>
        override p.AwaitBoxedResult (?pollingInterval) =
            p.AwaitBoxedResultAsync(?pollingInterval = pollingInterval)
            |> Async.RunSynchronously

    /// <summary>
    ///     Provides a basic process management logic for a given runtime.
    /// </summary>
    and internal ProcessManager (processManagerF : unit -> ActorRef<ProcessManagerMsg>, storeInfoF : unit -> StoreInfo) =
        
        // keep track of process id's handled by process manager
        let processes = Atom.atom Set.empty<ProcessId>

        let post (msg : ProcessManagerMsg) =
            try
                do processManagerF().Post msg
            with
            | MBraceExn e -> reraise' e
            // temporary solution due to Thespian issue
            | :? System.Net.Sockets.SocketException as e -> mfailwithInner e "Cannot communicate with runtime."
            | :? CommunicationException as e -> mfailwithInner e "Cannot communicate with runtime."
            | MessageHandlingExceptionRec e -> mfailwithInner e "Runtime replied with exception."

        let postWithReply (msgB : IReplyChannel<'R> -> ProcessManagerMsg) = async {
            try
                return! processManagerF().PostWithReply(msgB, MBraceSettings.DefaultTimeout)
            with
            | MBraceExn e -> return reraise' e
            // temporary solution due to Thespian issue
            | :? System.Net.Sockets.SocketException as e -> return mfailwithInner e "Cannot communicate with runtime."
            | :? CommunicationException as e -> return mfailwithInner e "Cannot communicate with runtime."
            | MessageHandlingExceptionRec e ->
                return! mfailwithInner e "Runtime replied with exception."

        }

        /// <summary>
        ///     CloudStore instance used by runtime.
        /// </summary>
        member pm.RuntimeStore = storeInfoF().Store

        /// <summary>
        ///     Asynchronously retrieves a ProcessInfo record from the runtime.
        /// </summary>
        /// <param name="pid">ProcessId identifier.</param>
        member pm.GetProcessInfo (pid : ProcessId) : Async<ProcessInfo> = 
            postWithReply(fun ch -> GetProcessInfo(ch, pid))

        /// <summary>
        ///     Downloads Vagrant dependencies for a running process if required.
        /// </summary>
        /// <param name="pid"></param>
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

            do! VagrantRegistry.Instance.ReceiveDependencies dependencyDownloader

            processes.Swap(fun ps -> ps.Add pid)
        }

        /// <summary>
        ///     Gets a client process object for given ProcessId.
        /// </summary>
        /// <param name="pid"></param>
        member pm.GetProcess (pid : ProcessId) = async {
            do! pm.DownloadProcessDependencies pid
            let! info = postWithReply <| fun ch -> GetProcessInfo(ch, pid)
            let returnType = info.Type

            return Process.CreateUntyped(returnType, pid, pm)
        }

        /// <summary>
        ///     Gets a collection of client objects for all running or completed processes in runtime.
        /// </summary>
        member pm.GetAllProcesses () = async {
            let! infos = postWithReply GetAllProcessInfo

            return!
                infos
                |> Array.map (fun info -> pm.GetProcess info.ProcessId)
                |> Async.Parallel
        }            

        /// <summary>
        ///     Erase all completed process info from runtime.
        /// </summary>
        /// <param name="pid"></param>
        member pm.ClearProcessInfo (pid : ProcessId) = postWithReply <| fun ch -> ClearProcessInfo(ch, pid)

        /// <summary>
        ///     Erase all process info from runtime.
        /// </summary>
        member pm.ClearAllProcessInfo () = postWithReply ClearAllProcessInfo

        /// <summary>
        ///     Initializes a new cloud workflow to runtime and asynchronously returns a process object.
        /// </summary>
        /// <param name="comp">Cloud workflow.</param>
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
            let! errors = VagrantRegistry.Instance.SubmitAssemblies(dependencyUploader, comp.Dependencies)

            let rawImage = CloudCompiler.GetRawImage comp

            let! info = postWithReply <| fun ch -> CreateDynamicProcess(ch, requestId, rawImage)

            processes.Swap(fun ps -> ps.Add info.ProcessId)

            return Process<'T>(info.ProcessId, pm)
        }

        /// <summary>
        ///     Violently kill a remote process.
        /// </summary>
        /// <param name="pid"></param>
        member pm.Kill (pid : ProcessId) = postWithReply <| fun ch -> KillProcess(ch, pid)

        /// <summary>
        ///     Get printed info for all processes in runtime.
        /// </summary>
        member pm.GetInfoAsync () : Async<string> =
            async {
                let! info = postWithReply GetAllProcessInfo 
                return ProcessReporter.Report(Array.toList info, showBorder = false)
            }