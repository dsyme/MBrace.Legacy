namespace Nessos.MBrace.Client

    open System
    open System.Diagnostics
    open System.IO

    open Nessos.Thespian
    open Nessos.Thespian.ConcurrencyTools
    open Nessos.Thespian.Remote.TcpProtocol
    open Nessos.Thespian.Remote.PipeProtocol
    open Nessos.Thespian.ActorExtensions.RetryExtensions

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.PrettyPrinters
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Utils
    open Nessos.MBrace.Runtime.Logging
    open Nessos.MBrace.Runtime.Daemon.Configuration
    
    open Nessos.MBrace.Client.Reporting

    type private MBraceNodeMsg = Nessos.MBrace.Runtime.MBraceNode

    /// Record containing Node performance metrics.
    type NodePerformanceInfo = Nessos.MBrace.Runtime.NodePerformanceInfo

    /// MBrace Node deployment permissions enumeration.
    type Permissions = Nessos.MBrace.Runtime.Permissions

    /// Provides a handle and administration API for remote MBrace nodes.
    [<Sealed; NoEquality; NoComparison; AutoSerializable(false)>]
    type MBraceNode private (nodeRef: ActorRef<MBraceNodeMsg>, uri : Uri) as self =

        static do MBraceSettings.Init()

        let handleError (e : exn) : 'T =
            match e with
            | MessageHandlingExceptionRec (MBraceExn e)
            | MBraceExn e -> reraise' e
            | CommunicationExceptionRec e -> mfailwithfInner e "Error communicating with node '%O'." self
            | MessageHandlingExceptionRec e -> mfailwithfInner e "Node '%O' has replied with exception." self
            | _ -> reraise' e

        let nodeInfo = 
            CacheAtom.Create(
                (fun () ->
                    let info : NodeDeploymentInfo = self.GetNodeInfoAsync(false) |> Async.RunSynchronously
                    let proc = info.TryGetLocalProcess()
                    info, proc), 
                        
                        interval = 100, keepLastResultOnError = true)

        internal new (nref: ActorRef<MBraceNodeMsg>) =
            let uri = ActorRef.toUri nref |> MBraceUri.actorUriToMbraceUri
            MBraceNode(nref, uri)

        internal new (info : NodeDeploymentInfo) = MBraceNode(info.Reference, info.Uri)

        /// Connect to an existing MBrace node.
        static member Connect(uri: Uri) =
            let nref = uri |> MBraceUri.mbraceUriToActorUri Serialization.SerializerRegistry.DefaultName |> ActorRef.fromUri
            let n = new MBraceNode(nref, uri)
            let _ = n.IsActive
            n

        /// Connect to an existing MBrace node.
        static member Connect(hostname : string, port : int) = 
            MBraceNode.Connect(MBraceUri.hostPortToUri(hostname, port))

        /// Connect to an existing MBrace node.
        static member Connect(uri : string) = MBraceNode.Connect(new Uri(uri))

        /// Gets the System.Diagnostics.Process object that corresponds to a local MBrace node.
        member __.Process : Process option = snd nodeInfo.Value
        member internal __.Ref = nodeRef
        
        /// Gets the Node uri.
        member __.Uri : Uri = uri

        /// Gets or sets the Node deployment permissions.
        member __.Permissions
            with get () : Permissions = (fst nodeInfo.Value).Permissions
            and  set (newPermissions: Permissions) =
                try nodeRef <-- SetNodePermissions newPermissions
                with e -> handleError e

        /// Gets or sets whether the Node can be run as slave in a cluster.
        member n.IsPermittedSlave
            with get() = n.Permissions.HasFlag Permissions.Slave
            and  set (x: bool) = n.Permissions <- Permissions.switch x Permissions.Slave n.Permissions

        /// Gets or sets whether the Node can be run as master in a cluster.
        member n.IsPermittedMaster
            with get () = n.Permissions.HasFlag Permissions.Slave
            and  set (x: bool) = n.Permissions <- Permissions.switch x Permissions.Slave n.Permissions 

        /// Gets the current deployment state of the Node.
        member n.State : NodeState = (fst nodeInfo.Value).State

        /// Gets a Guid that identifies the Node instance.
        member n.DeploymentId = (fst nodeInfo.Value).DeploymentId

        /// Gets whether the node is active in an MBrace cluster.
        member n.IsActive = n.State <> Idle

        /// <summary>
        ///     Asynchronously pings the remote node, returning the response timespan.
        /// </summary>
        /// <param name="timeout">The amount of time in milliseconds to wait for a reply from the node (default is 3 seconds).</param>
        member n.PingAsync(?timeout: int) : Async<TimeSpan> =
            async {
                try
                    let timeout = defaultArg timeout 3000

                    let timer = new Stopwatch()

                    timer.Start()
                    do! nodeRef.PostWithReply(Ping , timeout)
                    timer.Stop()

                    return timer.Elapsed
                with e -> return handleError e
            }

        /// <summary>
        ///     Pings the remote node, returning the response timespan.
        /// </summary>
        /// <param name="timeout">The amount of time in milliseconds to wait for a reply from the node (default is 3 seconds).</param>
        member n.Ping(?timeout: int) : TimeSpan = n.PingAsync(?timeout = timeout) |> Async.RunSynchronously

        /// <summary>
        ///     Asynchronously assigns a cloud storage definition to be used by the node.
        /// </summary>
        /// <param name="provider">Store connection definition.</param>
        member __.SetStoreAsync (store : ICloudStore) = async {
            let info = StoreRegistry.Register(store, makeDefault = false)
            let! storeManager = nodeRef.PostWithReply(GetStoreManager, MBraceSettings.DefaultTimeout)
                                |> Retry.retryAsync (Retry.RetryPolicy.Filter<UnknownRecipientException>(0.2<Retry.sec>))
            return! StoreManager.uploadStore info storeManager
        }

        /// <summary>
        ///     Assigns a cloud storage definition to be used by the node.
        /// </summary>
        /// <param name="provider">Store connection definition.</param>
        member __.SetStore (store : ICloudStore) = 
            __.SetStoreAsync(store) |> Async.RunSynchronously

        /// <summary>
        ///     Asynchronously returns a manager object for handling storage of remote Node.
        /// </summary>
        member __.GetStoreManagerAsync () = async {
            let! storeManager = nodeRef.PostWithReply(GetStoreManager, MBraceSettings.DefaultTimeout)
            let! info = StoreManager.downloadStore false storeManager
            return new StoreClient(info)
        }

        /// <summary>
        ///     Asynchronously returns a manager object for handling storage of remote Node.
        /// </summary>
        member __.GetStoreManager () = __.GetStoreManagerAsync() |> Async.RunSynchronously

        member internal __.GetNodeInfoAsync getPerformanceCounters = async {
            try
                return! nodeRef <!- fun ch -> GetNodeDeploymentInfo(ch, getPerformanceCounters)
            with e ->
                return handleError e
        }

        /// Asynchronously returns statistics about resources usage (CPU, Memory, etc) collected in this node.
        member __.GetPerformanceCountersAsync () : Async<NodePerformanceInfo> =
            async { 
                let! info = __.GetNodeInfoAsync true
                return info.PerformanceInfo |> Option.get
            }

        /// Returns statistics about resources usage (CPU, Memory, etc) collected in this node.
        member __.GetPerformanceCounters () : NodePerformanceInfo = __.GetPerformanceCountersAsync() |> Async.RunSynchronously

        /// <summary>
        /// Asynchronously returns information about the current node deployment.
        /// </summary>
        /// <param name="includePerformanceCounters">Include the performance statistics.</param>
        member __.GetInfoAsync (?includePerformanceCounters) : Async<string> =
            async { 
                let showPerf = defaultArg includePerformanceCounters false
                let! info = __.GetNodeInfoAsync(showPerf) 
                return Reporting.NodeReporter.Report(Seq.singleton info, showPerf = showPerf, showBorder = false)
            }

        /// <summary>
        /// Returns information about the current node deployment.
        /// </summary>
        /// <param name="includePerformanceCounters">Include the performance statistics.</param>
        member __.GetInfo (?includePerformanceCounters) : string = 
            __.GetInfoAsync(?includePerformanceCounters = includePerformanceCounters) 
            |> Async.RunSynchronously

        /// <summary>
        /// Asynchronously prints information about the current node deployment.
        /// </summary>
        /// <param name="includePerformanceCounters">Include the performance statistics.</param>
        member __.ShowInfoAsync(?includePerformanceCounters) : Async<unit> =
            async { 
                let! info = __.GetInfoAsync(?includePerformanceCounters = includePerformanceCounters)
                Console.WriteLine info
            }

        /// <summary>
        /// Prints information about the current node deployment.
        /// </summary>
        /// <param name="includePerformanceCounters">Include the performance statistics.</param>
        member __.ShowInfo(?includePerformanceCounters) : unit =
            __.ShowInfoAsync(?includePerformanceCounters = includePerformanceCounters) |> Async.RunSynchronously

        /// Asynchronously gets a dump of all logs printed by the node.
        member n.GetSystemLogsAsync () : Async<SystemLogEntry []> =
            async { 
                try return! nodeRef.PostWithReply GetLogDump
                with e -> return handleError e
            }

        /// Gets a dump of all logs printed by the node.
        member n.GetSystemLogs () : SystemLogEntry [] = n.GetSystemLogsAsync() |> Async.RunSynchronously

        /// Asynchronously prints a dump of all logs printed by the node.
        member n.ShowSystemLogsAsync () : Async<unit> = 
            async {
                let! logs = n.GetSystemLogsAsync () 
                Logs.show logs 
            }

        /// Prints a dump of all logs printed by the node.
        member n.ShowSystemLogs () : unit = n.ShowSystemLogsAsync() |> Async.RunSynchronously

        /// Returns whether this node corresponds to a local process.
        member n.IsLocal : bool = n.Process.IsSome

        /// Kills violently the node. This method works only on local nodes.
        member n.Kill() : unit =
            match n.Process with
            | None -> invalidOp "Node %A is not local and cannot be killed." self
            | Some proc -> proc.Kill()

        override node.ToString() = node.Uri.ToString()

        static member internal SpawnAsync (arguments : MBracedConfig list, ?background) : Async<MBraceNode> =
            async {
                let background = defaultArg background false
                let mbracedExe = MBraceSettings.MBracedExecutablePath
                if not <| File.Exists MBraceSettings.MBracedExecutablePath then
                    mfailwithf "Error: invalid mbraced.exe location '%s'." mbracedExe

                try
                    // start a receiver
                    let receiverId = Guid.NewGuid().ToString()
                    use nodeReceiver =
                        Receiver.create<MBracedSpawningServer>()
                        |> Receiver.rename receiverId
                        |> Receiver.publish [ PipeProtocol() ]
                        |> Receiver.start

                    // convert command line arguments to string
                    let flattenedArgs =
                        arguments
                        |> List.filter (function Parent_Receiver_Id _ -> false | _ -> true)
                        |> fun args -> Parent_Receiver_Id (selfProc.Id, receiverId) :: arguments
                        |> mbracedParser.PrintCommandLineFlat
        
                    let proc = new Process()

                    proc.EnableRaisingEvents <- true
                    proc.StartInfo.FileName <- mbracedExe
                    proc.StartInfo.Arguments <- flattenedArgs
                    proc.StartInfo.WorkingDirectory <- Path.GetDirectoryName mbracedExe

                    if background then
                        proc.StartInfo.UseShellExecute <- false
                        proc.StartInfo.CreateNoWindow <- true
                    else
                        proc.StartInfo.UseShellExecute <- true

                    let _ = proc.Start ()

                    let! result = 
                        nodeReceiver 
                        |> Receiver.toObservable
                        |> Observable.merge (proc.Exited |> Observable.map (fun _ -> StartupError (proc.ExitCode, None)))
                        |> Async.AwaitObservable

                    proc.EnableRaisingEvents <- false

                    return
                        match result with
                        | StartupError (id, msg) ->
                            match msg with
                            | None -> mfailwithf "Node %d exited with error %d." proc.Id id
                            | Some msg -> mfailwithf "Node %d exited with message '%s'." proc.Id msg
                        | StartupSuccessful(uri,_) -> 
                            let nref = uri |> MBraceUri.mbraceUriToActorUri Serialization.SerializerRegistry.DefaultName |> ActorRef.fromUri
                            MBraceNode(nref, uri)
                with
                | MBraceExn e -> return! Async.Raise e
                | e -> return mfailwithInner e "Error spawning local {m}brace daemon."
            }

        /// <summary>
        ///     Asynchronously spawns a new MBrace Node in the local machine with given CLI arguments.
        /// </summary>
        /// <param name="arguments">The command line arguments.</param>
        /// <param name="background">Spawn in the background (without a console window).</param>
        static member SpawnAsync(arguments : string [], ?background) : Async<MBraceNode> =
            async {
                let arguments = 
                    try mbracedParser.ParseCommandLine(arguments, ignoreMissing = true).GetAllResults()
                    with e -> mfailwithf "Argument Parse error: %s" e.Message

                return! MBraceNode.SpawnAsync(arguments, ?background = background)
            } 

        /// <summary>
        ///     Spawns a new MBrace Node in the local machine with given CLI arguments.
        /// </summary>
        /// <param name="arguments">The command line arguments.</param>
        /// <param name="background">Spawn in the background (without a console window).</param>
        static member Spawn(arguments : string [], ?background) : MBraceNode =
            MBraceNode.SpawnAsync(arguments, ?background = background) |> Async.RunSynchronously

        /// <summary>
        ///     Asynchronously spawns a new MBrace Node in the local machine with given parameters.
        ///     Unspecified parameters are loaded from the mbraced executable Application configuration file.
        /// </summary>
        /// <param name="hostname">The hostname to be used by the node and the runtime.</param>
        /// <param name="primaryPort">The port the node listens to.</param>
        /// <param name="workerPorts">The ports pool use by the {m}brace workers.</param>
        /// <param name="logFiles">Paths to file to use for logging.</param>
        /// <param name="logLevel">The level of entries to be logged.</param>
        /// <param name="permissions">Permissions for this node.</param>
        /// <param name="debug">Run in debug mode.</param>
        /// <param name="workingDirectory">The path to be used as a working directory.</param>
        /// <param name="useTemporaryWorkDir">Use a temporary folder as a working directory.</param>
        /// <param name="background">Spawn in the background (without a console window).</param>
        /// <param name="store">Store instance to be used with the Node.</param>
        static member SpawnAsync(?hostname : string, ?primaryPort : int, ?workerPorts: int list, ?logFiles : string list, ?logLevel : LogLevel,
                                    ?permissions : Permissions, ?debug : bool, ?workingDirectory : string, ?useTemporaryWorkDir : bool, 
                                    ?background : bool, ?store : ICloudStore) : Async<MBraceNode> = 
            async {
                let debug = defaultArg debug false
                let useTemporaryWorkDir = defaultArg useTemporaryWorkDir false
                let workerPorts = defaultArg workerPorts []
                let logFiles = defaultArg logFiles []
                let store = defaultArg store StoreRegistry.DefaultStoreInfo.Store

                // build arguments
                let args =
                    [
                        match hostname with Some h -> yield Hostname h | None -> ()
                        match primaryPort with Some pp -> yield Primary_Port pp | None -> ()

                        for p in workerPorts -> Worker_Port p
                        for l in logFiles -> Log_File l

                        if debug then yield Debug
                        if useTemporaryWorkDir then yield Use_Temp_WorkDir

                        match permissions with Some p -> yield MBracedConfig.Permissions(int p) | _ -> ()
                        match workingDirectory with Some w -> yield Working_Directory w | _ -> ()
                        match logLevel with Some l -> yield Log_Level l.Value | _ -> ()
                    ]
        
                let! node = MBraceNode.SpawnAsync(args, ?background = background)

                do! node.SetStoreAsync store

                return node
            }

        /// <summary>
        ///     Spawns a new MBrace Node in the local machine with given parameters.
        ///     Unspecified parameters are loaded from the mbraced executable Application configuration file.
        /// </summary>
        /// <param name="hostname">The hostname to be used by the node and the runtime.</param>
        /// <param name="primaryPort">The port the node listens to.</param>
        /// <param name="workerPorts">The ports pool use by the {m}brace workers.</param>
        /// <param name="logFiles">Paths to file to use for logging.</param>
        /// <param name="logLevel">The level of entries to be logged.</param>
        /// <param name="permissions">Permissions for this node.</param>
        /// <param name="debug">Run in debug mode.</param>
        /// <param name="workingDirectory">The path to be used as a working directory.</param>
        /// <param name="useTemporaryWorkDir">Use a temporary folder as a working directory.</param>
        /// <param name="background">Spawn in the background (without a console window).</param>
        /// <param name="store">Store instance to be used with the Node.</param>
        static member Spawn(?hostname : string, ?primaryPort : int, ?workerPorts: int list, ?logFiles : string list, ?logLevel : LogLevel,
                                    ?permissions : Permissions, ?debug : bool, ?workingDirectory : string, ?useTemporaryWorkDir : bool, 
                                    ?background : bool, ?store : ICloudStore) : MBraceNode =

            MBraceNode.SpawnAsync(?hostname = hostname, ?primaryPort = primaryPort, ?workerPorts = workerPorts, ?logFiles = logFiles,
                                        ?logLevel = logLevel, ?permissions = permissions, ?debug = debug, ?workingDirectory = workingDirectory,
                                        ?useTemporaryWorkDir = useTemporaryWorkDir, ?background = background, ?store = store)
            |> Async.RunSynchronously

        /// <summary>
        ///     Asynchronously spawns multiple MBrace Nodes in the local machine with given parameters.
        ///     Unspecified parameters are loaded from the mbraced executable Application configuration file.
        /// </summary>
        /// <param name="nodeCount">The number of nodes to spawn.</param>
        /// <param name="masterPort">Force a specific master port to the first node in the list.</param>
        /// <param name="workerPortsPerNode">The number of worker ports to be used by each {m}brace node.</param>
        /// <param name="hostname">The hostname to be used by each node.</param>
        /// <param name="logFiles">Paths to file to use for logging.</param>
        /// <param name="logLevel">The level of entries to be logged.</param>
        /// <param name="permissions">Permissions for all nodes.</param>
        /// <param name="debug">Run in debug mode.</param>
        /// <param name="background">Spawn in the background (without a console window).</param>
        /// <param name="store">Store instance to be used with the Node.</param>
        static member SpawnMultipleAsync(nodeCount : int, ?masterPort : int, ?workerPortsPerNode : int,  ?hostname : string, ?logFiles : string list, ?logLevel : LogLevel,
                                         ?permissions : Permissions, ?debug : bool, ?background : bool, ?store : ICloudStore) : Async<MBraceNode list> =
        
            let spawnSingle primary pool =
                    MBraceNode.SpawnAsync(?hostname = hostname, primaryPort = primary, workerPorts = pool, ?logFiles = logFiles,
                                            ?logLevel = logLevel, ?permissions = permissions, ?debug = debug, ?background = background,
                                                    ?store = store, useTemporaryWorkDir = true)
            async {
                let workerPortsPerNode = defaultArg workerPortsPerNode 7

                if nodeCount <= 0 || nodeCount > 50 then return invalidArg "nodeCount" "should be between 1 and 50."
                else
                    // TODO : THIS IS WRONG; CHILD SHOULD CHOOSE ITS OWN PORTS
                    let n = workerPortsPerNode + 1
                    let ports = getAvailableTcpPorts <| nodeCount * n |> Array.ofList

                    let! nodes =
                        [0..nodeCount-1] 
                        |> Seq.map (fun i ->
                            let nodePorts = ports.[i * n .. (i+1) * n - 1] |> Array.toList
                            match nodePorts, masterPort with
                            | h :: t, Some pp when i = 0 -> spawnSingle pp t
                            | h :: t, _ -> spawnSingle h t
                            | _ -> failwith "impossible")

                        |> Async.Parallel

                    return Array.toList nodes

            } 

        /// <summary>
        ///     Spawns multiple MBrace Nodes in the local machine with given parameters.
        ///     Unspecified parameters are loaded from the mbraced executable Application configuration file.
        /// </summary>
        /// <param name="nodeCount">The number of nodes to spawn.</param>
        /// <param name="masterPort">Force a specific master port to the first node in the list.</param>
        /// <param name="workerPortsPerNode">The number of worker ports to be used by each {m}brace node.</param>
        /// <param name="hostname">The hostname to be used by each node.</param>
        /// <param name="logFiles">Paths to file to use for logging.</param>
        /// <param name="logLevel">The level of entries to be logged.</param>
        /// <param name="permissions">Permissions for all nodes.</param>
        /// <param name="debug">Run in debug mode.</param>
        /// <param name="background">Spawn in the background (without a console window).</param>
        /// <param name="store">Store instance to be used with the Node.</param>
        static member SpawnMultiple(nodeCount : int, ?masterPort : int, ?workerPortsPerNode : int,  ?hostname : string, ?logFiles : string list, ?logLevel : LogLevel,
                                        ?permissions : Permissions, ?debug : bool, ?background : bool, ?store : ICloudStore) : MBraceNode list =
            MBraceNode.SpawnMultipleAsync(nodeCount, ?masterPort = masterPort, ?workerPortsPerNode = workerPortsPerNode, ?hostname = hostname,
                                            ?logFiles = logFiles, ?logLevel = logLevel, ?permissions = permissions, ?debug = debug, 
                                            ?background = background, ?store = store)
            |> Async.RunSynchronously

        /// <summary>
        ///     Prints a report on the given collection of nodes.
        /// </summary>
        /// <param name="nodes">The nodes.</param>
        /// <param name="displayPerfCounters">Also display performance statistics.</param>
        /// <param name="title">A title for the report.</param>
        /// <param name="useBorders">Use fancy borders.</param>
        static member PrettyPrintAsync(nodes : seq<MBraceNode>, ?displayPerfCounters, ?title, ?useBorders) : Async<string> = async {
            let displayPerfCounters = defaultArg displayPerfCounters false
            let! nodeInfo = nodes |> Seq.map (fun n -> n.GetNodeInfoAsync(displayPerfCounters)) |> Async.Parallel
            return NodeReporter.Report(nodeInfo, displayPerfCounters, ?title = title, ?showBorder = useBorders)
        }

        /// <summary>
        ///     Prints a report on the given collection of nodes.
        /// </summary>
        /// <param name="nodes">The nodes.</param>
        /// <param name="displayPerfCounters">Also display performance statistics.</param>
        /// <param name="title">A title for the report.</param>
        /// <param name="useBorders">Use fancy borders.</param>
        static member PrettyPrint(nodes : seq<MBraceNode>, ?displayPerfCounters, ?title, ?useBorders) =
            MBraceNode.PrettyPrintAsync(nodes, ?displayPerfCounters = displayPerfCounters, ?title = title, ?useBorders = useBorders)
            |> Async.RunSynchronously