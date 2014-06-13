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

    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Utils
    open Nessos.MBrace.Runtime.Logging
    open Nessos.MBrace.Runtime.Daemon.Configuration
    
    open Nessos.MBrace.Client.Reporting

    type private MBraceNodeMsg = Nessos.MBrace.Runtime.MBraceNode

    type NodePerformanceInfo = Nessos.MBrace.Runtime.NodePerformanceInfo
    type Permissions = Nessos.MBrace.Runtime.Permissions

    ///The type representing a {m}brace node.
    [<Sealed; NoEquality; NoComparison; AutoSerializable(false)>]
    type MBraceNode private (nodeRef: ActorRef<MBraceNodeMsg>, uri : Uri) as self =

        static do MBraceSettings.ClientId |> ignore

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

        /// Create a new MBraceNode object. No node is spawned.
        new (uri: Uri) =
            let nref = uri |> MBraceUri.mbraceUriToActorUri Serialization.SerializerRegistry.DefaultName |> ActorRef.fromUri
            MBraceNode(nref, uri)

        /// Create a new MBraceNode object. No node is spawned.
        new (hostname : string, port : int) = MBraceNode(MBraceUri.hostPortToUri(hostname, port))

         /// Create a new MBraceNode object. No node is spawned.
        new (uri : string) = MBraceNode(new Uri(uri))

        /// Gets the System.Diagnostics.Process object that corresponds to the node's process.
        member __.Process : Process option = snd nodeInfo.Value
        member internal __.Ref = nodeRef
        
        /// Gets the node's uri.
        member __.Uri : Uri = uri

        /// Sets the node's permissions.
        member __.Permissions
            with get () : Permissions = (fst nodeInfo.Value).Permissions
            and  set (newPermissions: Permissions) =
                try nodeRef <-- SetNodePermissions newPermissions
                with e -> handleError e

        /// Sets whether the node has slave permissions.
        member n.IsPermittedSlave
            with get() = n.Permissions.HasFlag Permissions.Slave
            and  set (x: bool) = n.Permissions <- Permissions.switch x Permissions.Slave n.Permissions

        /// Sets whether the node has master permissions.
        member n.IsPermittedMaster
            with get () = n.Permissions.HasFlag Permissions.Slave
            and  set (x: bool) = n.Permissions <- Permissions.switch x Permissions.Slave n.Permissions 

        member n.State : NodeState = (fst nodeInfo.Value).State
        member n.DeploymentId = (fst nodeInfo.Value).DeploymentId
        member n.IsActive = n.State <> Idle

        member n.Ping(?silent: bool, ?timeout: int) : int =
            try
                let silent = defaultArg silent false
                let timeout = defaultArg timeout 3000

                let timer = new Stopwatch()

                timer.Start()
                nodeRef <!== (Ping , timeout)
                timer.Stop()

                int timer.ElapsedMilliseconds
            with e -> handleError e

        member internal __.GetNodeInfoAsync getPerformanceCounters = async {
            try
                return! nodeRef <!- fun ch -> GetNodeDeploymentInfo(ch, getPerformanceCounters)
            with e ->
                return handleError e
        }

        member __.GetPerformanceCounters () : NodePerformanceInfo =
            let info = __.GetNodeInfoAsync true |> Async.RunSynchronously
            info.PerformanceInfo |> Option.get

        member __.ShowPerformanceCounters () =
                let info = __.GetNodeInfoAsync true |> Async.RunSynchronously
                Reporting.MBraceNodeReporter.Report(Seq.singleton info, showPerf = true, showBorder = false)
                |> Console.WriteLine

        member n.GetSystemLogs () : SystemLogEntry [] =
            try nodeRef <!= GetLogDump
            with e -> handleError e

        member n.ShowSystemLogs () : unit = n.GetSystemLogs () |> Logs.show

        member n.IsLocal : bool = n.Process.IsSome

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

        static member Spawn(arguments : string [], ?background) : MBraceNode =
            async {
                let arguments = 
                    try mbracedParser.ParseCommandLine(arguments, ignoreMissing = true).GetAllResults()
                    with e -> mfailwithf "Argument Parse error: %s" e.Message

                return! MBraceNode.SpawnAsync(arguments, ?background = background)
            } |> Async.RunSynchronously

        static member SpawnAsync(?hostname : string, ?primaryPort : int, ?workerPorts: int list, ?logFiles : string list, ?logLevel : LogLevel,
                                    ?permissions : Permissions, ?debug : bool, ?workingDirectory : string, ?useTemporaryWorkDir : bool, 
                                    ?background : bool, ?storeProvider : StoreProvider) : Async<MBraceNode> = 
            async {
                let debug = defaultArg debug false
                let useTemporaryWorkDir = defaultArg useTemporaryWorkDir false
                let workerPorts = defaultArg workerPorts []
                let logFiles = defaultArg logFiles []
                let storeProvider = defaultArg storeProvider MBraceSettings.StoreProvider

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

                        yield Store_Provider storeProvider.StoreFactoryQualifiedName
                        yield Store_EndPoint storeProvider.ConnectionString
                    ]
        
                return! MBraceNode.SpawnAsync(args, ?background = background)
            }

        static member Spawn(?hostname : string, ?primaryPort : int, ?workerPorts: int list, ?logFiles : string list, ?logLevel : LogLevel,
                                    ?permissions : Permissions, ?debug : bool, ?workingDirectory : string, ?useTemporaryWorkDir : bool, 
                                    ?background : bool, ?storeProvider : StoreProvider) : MBraceNode =

            MBraceNode.SpawnAsync(?hostname = hostname, ?primaryPort = primaryPort, ?workerPorts = workerPorts, ?logFiles = logFiles,
                                        ?logLevel = logLevel, ?permissions = permissions, ?debug = debug, ?workingDirectory = workingDirectory,
                                        ?useTemporaryWorkDir = useTemporaryWorkDir, ?background = background, ?storeProvider = storeProvider)
            |> Async.RunSynchronously

        static member SpawnMultiple(nodeCount : int, ?workerPortsPerNode : int,  ?hostname : string, ?logFiles : string list, ?logLevel : LogLevel,
                                        ?permissions : Permissions, ?debug : bool, ?background : bool, ?storeProvider : StoreProvider) : MBraceNode list =
        
            let spawnSingle primary pool =
                    MBraceNode.SpawnAsync(?hostname = hostname, primaryPort = primary, workerPorts = pool, ?logFiles = logFiles,
                                            ?logLevel = logLevel, ?permissions = permissions, ?debug = debug, ?background = background,
                                                    ?storeProvider = storeProvider, useTemporaryWorkDir = true)
            async {
                let workerPortsPerNode = defaultArg workerPortsPerNode 7

                if nodeCount <= 0 || nodeCount > 50 then return invalidArg "nodeCount" "should be between 1 and 50."
                else
                    // TODO : THIS IS WRONG; CHILD SHOULD CHOOSE ITS OWN PORTS
                    let n = workerPortsPerNode + 1
                    let ports = getAvailableTcpPorts <| nodeCount * n |> Array.ofList

                    let! nodes =
                        [0..nodeCount-1] 
                        |> Seq.map (fun i -> ports.[i * n .. (i+1) * n - 1] |> Array.toList)
                        |> Seq.map (function h :: t -> spawnSingle h t | _ -> failwith "impossible")
                        |> Async.Parallel

                    return Array.toList nodes

            } |> Async.RunSynchronously


        static member PrettyPrintAsync(nodes : seq<MBraceNode>, ?displayPerfCounters, ?title, ?useBorders) : Async<string> = async {
            let displayPerfCounters = defaultArg displayPerfCounters false
            let! nodeInfo = nodes |> Seq.map (fun n -> n.GetNodeInfoAsync(displayPerfCounters)) |> Async.Parallel
            return MBraceNodeReporter.Report(nodeInfo, displayPerfCounters, ?title = title, ?showBorder = useBorders)
        }

        static member PrettyPrint(nodes : seq<MBraceNode>, ?displayPerfCounters, ?title, ?useBorders) =
            MBraceNode.PrettyPrintAsync(nodes, ?displayPerfCounters = displayPerfCounters, ?title = title, ?useBorders = useBorders)
            |> Async.RunSynchronously