namespace Nessos.MBrace.Client

    open System
    open System.Text
    open System.Threading.Tasks
    open System.Diagnostics

    open Microsoft.FSharp.Quotations

    open Nessos.Thespian
    open Nessos.Thespian.ActorExtensions.RetryExtensions

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Core
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Logging
    open Nessos.MBrace.Runtime.Store
    open Nessos.MBrace.Client.Reporting

    open Nessos.MBrace.Client

    [<NoEquality ; NoComparison ; AutoSerializable(false)>]
    type MBraceRuntime private (runtime : ActorRef<MBraceNodeMsg>, disposables : IDisposable list) =

        static let handleError (e : exn) =
            match e with
            | CommunicationExceptionRec e -> mfailwithInner e.InnerException "Error communicating with runtime."
            | MBraceExn e -> reraise' e
            | MessageHandlingExceptionRec(MBraceExn e) -> reraise' e
            | MessageHandlingExceptionRec e -> mfailwithInner e "Runtime replied with exception."
            | _ -> reraise' e

        let postAsync msg = async {
            try return! runtime.PostRetriable(msg, 2)
            with e -> return handleError e
        }

        let postWithReplyAsync msgBuilder = async {
            try return! runtime.PostWithReplyRetriable(msgBuilder, 2)
            with e -> return handleError e
        }

        let post m = postAsync m |> Async.RunSynchronously
        let postWithReply m = postWithReplyAsync m |> Async.RunSynchronously

        let clusterConfiguration = CacheAtom.Create(fun () -> postWithReply(fun ch -> GetClusterDeploymentInfo(ch, false)))
        let nodeConfiguration = CacheAtom.Create(fun () -> postWithReply(fun ch -> GetNodeDeploymentInfo(ch, false)))

        // temporary store sanity check ; replace with store load protocol
        do 
            if clusterConfiguration.Value.StoreId <> MBraceSettings.StoreInfo.Id then
                mfailwith "Connecting to runtime with incompatible store configuration."

        let processManager = new Nessos.MBrace.Client.ProcessManager((fun () -> clusterConfiguration.Value.ProcessManager), StoreRegistry.DefaultStoreInfo)

        //
        //  Runtime Boot/Connect methods
        //

        static let initOfProxyActor(actor : Actor<MBraceNodeMsg>) = 
            do actor.Start()
            new MBraceRuntime(actor.Ref, [actor :> IDisposable])

        static member ConnectAsync(uri: Uri): Async<MBraceRuntime> =
            async {
                try
                    let node = MBraceNode uri
                    let! proxy = RuntimeProxy.connect node.Ref
                    return initOfProxyActor proxy

                with e -> return handleError e
            }

        static member BootAsync (master : MBraceNode, config : BootConfiguration) : Async<MBraceRuntime> =
            async {
                let! proxy = RuntimeProxy.boot(master.Ref, config)
                return initOfProxyActor proxy
            }

        static member BootAsync(nodes : MBraceNode list, ?replicationFactor, ?failoverFactor) : Async<MBraceRuntime> = async {
            let failoverFactor = defaultArg failoverFactor 2
            let replicationFactor = defaultArg replicationFactor (if failoverFactor = 0 then 0 else 2)
            let nodes = nodes |> Seq.map (fun n -> n.Ref) |> Seq.toArray
            let! proxy = RuntimeProxy.bootNodes(nodes, replicationFactor, failoverFactor)
            return initOfProxyActor proxy
        }

        static member Connect(uri: Uri): MBraceRuntime = MBraceRuntime.ConnectAsync(uri) |> Async.RunSynchronously
        static member Connect(host: string, port : int) : MBraceRuntime = MBraceRuntime.Connect(MBraceUri.hostPortToUri(host, port))
        static member Connect(uri: string): MBraceRuntime = MBraceRuntime.Connect(Uri(uri))
        static member ConnectAsync(uri: string): Async<MBraceRuntime> = MBraceRuntime.ConnectAsync(Uri(uri))


        static member Boot(nodes : MBraceNode list, ?replicationFactor, ?failoverFactor) : MBraceRuntime = 
            MBraceRuntime.BootAsync(nodes, ?replicationFactor = replicationFactor, ?failoverFactor = failoverFactor)
            |> Async.RunSynchronously

        static member InitLocal(totalNodes : int, ?hostname, ?replicationFactor : int, ?storeProvider,
                                         ?failoverFactor : int, ?debug, ?background) : MBraceRuntime =

            if totalNodes < 3 then invalidArg "totalNodes" "should have at least 3 nodes."
            let nodes = MBraceNode.SpawnMultiple(totalNodes, ?hostname = hostname, ?debug = debug,
                                                    ?storeProvider = storeProvider, ?background = background)
            
            MBraceRuntime.Boot(nodes, ?replicationFactor = replicationFactor, ?failoverFactor = failoverFactor)

        static member FromActorRef(ref : ActorRef<MBraceNodeMsg>) = new MBraceRuntime(ref, [])

        //
        //  Cluster Management section
        //

        member r.BootAsync (?replicationFactor, ?failoverFactor) : Async<unit> =
            async {
                if nodeConfiguration.Value.State <> Idle then
                    mfailwith "Cannot boot; runtime is already active."
                else
                    match clusterConfiguration.TryGetLastSuccessfulValue () with
                    | None -> mfailwith "Cannot boot; insufficient cluster information."
                    | Some info ->
                        let nodes = info.Nodes |> Array.map (fun n -> n.Reference)
                        let config =
                            {
                                Nodes = nodes
                                ReplicationFactor = defaultArg replicationFactor info.ReplicationFactor
                                FailoverFactor = defaultArg failoverFactor info.FailoverFactor
                            }

                        let! _ = postWithReplyAsync <| fun ch -> MasterBoot(ch,config)

                        return ()
            }

        member r.ShutdownAsync () : Async<unit> = async {
            // force clusterConfiguration evaluation
            do clusterConfiguration.TryGetValue() |> ignore

            return! postWithReplyAsync ShutdownSync
        }

        member internal r.RebootAsync (?replicationFactor, ?failoverFactor) : Async<unit> =
            async {
                do! r.ShutdownAsync()

                return! r.BootAsync(?replicationFactor = replicationFactor, ?failoverFactor = failoverFactor)
            }

        member r.AttachAsync (nodes : seq<MBraceNode>) : Async<unit> =
            async {
                for node in nodes do
                    do! postWithReplyAsync (fun ch -> Attach(ch, node.Ref))
            }

        // Add config update & provision for scheduler detachment 
        // RuntimeMonitor.GetNodeDataByRuntime not working as expected
        member r.DetachAsync (node: MBraceNode) : Async<unit> =
            async {
                try
                    do! node.Ref <!- Detach
                with
                | MBraceExn e -> return reraise' e
                | CommunicationException _ -> return mfailwithf "Failed to connect to node %A." node.Uri
                | MessageHandlingExceptionRec e -> return mfailwithInner e "Node %A replied with exception." node.Uri
            }

        member r.Boot (?replicationFactor, ?failoverFactor) : unit = 
            r.BootAsync(?replicationFactor = replicationFactor, ?failoverFactor = failoverFactor) |> Async.RunSynchronously

        member r.Shutdown() : unit = r.ShutdownAsync() |> Async.RunSynchronously
        member r.Reboot(?replicationFactor, ?failoverFactor) : unit = 
            r.RebootAsync(?replicationFactor = replicationFactor, ?failoverFactor = failoverFactor) |> Async.RunSynchronously

        member r.Attach (nodes : seq<MBraceNode>) : unit = r.AttachAsync nodes |> Async.RunSynchronously
        member r.Detach (node : MBraceNode) : unit = r.DetachAsync node |> Async.RunSynchronously

        member r.AttachLocal(totalNodes : int, ?permissions, ?debug, ?background) : unit =
            let nodes = MBraceNode.SpawnMultiple(totalNodes, ?permissions = permissions, ?debug = debug, ?background = background)
            r.Attach(nodes)


        //
        //  Region Misc runtime information
        //

        member __.Ping(?silent: bool, ?timeout: int) : int =
            let silent = defaultArg silent false
            let timeout = defaultArg timeout 5000

            let timer = new Stopwatch()

            timer.Start()
            runtime <!== (Ping, timeout)
            timer.Stop()

            int timer.ElapsedMilliseconds

        member r.Id : Guid = clusterConfiguration.Value.DeploymentId
        member r.Nodes : MBraceNode list = 
            match clusterConfiguration.TryGetLastSuccessfulValue() with
            | None -> mfailwith "Cannot extract runtime information." 
            | Some info -> info.Nodes |> Seq.map (fun n -> MBraceNode(n)) |> Seq.toList

        member r.LocalNodes : MBraceNode list = r.Nodes |> List.filter (fun node -> node.IsLocal)

        member r.Master : MBraceNode = let nI = clusterConfiguration.Value.MasterNode in MBraceNode(nI)
        member r.Alts : MBraceNode list = 
            clusterConfiguration.Value.Nodes 
            |> Seq.filter(fun nI -> nI.State = AltMaster) 
            |> Seq.map (fun n -> MBraceNode(n)) 
            |> Seq.toList

        member r.Active : bool = nodeConfiguration.Value.State <> Idle

        member r.ShowInfo (?showPerformanceCounters : bool) : unit = 
            try
                let showPerformanceCounters = defaultArg showPerformanceCounters false
                let info =
                    if showPerformanceCounters then
                        postWithReply (fun ch -> GetClusterDeploymentInfo(ch, true))
                    else
                        clusterConfiguration.Value

                Reporting.MBraceNodeReporter.Report(info.Nodes, showPerf = showPerformanceCounters, showBorder = false)
                |> Console.WriteLine

            with e -> Console.Error.WriteLine e.Message

        member r.GetSystemLogs() : SystemLogEntry [] = postWithReply GetLogDump
        member r.ShowSystemLogs() : unit = r.GetSystemLogs() |> Reporting.Logs.show

        //TODO : replace with cooperative shutdowns
        /// violent kill
        member r.Kill() : unit =
            if r.Nodes |> List.exists (fun t -> not t.IsLocal) then
                invalidOp "'Kill' operation is reserved for local runtimes."

            for node in r.LocalNodes do node.Kill()

            (r :> IDisposable).Dispose()

        interface IDisposable with
            member r.Dispose() = for d in disposables do d.Dispose()

        member r.StoreClient : StoreClient = StoreClient.Default

        //
        //  Computation Section
        //

        member __.CreateProcess (computation : CloudComputation<'T>) : Process<'T> = 
            processManager.CreateProcess computation |> Async.RunSynchronously

        member __.RunAsync (computation : CloudComputation<'T>) : Async<'T> =
            async {
                let! proc = processManager.CreateProcess computation
                return! proc.AwaitResultAsync ()
            }

        member __.Run (computation : CloudComputation<'T>) : 'T = __.RunAsync computation |> Async.RunSynchronously

        member __.CreateProcess (expr : Expr<Cloud<'T>>, ?name) : Process<'T> =
            let computation = CloudComputation.Compile(expr, ?name = name)
            __.CreateProcess computation

        member __.RunAsync (expr : Expr<Cloud<'T>>, ?name) : Async<'T> =
            let computation = CloudComputation.Compile(expr, ?name = name)
            __.RunAsync computation

        member __.Run (expr : Expr<Cloud<'T>>, ?name) : 'T =
            let computation = CloudComputation.Compile(expr, ?name = name)
            __.Run computation 

        [<CompilerMessage("Cloud blocks should be wrapped in quotation literals for better debug support.", 44)>]
        member __.CreateProcess (block : Cloud<'T>, ?name) : Process<'T> =
            let computation = CloudComputation.Compile(block, ?name = name)
            __.CreateProcess computation

        [<CompilerMessage("Cloud blocks should be wrapped in quotation literals for better debug support.", 44)>]
        member __.RunAsync (block : Cloud<'T>, ?name) : Async<'T> =
            let computation = CloudComputation.Compile(block, ?name = name)
            __.RunAsync computation

        [<CompilerMessage("Cloud blocks should be wrapped in quotation literals for better debug support.", 44)>]
        member __.Run (block : Cloud<'T>, ?name) : 'T =
            let computation = CloudComputation.Compile(block, ?name = name)
            __.Run computation 

        member __.KillProcess (pid : ProcessId) : unit = processManager.Kill pid |> Async.RunSynchronously

        member __.GetProcess (pid : ProcessId) : Process = processManager.GetProcess pid |> Async.RunSynchronously
        member __.GetProcess<'T> (pid : ProcessId) : Process<'T> = __.GetProcess pid :?> Process<'T>
        member __.GetAllProcesses () : Process [] = processManager.GetAllProcesses () |> Async.RunSynchronously

        member __.ClearProcessInfo (pid : ProcessId) : unit = processManager.ClearProcessInfo pid |> Async.RunSynchronously
        member __.ClearAllProcessInfo () : unit = processManager.ClearAllProcessInfo () |> Async.RunSynchronously

        member __.ShowProcessInfo () : unit = processManager.GetInfo () |> Console.WriteLine
        member __.GetProcessInfo () : unit = processManager.GetInfo () |> Console.WriteLine