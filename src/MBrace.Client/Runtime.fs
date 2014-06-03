namespace Nessos.MBrace.Client

    open System
    open System.Text
    open System.Threading.Tasks
    open System.Diagnostics

    open Nessos.Thespian
    open Nessos.Thespian.ActorExtensions
    open Nessos.Thespian.ActorExtensions.RetryExtensions

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Core
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Logging
    open Nessos.MBrace.Runtime.Store
    open Nessos.MBrace.Runtime.Utils

    open Nessos.MBrace.Client

    open Microsoft.FSharp.Quotations


    [<Sealed>]
    type MBraceRuntime internal (runtimeActor : Actor<ClientRuntimeProxy>, isEncapsulatedActor : bool) =        
        static do MBraceSettings.ClientId |> ignore

        // the runtime proxy actor is responsible for two things:
        // * keeps a manifest of all runtime nodes as an internal state and updates it accordingly
        // * forwards messages to the runtime with failover
        static let createRuntimeProxy (nodes : NodeRef list) =

            let rec proxyBehaviour (state : NodeRef list) (self : Actor<ClientRuntimeProxy>) : Async<NodeRef list> =
                async {

                    let! msg = self.Receive()

                    try
                        match msg with
                        | RemoteMsg (MasterBoot (R reply, conf)) ->
                            match conf.Nodes |> Array.tryFind (permitted Permissions.Master) with
                            | None ->
                                mkMBraceExn None "Cannot boot runtime; no appropriate master node candidate found." |> Reply.exn |> reply
                                return state
                            | Some candidate ->
                                if isActive candidate then
                                    mkMBraceExn None "Cannot boot runtime; runtime appears to be already active." |> Reply.exn |> reply
                                    return state
                                else
                                    let! master, alts = candidate <!- fun ch -> MasterBoot(ch, conf)
                                    let! state' = master <!- GetAllNodes

                                    if state'.Length <> 0 then
                                        reply <| Value (master,alts)
                                        return Array.toList state'
                                    else
                                        mkMBraceExn None "Cannot boot runtime; insufficient nodes." |> Reply.exn |> reply
                                        return state

                        | RemoteMsg (GetAllNodes(R reply)) ->
                            if state.IsEmpty then
                                mkMBraceExn None "Client: no nodes specified or runtime not booted." |> Reply.exn |> reply
                                return state
                            else 
                                let! response,_ = Failover.postWithReply GetAllNodes self.LogWarning state GetAllNodes

                                reply response

                                match response with
                                | Value nodes -> return Array.toList nodes
                                | Exception _ -> return state
//                            elif isActive state.Head then
//                                let! response,_ = Failover.postWithReply GetAllNodes self.LogWarning state GetAllNodes
//
//                                reply response
//
//                                match response with
//                                | Value nodes -> return Array.toList nodes
//                                | Exception _ -> return state
//                            else
//                                Array.ofList state |> Value |> reply
//                                return state
                        | RemoteMsg msg -> 
                            return! Failover.post GetAllNodes self.LogWarning state msg

                        // local messages

                        | GetLastRecordedState rc -> rc.Reply (Value state) ; return state

                    // uh oh, cannot communicate with runtime
                    with
                    | MBraceExn e -> tryReply msg <| Reply.exn e ; return state
                    | CommunicationException _ ->
                        mkMBraceExn None "Cannot communicate with {m}brace runtime." |> Reply.exn |> tryReply msg
                        return state
                    | MessageHandlingException _ as e ->
                        mkMBraceExn (Some e) "Runtime replied with exception." |> Reply.exn |> tryReply msg
                        return state
                    | e -> tryReply msg (Exception e) ; return state
                }

            let actor =
                proxyBehaviour
                |> Behavior.stateful2 nodes
                |> Actor.bind
                |> Actor.start

            new MBraceRuntime(actor, true)
        
        static let connect (candidate : NodeRef) =
            async {
                if isActive candidate then
                    let! nodes = candidate <!- Runtime.GetAllNodes
                    return createRuntimeProxy <| Array.toList nodes
                else
                    return mfailwith "Cannot connect to runtime; runtime is inactive."
            }

        //
        // nonstatic constructor
        //

        let runtime = runtimeActor.Ref
        let processManager = new Nessos.MBrace.Client.ProcessManager(runtime)

        // temporary store sanity check
        // TODO : shell logger
        do if not <| runtimeUsesCompatibleStore runtime then
            eprintfn "Warning: connecting to runtime with incompatible store configuration."
//            match Shell.Settings with
//            | Some s when s.Verbose -> 
//            | _ -> ()
            
        let postWithReplyAsync msgBuilder =
            async {
                try
                    return! runtime <!- (RemoteMsg << msgBuilder)
                with
                | MBraceExn e -> return raise e
                | MessageHandlingException (_,_,_,e) ->
                    return mfailwithInner e "Runtime client has replied with exception."
                | :? TimeoutException -> return mfailwith "Timeout when connecting to runtime."
                | :? ActorInactiveException -> return mfailwith "Client object has been disposed."
            }

        let post msg = 
            try
                runtime <-- RemoteMsg msg
            with
            | MBraceExn e -> raise e
            | MessageHandlingException(_,_,_,e) ->
                mfailwithInner e "Runtime client has replied with exception."
            | :? TimeoutException -> mfailwith "Timeout when connecting to runtime."
            | :? ActorInactiveException -> mfailwith "Runtime proxy has been disposed."

        let postWithReply m = postWithReplyAsync m |> Async.RunSynchronously

        let configuration = 
            CacheAtom.Create(fun () -> postWithReply GetAllNodes |> NodeInfo.Create)

        let storeInfo = StoreRegistry.DefaultStore
        let coreConfig = StoreRegistry.DefaultPrimitiveConfiguration

        member internal __.ActorRef = runtime
        member internal __.PostWithReply m = postWithReply m
        member internal __.PostWithReplyAsync m = postWithReplyAsync m
        
        member r.AttachAsync (nodes : #seq<MBraceNode>) =
            async {
                for node in nodes do
                    do! postWithReplyAsync (fun ch -> Attach(ch, node.Ref))
            }

        // Add config update & provision for scheduler detachment 
        // RuntimeMonitor.GetNodeDataByRuntime not working as expected
        member r.DetachAsync (node: MBraceNode) =
            async {
                try
                    do! node.Ref <!- Detach
                with
                | MBraceExn e -> return raise e
                | CommunicationException _ -> return mfailwithf "Failed to connect to node %A." node.Uri
                | MessageHandlingException _ as e -> return mfailwithInner e "Node %A replied with exception." node.Uri
            }

        static member internal BootAsync (conf : Configuration) : Async<MBraceRuntime> =
            async {
                let runtime = createRuntimeProxy <| Array.toList conf.Nodes

                let! _ = runtime.PostWithReplyAsync <| fun ch -> MasterBoot(ch,conf)

                return runtime
            }

        member internal r.BootAsync (?replicationFactor, ?failoverFactor) =
            async {
                if isActiveRuntime runtime then
                    mfailwith "Cannot boot; runtime is already active."
                else
                    let! nodes = runtime <!- GetLastRecordedState

                    let nodes = Array.ofList nodes

                    let replicationFactor = defaultArg replicationFactor 0
                    let failoverFactor = defaultArg failoverFactor <| min (nodes.Length - 1) 3

                    let conf = Configuration(nodes,replicationFactor,failoverFactor)

                    let! _ = postWithReplyAsync <| fun ch -> MasterBoot(ch,conf)

                    return ()
            } |> Error.handleAsync

        member r.BootAsync () = 
            r.BootAsync(?replicationFactor = None, ?failoverFactor = None)

        member r.ShutdownAsync () =
            async {
                post Shutdown

                do! Async.Sleep 2000
            } |> Error.handleAsync

        member internal r.RebootAsync (?replicationFactor, ?failoverFactor) =
            async {
                do! postWithReplyAsync ShutdownSync

                return! r.BootAsync(?replicationFactor = replicationFactor, ?failoverFactor = failoverFactor)
            } |> Error.handleAsync
        member r.RebootAsync () =
            r.RebootAsync(?replicationFactor = None, ?failoverFactor = None)

        member __.Ping(?silent: bool, ?timeout: int) =
            try
                let silent = defaultArg silent false
                let timeout = defaultArg timeout 10000

                let timer = new Stopwatch()

                try
                    timer.Start()
                    runtime <!== ( (fun chan -> RemoteMsg(Ping(chan,silent))) , timeout )
                    timer.Stop()

                    int timer.ElapsedMilliseconds
                with
                | :? TimeoutException -> mfailwith "PING request timed out."
                | :? ActorInactiveException -> mfailwith "Client object has been disposed."
            with e -> Error.handle e

        member r.Id = try postWithReply GetDeploymentId with e -> Error.handle e

        member r.Attach (nodes : #seq<MBraceNode>) = r.AttachAsync nodes |> Error.handleAsync2
        member r.Detach (node : MBraceNode) = r.DetachAsync node |> Error.handleAsync2
        member r.DetachAsync (uri : string) = r.DetachAsync(MBraceNode(uri)) |> Error.handleAsync2
        member r.Detach (uri : Uri) = r.Detach(MBraceNode(uri))
        member r.Detach (uri : string) = r.Detach(MBraceNode(uri))
        static member internal Boot (conf) = MBraceRuntime.BootAsync conf |> Error.handleAsync2
        member internal r.Boot (?replicationFactor, ?failoverFactor) =
            r.BootAsync (?replicationFactor = replicationFactor, ?failoverFactor = failoverFactor) |> Error.handleAsync2
        member r.Boot () =
            r.Boot(?replicationFactor = None, ?failoverFactor = None)
        member r.Shutdown() = r.ShutdownAsync() |> Error.handleAsync2
        member internal r.Reboot(?replicationFactor, ?failoverFactor) =
            //let failoverFactor = defaultArg failoverFactor <| List.length r.Alts
            r.RebootAsync(?replicationFactor = replicationFactor, ?failoverFactor = failoverFactor) |> Error.handleAsync2
        member r.Reboot() =
            r.Reboot(?replicationFactor = None, ?failoverFactor = None)
                
        member r.Nodes = configuration.Value.Nodes
        member r.Alts = configuration.Value.Alts
        member r.Master = configuration.Value.Master
        member r.Active = isActiveRuntime runtime
        member r.LocalNodes = configuration.Value.Nodes |> List.filter (fun node -> node.IsLocal)
        member r.ShowInfo (?showPerformanceCounters : bool, ?useBorders) = 
            try
                configuration.Value.Display(?displayPerfCounters = showPerformanceCounters, ?useBorders = useBorders)
                |> printfn "%s" 
            with e -> printfn "%s" e.Message

        member r.GetSystemLogs() = postWithReply GetLogDump

        member r.ShowSystemLogs() = r.GetSystemLogs() |> Logs.show

        //TODO : replace with cooperative shutdowns
        /// violent kill
        member r.Kill() =
            try
                if r.Nodes |> List.exists (fun t -> not t.IsLocal) then
                    mfailwith "'Kill' operation is reserved for local runtimes."

                for node in r.LocalNodes do node.Kill()

                (r :> IDisposable).Dispose()
            with e -> Error.handle e

        static member ConnectAsync(uri: Uri): Async<MBraceRuntime> =
            async {
                let node = MBraceNode uri
                return! connect node.Ref
            } |> Error.handleAsync

        static member Connect(uri: Uri): MBraceRuntime = MBraceRuntime.ConnectAsync(uri) |> Error.handleAsync2
        static member Connect(host: string, port : int) = MBraceRuntime.Connect(hostPortToUri(host, port))

        static member Connect(uri: string): MBraceRuntime = MBraceRuntime.Connect(Uri(uri))

        static member ConnectAsync(uri: string): Async<MBraceRuntime> = MBraceRuntime.ConnectAsync(Uri(uri))

        static member internal Boot(nodes : MBraceNode list, ?replicationFactor, ?failoverFactor) =
            try
                if nodes.Length < 1 then raise <| ArgumentException("Cannot boot; insufficient nodes.")

                let nrefs = nodes |> Seq.map (fun node -> node.Ref) |> Seq.toArray
                let replicationFactor = defaultArg replicationFactor <| min (nodes.Length - 1) 2
                let failoverFactor = defaultArg failoverFactor <| min (nodes.Length - 1) 3
            
                let conf = Configuration(nrefs, replicationFactor, failoverFactor)

                MBraceRuntime.Boot(conf)
            with e -> Error.handle e
        static member Boot(nodes : MBraceNode list) =
            MBraceRuntime.Boot(nodes, ?replicationFactor = None, ?failoverFactor = None)

        static member internal Boot(uris: Uri list, ?replicationFactor, ?failoverFactor) =
            try
                let nodes = uris |> List.map (fun uri -> MBraceNode uri)

                MBraceRuntime.Boot(nodes, ?replicationFactor = replicationFactor, ?failoverFactor = failoverFactor)
            with e -> Error.handle e
        static member Boot(uris: Uri list) =
            MBraceRuntime.Boot(uris,?replicationFactor = None, ?failoverFactor = None)

        static member internal Boot(uris: string list, ?replicationFactor, ?failoverFactor) =
            try
                let nodes = uris |> List.map (fun uri -> MBraceNode uri)

                MBraceRuntime.Boot(nodes, ?replicationFactor = replicationFactor, ?failoverFactor = failoverFactor)
            with e -> Error.handle e
        static member Boot(uris: string list) =
            MBraceRuntime.Boot(uris, ?replicationFactor = None, ?failoverFactor = None)

        member r.AttachLocal(totalNodes : int, ?permissions, ?debug, ?background) =
            try
                let nodes = MBraceNode.SpawnMultiple(totalNodes, ?permissions = permissions, ?debug = debug, ?background = background)

                r.Attach(nodes)
            with e -> Error.handle e

        interface IDisposable with
            member r.Dispose() =
//                (processManager :> IDisposable).Dispose()
                if isEncapsulatedActor then runtimeActor.Stop()

        interface IComparable with
            member r.CompareTo(y) =
                match y with
                | :? MBraceRuntime as r' -> r.Id.CompareTo(r'.Id)
                | _ -> raise <| ArgumentException("invalid argument")

        override r.Equals(y) =
            match y with
            | :? MBraceRuntime as r' -> r.Id = r'.Id
            | _ -> false

        override r.GetHashCode() = r.Id.GetHashCode()

        member r.StoreClient = StoreClient.Default // TODO : change this

        //
        //  Computation Section
        //

        member __.CreateProcess (computation : CloudComputation<'T>) = processManager.CreateProcess computation

        member __.RunAsync (computation : CloudComputation<'T>) =
            async {
                let! proc = processManager.CreateProcess computation
                return! proc.AwaitResultAsync ()
            } |> Error.handleAsync

        member __.Run (computation : CloudComputation<'T>) = __.RunAsync computation |> Error.handleAsync2

        member __.CreateProcess (expr : Expr<Cloud<'T>>, ?name) =
            try
                let computation = CloudComputation.Compile(expr, ?name = name)
                processManager.CreateProcess computation |> Async.RunSynchronously
            with e -> Error.handle e

        member __.RunAsync (expr : Expr<Cloud<'T>>, ?name) =
            let computation = CloudComputation.Compile(expr, ?name = name)
            __.RunAsync computation

        member __.Run (expr : Expr<Cloud<'T>>, ?name) =
            try
                let computation = CloudComputation.Compile(expr, ?name = name)
                __.Run computation 
            with e -> Error.handle e

        [<CompilerMessage("Cloud blocks should be wrapped in quotation literals for better debug support.", 44)>]
        member __.CreateProcess (block : Cloud<'T>, ?name) =
            try
                let computation = CloudComputation.Compile(block, ?name = name)
                processManager.CreateProcess computation
            with e -> Error.handle e

        [<CompilerMessage("Cloud blocks should be wrapped in quotation literals for better debug support.", 44)>]
        member __.RunAsync (block : Cloud<'T>, ?name) =
            let computation = CloudComputation.Compile(block, ?name = name)
            __.RunAsync computation

        [<CompilerMessage("Cloud blocks should be wrapped in quotation literals for better debug support.", 44)>]
        member __.Run (block : Cloud<'T>, ?name) =
            try
                let computation = CloudComputation.Compile(block, ?name = name)
                __.Run computation 
            with e -> Error.handle e

        member __.KillProcess (pid : ProcessId) = processManager.Kill pid |> Async.RunSynchronously

        member __.GetProcess (pid : ProcessId) = processManager.GetProcess pid |> Async.RunSynchronously
        member __.GetProcess<'T> (pid : ProcessId) = processManager.GetProcess pid |> Async.RunSynchronously :?> Process<'T>
        member __.GetProcess (pid : string) = __.GetProcess (stringToProcessId pid)
        member __.GetProcess<'T> (pid : string) = __.GetProcess<'T> (stringToProcessId pid)
        member __.GetAllProcesses () = processManager.GetAllProcesses () |> Async.RunSynchronously

        member __.ClearProcessInfo (pid : ProcessId) = processManager.ClearProcessInfo pid |> Async.RunSynchronously
        member __.ClearProcessInfo (pid : string) =  __.ClearProcessInfo (stringToProcessId pid)
        member __.ClearAllProcessInfo () = processManager.ClearAllProcessInfo () |> Async.RunSynchronously

        member __.ShowProcessInfo (?useBorders) = processManager.ShowInfo (?useBorders = useBorders)

        static member internal InitLocal(totalNodes : int, ?hostname, ?replicationFactor : int, ?storeProvider,
                                         ?failoverFactor : int, ?debug, ?background) : MBraceRuntime =
            if totalNodes < 2 then mfailwithInner (ArgumentException()) "Error spawning local runtime."

            let nodes = MBraceNode.SpawnMultiple(totalNodes, ?hostname = hostname, ?storeProvider = storeProvider, 
                                                                                ?debug = debug, ?background = background)
            
            MBraceRuntime.Boot(nodes, ?replicationFactor = replicationFactor, ?failoverFactor = failoverFactor)

        static member InitLocal(totalNodes : int, ?hostname, ?storeProvider, ?debug, ?background) : MBraceRuntime =
            MBraceRuntime.InitLocal(totalNodes, 
                            ?hostname = hostname,
                            ?replicationFactor = None, 
                            ?storeProvider = storeProvider,
                            ?failoverFactor = None,
                            ?debug = debug, 
                            ?background = background)

    type MBrace = MBraceRuntime

namespace Nessos.MBrace.Runtime

    open Nessos.Thespian

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Client

    [<AutoOpen>]
    module RuntimeExtensions =

        type MBraceRuntime with
            static member FromActor(actor: Actor<RuntimeMsg>) =
                let wrapper = 
                    function
                    | RemoteMsg msg -> msg
                    | GetLastRecordedState rc -> GetAllNodes (ReplyChannel.map List.ofArray rc)

                let wrappedActor = Actor.map wrapper actor |> Actor.start

                new MBraceRuntime(wrappedActor, true)