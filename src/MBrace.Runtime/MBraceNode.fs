module Nessos.MBrace.Runtime.Definitions.MBraceNode

open System

open Nessos.Thespian
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Cluster
open Nessos.Thespian.Cluster.BehaviorExtensions
open Nessos.Thespian.Cluster.BehaviorExtensions.FSM

open Nessos.MBrace
open Nessos.MBrace.Utils
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Logging
open Nessos.MBrace.Runtime.Store

// updated state event
let internal StateChangeEvent = new Event<NodeType>()
/// node state observable
let stateChangeObservable = StateChangeEvent.Publish

// Dependency Injection : change
let private readEntriesFromMasterLogFile () =
    let file = IoC.Resolve<string>("masterLogFile")
    JsonFileLogger.ReadLogs file

type internal MBraceNodeManager = Runtime

type State = {
    DeploymentId: Guid
} with static member Empty = { DeploymentId = Guid.Empty; }

let private initMultiNodeRuntime (ctx: BehaviorContext<_>) (configuration: Configuration) nodes = 
    async {
        let isInMemory = nodes |> Seq.forall ActorRef.isCollocated

        let! nodeManagers = 
            nodes
            //FaultPoint
            //-
            |> Broadcast.postWithReply GetInternals
            |> Broadcast.exec

        let nodeManagers = 
            nodeManagers |> Seq.map (fun nm -> nm :?> ActorRef<NodeManager>)
            |> Seq.toArray

        let clusterConfiguration = {
            ClusterId = "HEAD"
            Nodes = nodeManagers
            ReplicationFactor = configuration.ReplicationFactor
            FailoverFactor = configuration.FailoverFactor
            NodeDeadNotify = fun n -> async { do! Cluster.ClusterManager <-!- ClusterManager.RemoveNode n }
        }

        ctx.LogInfo "Initializing ClusterManager..."

        //init cluster on this node
        let! altAddresses = Cluster.NodeManager <!- fun ch -> InitCluster(ch, clusterConfiguration)
        //when the runtime is in-memory this returns Array.empty
        assert (if isInMemory then Array.isEmpty altAddresses else true)

        //wait for events to be processed
        do! Cluster.NodeManager <!- SyncNodeEvents
        
        let serializerName = Nessos.Thespian.Serialization.SerializerRegistry.GetDefaultSerializer().Name
        let alts = altAddresses |> Array.map (fun addr -> Remote.TcpProtocol.ActorRef.fromUri (sprintf' "btcp://%O/*/runtime/%s" addr serializerName) : ActorRef<Runtime>)

        return alts
    }

let private addressToRuntime (address: Address): ActorRef<Runtime> =
    let serializerName = Nessos.Thespian.Serialization.SerializerRegistry.GetDefaultSerializer().Name
    ActorRef.fromUri (sprintf' "btcp://%O/*/runtime/%s" address serializerName)
    
type private LogLevel = Nessos.Thespian.LogLevel
type private NodeType = Nessos.Thespian.Cluster.Common.NodeType

let rec private triggerNodeFailure (innerException: exn) (ctx: BehaviorContext<_>) (state: State) (msg: MBraceNodeManager) =
    let reply r = 
        ctx.LogError innerException
        match innerException with
        | SystemCorruptionException _ as e -> 
            //r <| Exception(new MBrace.SystemCorruptedException(e.Message, e.InnerException))
            r <| Exception(new SystemCorruptedException(e.Message))
        | e -> 
            //r <| Exception(new MBrace.SystemCorruptedException("Unexpected exception occurred.", e))
            r <| Exception(new SystemCorruptedException("Unexpected exception occurred."))
    let warning () = ctx.LogEvent(LogLevel.Error, sprintf' "Unexpected exception occurred: %A" innerException)
    async {
        match msg with
        | MasterBoot(RR ctx r, _) -> reply r 
        | GetProcessManager(RR ctx r) -> reply r
        | GetMasterAndAlts(RR ctx r) -> reply r
        | GetDeploymentId(RR ctx r) -> reply r
        | GetStoreId(RR ctx r) -> reply r
        | GetAllNodes(RR ctx r) -> reply r
        | GetLogDump(RR ctx r) -> reply r
        | Attach(RR ctx r, _) -> reply r
        | Detach(RR ctx r) -> reply r
//        | GetNodeState(RR ctx r) -> reply r
//        | GetNodePermissions(RR ctx r) -> reply r
        | GetNodeDeploymentInfo(RR ctx r) -> reply r
        | Ping(RR ctx r, _) -> reply r
        | GetNodePerformanceCounters(RR ctx r) -> reply r
        | GetInternals(RR ctx r) -> reply r
        | ShutdownSync(RR ctx r) -> reply r
        | SetNodePermissions _
        | Shutdown -> warning()

        try
            //FaultPoint
            //SystemCorruptionException => SYSTEM FAULT;; already in that;; ignore
            let! nodeType = Cluster.NodeManager <!- GetNodeType

            if nodeType <> NodeType.Idle then
                Cluster.NodeManager <-- DetachFromCluster
                //FaultPoint
                //FailureException => master failure;; ignore
                Cluster.ClusterManager <-- ClusterManager.RemoveNode Cluster.NodeManager

        with e -> ()

        return goto mbraceNodeManagerBehaviorFailed state
    }

//and private getLogDump reply clear =
//    async {
//        match imemLogger with
//        | None -> SystemException("Log dumping not supported in this runtime.") :> exn |> Exception |> reply
//        | Some logger ->
//            logger.Dump() |> Value |> reply
//
//            if clear then logger.Clear()
//    } 

and mbraceNodeManagerBehavior (ctx: BehaviorContext<_>) (state: State) (msg: MBraceNodeManager) =
    async {
        try
            //FaultPoint
            //SystemCorruptionException => SYSTEM FAULT
            let! nodeType = Cluster.NodeManager <!- GetNodeType

            match msg with
            | MasterBoot(RR ctx reply, configuration) when nodeType = NodeType.Idle ->
                try
                    // preactively trigger state change on master boot
                    do StateChangeEvent.Trigger(Nessos.MBrace.Runtime.NodeType.Master)
                
                    ctx.LogInfo "MASTER BOOT..."
                    ctx.LogInfo "---------------------"
                    ctx.LogInfo "Initial configuration"
                    ctx.LogInfo "Nodes: "
                    ctx.LogInfo <| sprintf "%A" configuration.Nodes
                    ctx.LogInfo <| sprintf "Replication factor: %d" configuration.ReplicationFactor
                    ctx.LogInfo <| sprintf "Failover factor: %d" configuration.FailoverFactor
                    ctx.LogInfo "--------------------"

                    let deploymentId = Guid.NewGuid()

                    ctx.LogInfo "--------------------"
                    ctx.LogInfo <| sprintf "Deployment id: %A" deploymentId
                    ctx.LogInfo "--------------------"
                    ctx.LogInfo "--------------------"

                    ctx.LogInfo "Preparing..."

                    let nodes = configuration.Nodes |> Array.filter (fun node' -> node' <> ctx.Self)

                    let! alts = async {
                        if nodes.Length = 0 then
                            return! Async.Raise <| new InvalidOperationException("No nodes specified")
                        else 
                            //Throws
                            //-
                            return! initMultiNodeRuntime ctx configuration nodes
                    }

                    ctx.LogInfo "BOOT COMPLETE."

                    reply <| Value (ctx.Self, alts)

                    return stay { state with DeploymentId = deploymentId }
                with e ->
                    return! triggerNodeFailure e ctx state msg

            | MasterBoot(RR ctx reply, _) ->
                reply <| Exception(new InvalidOperationException("Node is not idle."))

                return stay state

            | GetProcessManager(RR ctx reply) when nodeType <> NodeType.Idle ->
                try
                    //FaultPoint
                    //-
                    let! r = Cluster.ClusterManager <!- fun ch -> ResolveActivationRefs(ch, empDef/"master"/"processManager" |> ActivationReference.FromPath)
                    
                    //Throws
                    //KeyNotFoundException => allow to fall through;; SYSTEM FAULT
                    //InvalidCastException => allow to fall through;; SYSTEM FAULT
                    let processManager = r.[0] :?> ActorRef<ProcessManager>

                    reply (Value processManager)

                    return stay state
                with FailureException _ as e ->
                        reply (Exception e)
                        return stay state  
                    | e -> return! triggerNodeFailure e ctx state msg

            | GetProcessManager(RR ctx reply) ->
                reply <| Exception(new InvalidOperationException("Node is idle."))

                return stay state

            | GetStoreId(RR ctx reply) ->
                try
                    reply <| Value StoreRegistry.DefaultStore.Id

                    return stay state
                with e ->
                    return! triggerNodeFailure e ctx state msg

            | GetMasterAndAlts(RR ctx reply) when nodeType <> NodeType.Idle ->
                try
                    //FaultPoint
                    //-
                    let! altNodes = Cluster.ClusterManager <!- GetAltNodes
                    
                    let masterAddress = Cluster.ClusterManager |> ActorRef.toUniTcpAddress |> Option.get
                    let masterRuntime = addressToRuntime masterAddress

                    let alts =
                        altNodes
                        |> Seq.map (fun altNodeManager -> altNodeManager |> ActorRef.toUniTcpAddress)
                        |> Seq.choose id
                        |> Seq.map addressToRuntime
                        |> Seq.toArray

                    reply <| Value (masterRuntime, alts)

                    return stay state
                with FailureException _ as e ->
                        reply (Exception e)
                        return stay state
                    | e -> 
                        return! triggerNodeFailure e ctx state msg

            | GetMasterAndAlts(RR ctx reply) ->
                reply <| Exception(new InvalidOperationException("Node is idle."))

                return stay state

            | GetDeploymentId(RR ctx reply) when nodeType <> NodeType.Idle ->
                reply (Value state.DeploymentId)

                return stay state

            | GetDeploymentId(RR ctx reply) ->
                reply <| Exception(new InvalidOperationException("Node is idle."))

                return stay state

            | GetAllNodes(RR ctx reply) when nodeType <> NodeType.Idle ->
                try
                    //FaultPoint
                    //-
                    let! nodes = Cluster.ClusterManager <!- ClusterManager.GetAllNodes

                    let mbraceNodes =
                        nodes
                        |> Seq.map ActorRef.toUniTcpAddress
                        |> Seq.choose id
                        |> Seq.map addressToRuntime
                        |> Seq.toArray

                    reply (Value mbraceNodes)

                    return stay state
                with FailureException _ as e ->
                        reply (Exception e)
                        return stay state
                    | e -> return! triggerNodeFailure e ctx state msg

            | GetAllNodes(RR ctx reply) ->
                reply <| Exception(new InvalidOperationException("Node is idle."))

                return stay state

            | GetLogDump(RR ctx reply) ->
                try
                    let entries = readEntriesFromMasterLogFile ()

                    reply <| Value entries

                    return stay state
                with e ->
                    return! triggerNodeFailure e ctx state msg

            | Attach(RR ctx reply, mbraceNode) when nodeType <> NodeType.Idle ->
                try
                    let nodeManager =
                        let addr = ActorRef.toEndPoint mbraceNode
                                   |> Address.FromEndPoint

                        let serializerName = Nessos.Thespian.Serialization.SerializerRegistry.GetDefaultSerializer().Name
                        ActorRef.fromUri (sprintf' "utcp://%O/*/nodeManager/%s" addr serializerName)

                    //FaultPoint
                    //-
                    try
                        do! Cluster.ClusterManager <-!- AddNode nodeManager
                        reply nothing
                    with e ->
                        reply <| Exception e

                    return stay state
                with e ->
                    return! triggerNodeFailure e ctx state msg

            | Attach(RR ctx reply, _) ->
                reply <| Exception(new InvalidOperationException("Node is idle."))

                return stay state

            | Detach(RR ctx reply) when nodeType <> NodeType.Idle ->
                try
                    Cluster.NodeManager <-- DetachFromCluster
                    //FaultPoint
                    //-
                    Cluster.ClusterManager <-- ClusterManager.RemoveNode Cluster.NodeManager
                
                    reply nothing

                    return stay state
                with FailureException _ as e ->
                        reply (Exception e)
                        return stay state
                    | e -> return! triggerNodeFailure e ctx state msg

            | Detach(RR ctx reply) ->
                reply <| Exception(new InvalidOperationException("Node is idle."))

                return stay state

            | SetNodePermissions _ ->
                return stay state

            | Ping(RR ctx reply, silence) ->
                if not silence then ctx.LogInfo "PING"

                reply nothing

                return stay state

            | GetNodeDeploymentInfo(RR ctx reply) ->
                let nodeType =
                    match nodeType with
                    | Nessos.Thespian.Cluster.Common.NodeType.Master -> Nessos.MBrace.Runtime.NodeType.Master
                    | Nessos.Thespian.Cluster.Common.NodeType.Slave -> Nessos.MBrace.Runtime.NodeType.Slave
                    | Nessos.Thespian.Cluster.Common.NodeType.Idle -> Nessos.MBrace.Runtime.NodeType.Idle

                let info = Utils.mkNodeDeploymentInfo Permissions.All nodeType

                reply <| Value info

                return stay state

            | GetNodePerformanceCounters(RR ctx reply) ->
                try          
                    PerformanceMonitor.getCounters ()
                    |> Value
                    |> reply

                    return stay state
                with e -> 
                    return! triggerNodeFailure e ctx state msg

            | Shutdown when nodeType <> NodeType.Idle ->
                try
                    //FaultPoint
                    //-
                    Cluster.ClusterManager <-- KillCluster

                    return stay state
                with e -> return! triggerNodeFailure e ctx state msg

            | Shutdown ->
                ctx.LogWarning "Unable to Shutdown. Node is idle."

                return stay state

            | ShutdownSync(RR ctx reply) when nodeType <> NodeType.Idle ->
                try
                    //FaultPoint
                    //-
                    do! Cluster.ClusterManager <!- KillClusterSync

                    reply nothing

                    return stay state
                with FailureException _ as e ->
                        reply (Exception e)
                        return stay state
                    | e -> return! triggerNodeFailure e ctx state msg

            | ShutdownSync(RR ctx reply) ->
                try
                    reply <| Exception (new InvalidOperationException("Unable to Shutdown. Node is idle."))

                    return stay state
                with e ->
                    return! triggerNodeFailure e ctx state msg

            | GetInternals(RR ctx reply) ->
                try
                    Cluster.NodeManager :> ActorRef
                    |> Value
                    |> reply

                    return stay state
                with e ->
                    return! triggerNodeFailure e ctx state msg

        with MessageHandlingException2(SystemCorruptionException _ as e)
             | e -> return! triggerNodeFailure e ctx state msg
    }

and mbraceNodeManagerBehaviorFailed (ctx: BehaviorContext<_>) (state: State) (msg: MBraceNodeManager) =
    let reply r = r <| Exception(new SystemFailedException "System is in failed state.")
    let warning () = ctx.LogWarning "System is in failed state. No message is processed."
    async {
        match msg with
        | ShutdownSync(RR ctx r) -> reply r
        | MasterBoot(RR ctx r, _) -> reply r
        | GetProcessManager(RR ctx r) -> reply r
        | GetMasterAndAlts(RR ctx r) -> reply r
        | GetDeploymentId(RR ctx r) -> reply r
        | GetStoreId(RR ctx r) -> reply r
        | GetAllNodes(RR ctx r) -> reply r
        | GetLogDump(RR ctx r) ->
            try
                let entries = readEntriesFromMasterLogFile ()
                r <| Value entries

            with e -> ctx.LogError e
        | Attach(RR ctx r, _) -> reply r
        | Detach(RR ctx r) -> reply r
//        | GetNodeState(RR ctx r) -> reply r //TODO!!! Return a failed state.
//        | GetNodePermissions(RR ctx r) -> r (Value Nessos.MBrace.Runtime.CommonAPI.Permissions.All)
        | Ping(RR ctx reply, silence) ->
            try
                if not silence then ctx.LogInfo "PING"

                reply nothing
            with e -> ctx.LogError e
        | GetNodeDeploymentInfo(RR ctx r) ->
            try reply r
            with e -> ctx.LogError e
        | GetNodePerformanceCounters(RR ctx r) -> reply r
        | GetInternals(RR ctx r) -> reply r
        | SetNodePermissions _
        | Shutdown -> warning()

        return stay state
    }
