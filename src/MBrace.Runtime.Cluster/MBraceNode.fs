module Nessos.MBrace.Runtime.Definitions.MBraceNode

open System

open Nessos.Thespian
open Nessos.Thespian.ConcurrencyTools
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Cluster
open Nessos.Thespian.Cluster.BehaviorExtensions
open Nessos.Thespian.Cluster.BehaviorExtensions.FSM

open Nessos.MBrace
open Nessos.MBrace.Utils
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Logging

// updated state event
let internal StateChangeEvent = new Event<NodeState>()
/// node state observable
let stateChangeObservable = StateChangeEvent.Publish

// Dependency Injection : change
let private readEntriesFromMasterLogFile () =
    let file = IoC.Resolve<string>("jsonLogFile")
    JsonFileLogger.ReadLogs file

type internal MBraceNodeManager = MBraceNode

type State = 
    {
        DeploymentId: Guid
        Permissions : Permissions
    } 
with
    static member Empty = { DeploymentId = Guid.Empty; Permissions = IoC.Resolve<Permissions>() }

let private initMultiNodeRuntime (ctx: BehaviorContext<_>) (configuration: BootConfiguration) nodes = 
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
        let alts = altAddresses |> Array.map (fun addr -> Remote.TcpProtocol.ActorRef.fromUri (sprintf' "btcp://%O/*/runtime/%s" addr serializerName) : ActorRef<MBraceNode>)

        return alts
    }    

let private addressToRuntime (address: Address): ActorRef<MBraceNode> =
    let serializerName = Nessos.Thespian.Serialization.SerializerRegistry.GetDefaultSerializer().Name
    ActorRef.fromUri (sprintf' "btcp://%O/*/runtime/%s" address serializerName)
    
type private LogLevel = Nessos.Thespian.LogLevel
type private NodeType = Nessos.Thespian.Cluster.Common.NodeType


let private getNodeDeploymentInfo self nodeType permissions includePerfCounters =
    let nodeState =
        match nodeType with
        // TODO : AltMaster is not properly reported here
        | NodeType.Master -> NodeState.Master
        | NodeType.Slave -> NodeState.Slave
        | NodeType.Idle -> NodeState.Idle

    NodeDeploymentInfo.CreateLocal(permissions, nodeState, self, includePerfCounters)

let private getClusterDeploymentInfo deploymentId self nodeType permissions includePerfCounters (restNodes : ActorRef<MBraceNode> []) = async {
    
    let gatherThisNode = async { return getNodeDeploymentInfo self nodeType permissions includePerfCounters }
    let gatherOthers = restNodes |> Array.map (fun n -> n <!- fun ch -> GetNodeDeploymentInfo(ch, includePerfCounters))

    let! nodeInfo = seq { yield gatherThisNode ; yield! gatherOthers } |> Async.Parallel

    let! r = Cluster.ClusterManager <!- fun ch -> ResolveActivationRefs(ch, empDef/"master"/"processManager" |> ActivationReference.FromPath)
                    
    //Throws
    //KeyNotFoundException => allow to fall through;; SYSTEM FAULT
    //InvalidCastException => allow to fall through;; SYSTEM FAULT
    let processManager = r.[0] :?> ActorRef<ProcessManager>
    
    return
        {
            DeploymentId = deploymentId
            MasterNode = nodeInfo |> Array.find (fun n -> n.State = Master)

            // TODO
            ReplicationFactor = 0
            FailoverFactor = 0

            Nodes = nodeInfo
            StoreId = StoreRegistry.DefaultStoreInfo.ActivationInfo.Id
            ProcessManager = processManager
        }
}

let storeActivationProtocol (config : BootConfiguration) (restNodes : ActorRef<MBraceNode> []) = async {
    
    let negotiateUpload (storeInfo : StoreInfo) (node : ActorRef<MBraceNode>) = async {
        let! storeManager = node <!- GetStoreManager
        return! StoreManager.uploadStore storeInfo storeManager
    }

    let defaultStore = StoreRegistry.DefaultStoreInfo

    match config.StoreId with
    | Some id when defaultStore.Id <> id -> 
        invalidOp <| sprintf "Attempting to boot with incompatible store configuration '%s'" id.AssemblyQualifiedName
    | _ -> ()

    do! restNodes |> Seq.map (negotiateUpload defaultStore) |> Async.Parallel |> Async.Ignore
}


let rec private triggerNodeFailure (innerException: exn) (ctx: BehaviorContext<_>) (state: State) (msg: MBraceNodeManager) =
    let reply r = 
        ctx.LogError innerException
        match innerException with
        | SystemCorruptionException _ as e -> 
            //r <| Exception(new MBrace.SystemCorruptedException(e.Message, e.InnerException))
            r <| Exception(new SystemCorruptedException(e.Message, innerException))
        | e -> 
            //r <| Exception(new MBrace.SystemCorruptedException("Unexpected exception occurred.", e))
            r <| Exception(new SystemCorruptedException("Unexpected exception occurred.", innerException))
    let warning () = ctx.LogEvent(LogLevel.Error, sprintf' "Unexpected exception occurred: %A" innerException)
    async {
        match msg with
        | MasterBoot(RR ctx r, _) -> reply r 
        | GetLogDump(RR ctx r) -> reply r
        | Attach(RR ctx r, _) -> reply r
        | Detach(RR ctx r) -> reply r
        | GetNodeDeploymentInfo(RR ctx r,_) -> reply r
        | GetStoreManager(RR ctx r) -> reply r
        | GetClusterDeploymentInfo(RR ctx r,_) -> reply r
        | Ping(RR ctx r) -> reply r
        | GetInternals(RR ctx r) -> reply r
        | ShutdownSync(RR ctx r) -> reply r
        | ResetNodeState(RR ctx r) -> reply r
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
                    do StateChangeEvent.Trigger(Nessos.MBrace.Runtime.NodeState.Master)
                
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

                    do! storeActivationProtocol configuration nodes

                    let! alts = async {
                        if nodes.Length = 0 then
                            return! Async.Raise <| new InvalidOperationException("No nodes specified")
                        else 
                            //Throws
                            //-
                            return! initMultiNodeRuntime ctx configuration nodes
                    }

                    ctx.LogInfo "BOOT COMPLETE."

                    let! info = getClusterDeploymentInfo deploymentId ctx.Self NodeType.Master state.Permissions false nodes
                    reply <| Value info

                    return stay { state with DeploymentId = deploymentId }
                with e ->
                    return! triggerNodeFailure e ctx state msg

            | MasterBoot(RR ctx reply, _) ->
                reply <| Exception(new InvalidOperationException("Node is not idle."))

                return stay state

            | GetClusterDeploymentInfo(RR ctx reply, includePerfMetrics) when nodeType <> NodeType.Idle ->
                try
                    //FaultPoint
                    //-
                    let! nodes = Cluster.ClusterManager <!- ClusterManager.GetAllNodes

                    let otherNodes =
                        nodes
                        |> Seq.choose ActorRef.toUniTcpAddress
                        |> Seq.map addressToRuntime
                        |> Seq.filter (fun n -> n <> ctx.Self)
                        |> Seq.toArray

                    //FaultPoint
                    //-
                    let! info = getClusterDeploymentInfo state.DeploymentId ctx.Self NodeType.Master state.Permissions includePerfMetrics otherNodes

                    reply <| Value info

                    return stay state

                with FailureException _ as e ->
                        reply (Exception e)
                        return stay state  
                    | e -> return! triggerNodeFailure e ctx state msg

            | GetStoreManager(RR ctx reply) ->
                try
                    //Throws
                    //KeyNotFoundException => allow to fall through;; SYSTEM FAULT
                    //InvalidCastException => allow to fall through;; SYSTEM FAULT
                    let sm = Cluster.Get<Actor<StoreManager>>("StoreManager")

                    reply <| Value sm.Ref

                    return stay state

                with FailureException _ as e ->
                        reply (Exception e)
                        return stay state  
                    | e -> return! triggerNodeFailure e ctx state msg

            | ResetNodeState(RR ctx reply) ->
                reply <| Exception(NotImplementedException())
                return stay state

            | GetClusterDeploymentInfo(RR ctx reply, _) -> 
                reply <| Exception(new InvalidOperationException("Node is idle."))
                return stay state

//            | GetProcessManager(RR ctx reply) when nodeType <> NodeType.Idle ->
//                try
//                    //FaultPoint
//                    //-
//                    let! r = Cluster.ClusterManager <!- fun ch -> ResolveActivationRefs(ch, empDef/"master"/"processManager" |> ActivationReference.FromPath)
//                    
//                    //Throws
//                    //KeyNotFoundException => allow to fall through;; SYSTEM FAULT
//                    //InvalidCastException => allow to fall through;; SYSTEM FAULT
//                    let processManager = r.[0] :?> ActorRef<ProcessManager>
//
//                    reply (Value processManager)
//
//                    return stay state
//                with FailureException _ as e ->
//                        reply (Exception e)
//                        return stay state  
//                    | e -> return! triggerNodeFailure e ctx state msg
//
//            | GetProcessManager(RR ctx reply) ->
//                reply <| Exception(new InvalidOperationException("Node is idle."))
//
//                return stay state
//
//            | GetStoreId(RR ctx reply) ->
//                try
//                    reply <| Value StoreRegistry.DefaultStoreInfo.Id
//
//                    return stay state
//                with e ->
//                    return! triggerNodeFailure e ctx state msg
//
//            | GetMasterAndAlts(RR ctx reply) when nodeType <> NodeType.Idle ->
//                try
//                    //FaultPoint
//                    //-
//                    let! altNodes = Cluster.ClusterManager <!- GetAltNodes
//                    
//                    let masterAddress = Cluster.ClusterManager |> ActorRef.toUniTcpAddress |> Option.get
//                    let masterRuntime = addressToRuntime masterAddress
//
//                    let alts =
//                        altNodes
//                        |> Seq.map (fun altNodeManager -> altNodeManager |> ActorRef.toUniTcpAddress)
//                        |> Seq.choose id
//                        |> Seq.map addressToRuntime
//                        |> Seq.toArray
//
//                    reply <| Value (masterRuntime, alts)
//
//                    return stay state
//                with FailureException _ as e ->
//                        reply (Exception e)
//                        return stay state
//                    | e -> 
//                        return! triggerNodeFailure e ctx state msg
//
//            | GetMasterAndAlts(RR ctx reply) ->
//                reply <| Exception(new InvalidOperationException("Node is idle."))
//
//                return stay state
//
//            | GetDeploymentId(RR ctx reply) when nodeType <> NodeType.Idle ->
//                reply (Value state.DeploymentId)
//
//                return stay state
//
//            | GetDeploymentId(RR ctx reply) ->
//                reply <| Exception(new InvalidOperationException("Node is idle."))
//
//                return stay state
//
//            | GetAllNodes(RR ctx reply) when nodeType <> NodeType.Idle ->
//                try
//                    //FaultPoint
//                    //-
//                    let! nodes = Cluster.ClusterManager <!- ClusterManager.GetAllNodes
//
//                    let mbraceNodes =
//                        nodes
//                        |> Seq.map ActorRef.toUniTcpAddress
//                        |> Seq.choose id
//                        |> Seq.map addressToRuntime
//                        |> Seq.toArray
//
//                    reply (Value mbraceNodes)
//
//                    return stay state
//                with FailureException _ as e ->
//                        reply (Exception e)
//                        return stay state
//                    | e -> return! triggerNodeFailure e ctx state msg
//
//            | GetAllNodes(RR ctx reply) ->
//                reply <| Exception(new InvalidOperationException("Node is idle."))
//
//                return stay state

            | GetLogDump(RR ctx reply) ->
                try
                    let entries = readEntriesFromMasterLogFile ()

                    reply <| Value entries

                    return stay state
                with e ->
                    return! triggerNodeFailure e ctx state msg

            | Attach(RR ctx reply, mbraceNode) when nodeType <> NodeType.Idle ->
                try
                    let! storeManager = mbraceNode <!- GetStoreManager
                    do! StoreManager.uploadStore StoreRegistry.DefaultStoreInfo storeManager

                    let nodeManager =
                        let addr = ActorRef.toEndPoint mbraceNode
                                   |> Address.FromEndPoint

                        let serializerName = Nessos.Thespian.Serialization.SerializerRegistry.GetDefaultSerializer().Name
                        ActorRef.fromUri (sprintf' "utcp://%O/*/nodeManager/%s" addr serializerName)

                    //FaultPoint
                    //-
                    try
                        // TODO : should be blocking
                        do! Cluster.ClusterManager <-!- AddNode nodeManager
                        // Interim solution ; sleep
                        do! Async.Sleep 1000
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
                    do! Cluster.NodeManager <-!- DetachFromCluster
                    //FaultPoint
                    //-

                    // TODO : should be blocking
                    do! Cluster.ClusterManager <-!- ClusterManager.RemoveNode Cluster.NodeManager
                    // Interim solution ; sleep
                    do! Async.Sleep 1000
                
                    reply nothing

                    return stay state
                with FailureException _ as e ->
                        reply (Exception e)
                        return stay state
                    | e -> return! triggerNodeFailure e ctx state msg

            | Detach(RR ctx reply) ->
                reply <| Exception(new InvalidOperationException("Node is idle."))

                return stay state

            | SetNodePermissions perms ->
                return stay { state with Permissions = perms }

            | Ping(RR ctx reply) ->
                ctx.LogInfo "PING"

                reply nothing

                return stay state

            | GetNodeDeploymentInfo(RR ctx reply, includePerfCounters) ->
                try
                    let info = getNodeDeploymentInfo ctx.Self nodeType state.Permissions includePerfCounters

                    reply <| Value info

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
        | GetStoreManager(RR ctx r) -> reply r
        | GetLogDump(RR ctx r) ->
            try
                let entries = readEntriesFromMasterLogFile ()
                r <| Value entries

            with e -> ctx.LogError e
        | Attach(RR ctx r, _) -> reply r
        | Detach(RR ctx r) -> reply r
        | Ping(RR ctx reply) ->
            try
                ctx.LogInfo "PING"
                reply nothing
            with e -> ctx.LogError e
        | GetClusterDeploymentInfo(RR ctx r,_) ->
            try reply r
            with e -> ctx.LogError e
        | GetNodeDeploymentInfo(RR ctx r,_) ->
            try reply r
            with e -> ctx.LogError e
        | ResetNodeState(RR ctx r) -> reply r
        | GetInternals(RR ctx r) -> reply r
        | SetNodePermissions _
        | Shutdown -> warning()

        return stay state
    }
