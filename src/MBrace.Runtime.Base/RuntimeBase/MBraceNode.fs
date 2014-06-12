namespace Nessos.MBrace.Runtime

    open System

    open Nessos.Thespian

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime.Store
    open Nessos.MBrace.Runtime.Logging

    type NodeState =
        | Master 
        | AltMaster 
        | Slave 
        | Idle
    with
        override t.ToString() =
            match t with
            | Master -> "Master"
            | AltMaster -> "Alt Master"
            | Slave -> "Slave"
            | Idle -> "Idle"
                
    and Permissions =
        | None   = 0
        | Slave  = 1
        | Master = 2
        | All    = 3

    and BootConfiguration =
        {
            Nodes : ActorRef<MBraceNode> []
            ReplicationFactor : int
            FailoverFactor : int
        }

    and NodeDeploymentInfo =
        {
            /// A Guid that identifies this process instance
            DeploymentId : Guid

            /// Actor Reference for interaction with node
            Reference : ActorRef<MBraceNode>

            /// Host identifier ; used to determine if process is local
            HostId : HostId

            /// OS process id of daemon ; used for local node management
            ProcessId : int

            /// Current permissions set for node
            Permissions : Permissions

            /// Determines the current node state
            State : NodeState

            /// Info on node performance
            PerformanceInfo : NodePerformanceInfo option
        }

    and ClusterDeploymentInfo =
        {
            /// Unique identifier for cluster deployment
            DeploymentId : Guid

            /// Runtime store Identifier
            StoreId : StoreId

            /// Number of additional nodes replicating cloud process states.
            ReplicationFactor : int

            /// Number of alternative master nodes used in the runtime.
            FailoverFactor : int

            /// Cluster Master node
            MasterNode : NodeDeploymentInfo

            /// Information on nodes that constitute the cluster.
            Nodes : NodeDeploymentInfo []

            /// Current process manager instance
            ProcessManager : ActorRef<ProcessManager>
        }


    and MBraceNode =
        /// Returns node-specifig information
        | GetNodeDeploymentInfo of IReplyChannel<NodeDeploymentInfo> * includePerfCounters:bool
        /// Returns cluster-specific information
        | GetClusterDeploymentInfo of IReplyChannel<ClusterDeploymentInfo> * includePerfCounters:bool
        /// Receive a dump of all logs printed by the node
        | GetLogDump of IReplyChannel<SystemLogEntry []>
        /// Boots the runtime. The node receiving this message becomes the Master Node
        | MasterBoot of IReplyChannel<ClusterDeploymentInfo> * BootConfiguration
        /// Tell a node to attach itself to the runtime.
        | Attach of IReplyChannel<unit> * ActorRef<MBraceNode>
        /// Tell a node to detach itself from the runtime it is attached to.
        | Detach of IReplyChannel<unit>
        /// Sets node permissions
        | SetNodePermissions of Permissions
        /// Ping the node
        | Ping of IReplyChannel<unit>
        /// Shuts down a cluster (deprecate?)
        | Shutdown
        /// Shuts down a cluster sychronously
        | ShutdownSync of IReplyChannel<unit>
        /// Try resetting node state, if at all possible
        | ResetNodeState of IReplyChannel<unit>
        /// Internal communication, not for client use
        | GetInternals of IReplyChannel<ActorRef>