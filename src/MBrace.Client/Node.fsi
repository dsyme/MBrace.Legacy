namespace Nessos.MBrace.Client

    open Nessos.Thespian

    open Nessos.MBrace
    open Nessos.MBrace.Runtime.Daemon.Configuration

    ///The module responsible for the LogEntry type.
    module Logs = begin
        type LogEntry = Utils.LogEntry
        type LogLevel = Utils.LogLevel
    
        ///Prints a sequence of LogEntry objects.
        val show : log:seq<LogEntry> -> unit
    end

    open Logs
    
    ///The type representing a {m}brace node.
    type MBraceNode =
        class
            interface System.IComparable
            internal new : nref:ActorRef<Runtime.Runtime> ->
                                             MBraceNode

            ///Create a new MBraceNode object. No node is spawned.
            new : uri:System.Uri -> MBraceNode
            ///Create a new MBraceNode object. No node is spawned.
            new : uri:string -> MBraceNode
            ///Create a new MBraceNode object. No node is spawned.
            new : hostname:string * port:int -> MBraceNode

            private new : nodeRef:ActorRef<Runtime.Runtime> * uri:System.Uri -> MBraceNode

            member CompareTo : y:obj -> int
            
            ///Get all the system logs from this node.
            member GetLogs : ?clear:bool -> Utils.LogEntry []
            
            override Equals : y:obj -> bool
            override GetHashCode : unit -> int
            
            ///Gets usage statistics for this node.
            member GetPerformanceCounters : unit -> Runtime.NodePerformanceInfo

            ///Kills violently this node. The node must be local.
            member Kill : unit -> unit

            ///<summary>Send a ping message to the node and return the number of milliseconds of the roundtrip.</summary>
            ///<param name="silent">Does not print a PING log in the system logs.</param>
            ///<param name="timeout">Timeout in milliseconds.</param>
            member Ping : ?silent:bool * ?timeout:int -> int
            
            ///Prints the system logs for this node.
            ///<param name="clear">Deletes the logs.</param>
            member ShowLogs : ?clear:bool -> unit
            
            override ToString : unit -> string
            
            ///Returns whether the node is active or not.
            member IsActive : bool

            ///Returns whether the node is local or not.
            member IsLocal : bool

            ///Gets the node's permissions.
            member internal Permissions : Runtime.Permissions

            ///Returns whether the node has master permissions.
            member internal PermittedMaster : bool
            ///Returns whether the node has slave permissions.
            member internal PermittedSlave : bool

            ///Returns the System.Diagnostics.Process object for this node.
            member Process : System.Diagnostics.Process option
            ///Returns a UUID bound to this node instance.
            member DeploymentId : System.Guid
            
            member internal Ref : ActorRef<Runtime.Runtime>
            
            ///Gets the node's state.
            member State : Runtime.NodeType
            
            ///Gets the node's uri.
            member Uri : System.Uri

            ///Sets the node's permissions.
            member internal Permissions : Runtime.Permissions with set
            
            ///Sets whether the node has master permissions.
            member internal PermittedMaster : bool with set

            ///Sets whether the node has slave permissions.
            member internal PermittedSlave : bool with set

            ///<summary>Spawns a new {m}brace daemon (node).</summary>
            ///<param name="arguments">Typed command line arguments passed to the shell.</param>
            ///<param name="background">Spawn in the background.</param>
            static member internal SpawnAsync : arguments:MBracedConfig list * ?background : bool -> Async<MBraceNode>

            ///<summary>Spawns a new {m}brace daemon (node).</summary>
            ///<param name="arguments">command line arguments passed to the shell.</param>
            ///<param name="background">Spawn in the background.</param>
            static member Spawn : arguments:string [] * ?background : bool -> MBraceNode

            ///<summary>Spawns a new {m}brace daemon (node).</summary>
            ///<param name="hostname">The hostname to be used by the node and the runtime.</param>
            ///<param name="primaryPort">The port the node listens to.</param>
            ///<param name="workerPorts">The ports pool use by the {m}brace workers.</param>
            ///<param name="logFiles">The files (path) to use for logging.</param>
            ///<param name="logLevel">The level of entries to be logged.</param>
            ///<param name="permissions">The permissions of the node.</param>
            ///<param name="serializerName">The name of the serializer to be used.</param>
            ///<param name="compressSerialization">Use compression during serialization.</param>
            ///<param name="debug">Run in Debug mode.</param>
            ///<param name="workingDirectory">The working directory used by the node.</param>
            ///<param name="useTemporaryWorkDir">Use a temporary folder as a working directory.</param>
            ///<param name="background">Spawn in the background.</param>
            ///<param name="storeProvider">The store provider to be used.</param>
            static member Spawn : ?hostname:string * ?primaryPort:int * ?workerPorts:int list *
                                ?logFiles:string list * ?logLevel:LogLevel *
                                ?permissions:Runtime.Permissions *
                                ?serializerName:string * ?compressSerialization:bool *
                                ?debug:bool * ?workingDirectory:string *
                                ?useTemporaryWorkDir:bool * ?background:bool *
                                ?storeProvider:StoreProvider -> MBraceNode

            ///<summary>Spawns a new {m}brace daemon (node).</summary>
            ///<param name="hostname">The hostname to be used by the node and the runtime.</param>
            ///<param name="primaryPort">The port the node listens to.</param>
            ///<param name="workerPorts">The ports pool use by the {m}brace workers.</param>
            ///<param name="logFiles">The files (path) to use for logging.</param>
            ///<param name="logLevel">The level of entries to be logged.</param>
            ///<param name="permissions">The permissions of the node.</param>
            ///<param name="serializerName">The name of the serializer to be used.</param>
            ///<param name="compressSerialization">Use compression during serialization.</param>
            ///<param name="debug">Run in Debug mode.</param>
            ///<param name="workingDirectory">The working directory used by the node.</param>
            ///<param name="useTemporaryWorkDir">Use a temporary folder as a working directory.</param>
            ///<param name="background">Spawn in the background.</param>
            ///<param name="storeProvider">The store provider to be used.</param>
            static member SpawnAsync : ?hostname:string * ?primaryPort:int * ?workerPorts:int list *
                                       ?logFiles:string list * ?logLevel:LogLevel *
                                       ?permissions:Runtime.Permissions *
                                       ?serializerName:string * ?compressSerialization:bool *
                                       ?debug:bool * ?workingDirectory:string *
                                       ?useTemporaryWorkDir:bool * ?background:bool *
                                       ?storeProvider:StoreProvider -> Async<MBraceNode>

            ///<summary>Spawns a new {m}brace daemon (node).</summary>
            ///<param name="nodeCount">The number of nodes to spawn.</param>
            ///<param name="workerPortsPerNode">The number of worker ports for each node.</param>
            ///<param name="hostname">The hostname to be used by the node and the runtime.</param>
            ///<param name="logFiles">The files (path) to use for logging.</param>
            ///<param name="logLevel">The level of entries to be logged.</param>
            ///<param name="permissions">The permissions of the nodes.</param>
            ///<param name="serializerName">The name of the serializer to be used.</param>
            ///<param name="compressSerialization">Use compression during serialization.</param>
            ///<param name="debug">Run in Debug mode.</param>
            ///<param name="background">Spawn in the background.</param>
            ///<param name="storeProvider">The store provider to be used.</param>
            static member SpawnMultiple : nodeCount:int * ?workerPortsPerNode:int *
                                        ?hostname:string * ?logFiles:string list *
                                        ?logLevel:LogLevel *
                                        ?permissions:Runtime.Permissions *
                                        ?serializerName:string * ?compressSerialization:bool *
                                        ?debug:bool * ?background:bool *
                                        ?storeProvider:StoreProvider -> MBraceNode list

            ///<summary>Prints information on the given collection of nodes.</summary>
            ///<param name="displayPerfCounters">Display performance information.</param>
            ///<param name="header">Specify a header for the table.</param>
            ///<param name="useBorders">Enable fancy mySQL-like bordering.</param>
            static member PrettyPrint : nodes:MBraceNode list * ?displayPerfCounters:bool * ?header:string * ?useBorders:bool -> string
        end

    type internal NodeInfo =
        class
            new : nrefs:seq<MBraceNode> -> NodeInfo
            member Display : ?displayPerfCounters : bool * ?useBorders : bool -> string
            member Alts : MBraceNode list
            member Idle : MBraceNode list
            member Master : MBraceNode option
            member Nodes : MBraceNode list
            member Slaves : MBraceNode list
            static member Create : nrefs:seq<Runtime.NodeRef> -> NodeInfo
            static member internal PrettyPrint : nodes:MBraceNode list list * ?displayPerfCounters : bool 
                                                        * ?headers : string * ?useBorders : bool -> string
        end

    /// This is an abbreviation to the MBraceNode type.
    type Node = MBraceNode