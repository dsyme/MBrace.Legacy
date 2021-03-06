﻿module internal Nessos.MBrace.Runtime.Definitions.ProcessDomainManager

module Configuration =
    let IsolateProcesses = "IsolateProcesses"
    let PortPool = "PortPool"
    let ProcessDomainPoolSize = "ProcessDomainPoolSize"

open System
open System.Diagnostics

open Nessos.Thespian
open Nessos.Thespian.ConcurrencyTools
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Cluster
open Nessos.Thespian.ImemDb

open Nessos.Vagrant

open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.ProcessDomain.Configuration
open Nessos.MBrace.Utils
open Nessos.MBrace.Utils.String


//Process creation strategy
//A single process domain may host more than one cloud processes
//How is a new process allocated to a process domain is controlled by
//the process creation strategy.
//A new process can either:
//i). Reuse an existing process domain, if that process domain has all the required
//dependencies already loaded
//ii). Extend an existing process domain, if that process domain has some of the required
//dependencies, and there are no conflicting dependencies
//iii). Create a new process domain to host the process.
//Note: These are purely disjoint. For a particular process domain and process only one of these
//is possible.
//By default, a process is allocated to the process domain with the least hosted processes,
//if that can be reused or extended. If reuse/extend is not possible for that process domain,
//a new process domain is created to host the process.
//This behavior can be changed by assigning priorities to reuse/extend/create, from 0 to 100.
//The higher the priotiy the more likely this strategy is going to be chosen.
//How it works: Candidate process domains are sorted is ascending order of hosted processes,
//and for each the possible strategy is determined. Then, the process domains are re-sorted,
//where the hosted process count is descreased by priority percent. The first process domain
//and strategy is chosen in the generated order.

type private ProcessCreationStrategy = Reuse | Extend of AssemblyId list | Create
//priorities for process creation strategy; use 0 to 100, 100 meaning highest priority
let private reusePriority = 60
let private extendPriority = 50
let private createPriority = 0

type OsProcess = System.Diagnostics.Process

type State = {
    Db: ProcessDomainDb
    //ProcessMonitor: ReliableActorRef<Replicated<ProcessMonitor, ProcessMonitorDb>>
    PortPool: int list
} with
    static member Init(?portPool: int list) = {
        Db = ProcessDomainDb.Create()
        //ProcessMonitor = ReliableActorRef.FromRef <| ActorRef.empty()
        PortPool = defaultArg portPool []
    }

type private AssemblyLoadState =
    | NotLoaded
    | Loaded
    | InCompatible

//TODO!!! Handle failures
let private createProcessDomain (ctx: BehaviorContext<_>) clusterManager processDomainId preloadAssemblies portOpt =
    async {
        ctx.LogInfo (sprintf' "Creating process domain %A..." processDomainId)

        //create os process of cloud process
        let ospid = System.Diagnostics.Process.GetCurrentProcess().Id
        let debugMode = defaultArg (IoC.TryResolve<bool> "debugMode") false

        let storeInfo = StoreRegistry.DefaultStoreInfo.ActivationInfo

        // protocol specific! should be changed
        let primaryAddr = IoC.Resolve<Address> "primaryAddress"
        let minThreads = IoC.Resolve<int> "minThreadsInThreadPool"
             
        let args =
            [
                yield Parent_Pid ospid
                yield Process_Domain_Id processDomainId
                yield Working_Directory SystemConfiguration.WorkingDirectory
                yield Parent_Address <| primaryAddr.ToString ()
                yield Store_Activator <| Serialization.Serialize storeInfo
                yield Min_Threads minThreads

                yield Debug debugMode
                match portOpt with
                | Some selectedPort ->
                    yield HostName TcpListenerPool.DefaultHostname
                    yield Port selectedPort
                | None -> ()
            ]

        let mbraced = SystemConfiguration.MBraceWorkerExecutablePath
        let args = workerConfig.PrintCommandLineFlat args

        use nodeManagerReceiver = Receiver.create()
                                  |> Receiver.rename "activatorReceiver"
                                  |> Receiver.publish [Unidirectional.UTcp()]
                                  |> Receiver.start
        
        let awaitNodeManager = nodeManagerReceiver |> Receiver.toObservable |> Async.AwaitObservable
        ctx.LogInfo (sprintf' "Receiver address %O" primaryAddr)
        ctx.LogInfo "Spawning process domain..."

#if APPDOMAIN_ISOLATION
        let appDomain = AppDomain.CreateDomain(processDomainId.ToString())
        async { appDomain.ExecuteAssembly(mbraced, args) |> ignore } |> Async.Start
        let killF () = AppDomain.Unload appDomain
#else
        let startInfo = new ProcessStartInfo(mbraced, args)

        startInfo.UseShellExecute <- false
        startInfo.CreateNoWindow <- true
        startInfo.RedirectStandardOutput <- true
        startInfo.RedirectStandardError <- true
        startInfo.WorkingDirectory <- System.IO.Path.GetDirectoryName mbraced

        let osProcess = Process.Start(startInfo)

        // TODO : register IDisposables for later disposal
        let d1 = osProcess.OutputDataReceived.Subscribe(fun (args : DataReceivedEventArgs) -> Console.WriteLine args.Data)
        let d2 = osProcess.ErrorDataReceived.Subscribe(fun (args : DataReceivedEventArgs) -> Console.Error.WriteLine args.Data)

        osProcess.EnableRaisingEvents <- true
        osProcess.BeginOutputReadLine()
        osProcess.BeginErrorReadLine()

        let killF () = osProcess.Kill()
#endif
        
        //receive NodeManager and confirm it
        //TODO!!! Add timeout on wait
        let! R(reply), processDomainNodeManager = awaitNodeManager
        reply nothing

        ctx.LogInfo (sprintf' "Process domain %A spawned." processDomainId)

        ctx.LogInfo "Adding to cluster..."
        do! clusterManager <!- fun ch -> AddNodeSync(ch, processDomainNodeManager)
        
        ctx.LogInfo "Activating assembly manager..."

        let assemblyManagerRecord = {
            Definition = empDef/"common"/"assemblyManager"
            Instance = Some 0
            Configuration = ActivationConfiguration.Empty
            ActivationStrategy = ActivationStrategy.specificNode processDomainNodeManager
        }

        try
            //FaultPoint
            //MessageHandlingException InvalidActivationStrategy => invalid argument;; collocation strategy not supported ;; SYSTEM FAULT
            //MessageHandlingException CyclicDefininitionDependency => invalid configuration;; SYSTEM FAULT
            //MessageHandlingException OutOfNodesExceptions => unable to recover due to lack of nodes;; SYSTEM FAULT
            //MessageHandlingException KeyNotFoundException ;; from NodeManager => definition.Path not found in node;; SYSTEM FAULT
            //MessageHandlingException ActivationFailureException ;; from NodeManager => failed to activate definition.Path;; SYSTEM FAULT
            //MessageHandlingException SystemCorruptionException => system failure occurred during execution;; SYSTEM FAULT
            //MessageHandlingException SystemFailureException => Global system failure;; SYSTEM FAULT
            //FailureException => SYSTEM FAULT
            do! clusterManager <!- fun ch -> ActivateDefinition(ch, assemblyManagerRecord)
        with MessageHandlingException2 e | e -> return! Async.Raise <| SystemCorruptionException("System entered corrupted state.", e)

        ctx.LogInfo "Resolving assmebly manager..."

        let! r = clusterManager <!- fun ch -> ResolveActivationRefs(ch, { Definition = assemblyManagerRecord.Definition; InstanceId = 0 })
        let nodeAssemblyManager = ReliableActorRef.FromRef (r.[0] :?> ActorRef<AssemblyManager>)

        ctx.LogInfo "Loading assemblies..."

        //FaultPoint
        //-
        do! nodeAssemblyManager <!- fun ch -> LoadAssembliesSync(ch, preloadAssemblies)

        ctx.LogInfo "Process domain created."

        return processDomainNodeManager, killF
    }

let rec processDomainManagerBehavior (processDomainClusterManager: ActorRef<ClusterManager>)
                                     (ctx: BehaviorContext<_>)
                                     (state: State)
                                     (msg: ProcessDomainManager) =

    async {
        match msg with
        | CreateProcessDomain(RR ctx reply, preloadAssemblies) ->
            //TODO!!! Sanitize failure handling. Use RevAsync perhaps

            let processDomainId = Guid.NewGuid()

            try
                //First check if we can assign the process domain a public endpoint
                let portOpt, state' =
                    match state.PortPool with
                    | [] -> None, state
                    | available::rest -> Some available, { state with PortPool = rest }

                let! processDomainNodeManager, killF = createProcessDomain ctx processDomainClusterManager processDomainId preloadAssemblies portOpt

                let clusterProxyManager, proxyMap =
                    if portOpt.IsNone then
                        ctx.LogInfo "Starting cluster proxy manager..."

                        let proxyMap = Atom.atom Map.empty<ActivationReference, ReliableActorRef<RawProxy>>

                        let name = sprintf' "clusterProxyManager.%A" processDomainId
                        Actor.bind <| Behavior.stateless (ClusterProxy.clusterProxyBehavior proxyMap)
                        |> Actor.subscribeLog (Default.actorEventHandler Default.fatalActorFailure name)
                        |> Actor.rename name
                        |> Actor.publish [Unidirectional.UTcp()]
                        |> Actor.start
                        |> Some,
                        Some proxyMap
                    else None, None

                ctx.LogInfo (sprintf' "Process domain %A ready." processDomainId)

                reply <| Value (processDomainNodeManager, processDomainId, portOpt.IsSome)

                return { state' with 
                            Db = state.Db |> Database.insert <@ fun db -> db.ProcessDomain @> {
                                Id = processDomainId
                                NodeManager = ReliableActorRef.FromRef processDomainNodeManager
                                LoadedAssemblies = preloadAssemblies
                                Port = portOpt
                                ClusterProxyManager = clusterProxyManager
                                ClusterProxyMap = proxyMap
                                KillF = killF
                            }
                        }
            with e -> 
                ctx.LogInfo "Process domain creation failed due to error."
                ctx.LogError e

                ctx.Self <-- DestroyProcessDomain processDomainId

                reply <| Exception e

                return state

        | DestroyProcessDomain processDomainId ->
            try
                match state.Db.ProcessDomain.DataMap.TryFind processDomainId with
                | Some { NodeManager = processDomainNodeManager; Port = portOpt; ClusterProxyManager = clusterProxyManager; KillF = killF } ->
                    ctx.LogInfo <| sprintf "Destroying process domain: %A" processDomainId

                    processDomainClusterManager <-- DetachNodes [| processDomainNodeManager |]

                    ctx.LogInfo "Detach from process domain cluster triggered."

                    do if clusterProxyManager.IsSome then clusterProxyManager.Value.Stop()

                    do killF()

                    return { state with
                                Db = state.Db |> Database.remove <@ fun db -> db.ProcessDomain @> processDomainId
                                              |> Database.delete <@ fun db -> db.Process @> <@ fun cloudProcess -> cloudProcess.ProcessDomain = processDomainId @>
                                PortPool = match portOpt with Some port -> port::state.PortPool | None -> state.PortPool
                            }
                | None -> 
                    ctx.LogWarning <| sprintf' "Attempted to destroy a non existent process domain: %A" processDomainId

                    return state
            with e ->
                //TODO!!! unxpected exception;; trigger system fault
                ctx.LogError e

                return state

        | ClearProcessDomains(RR ctx reply) ->
            try
                ctx.LogInfo "Clearing all process domains..."
                
                let processDomains =
                    Query.from state.Db.ProcessDomain
                    |> Query.toSeq
                    |> Seq.cache
                
                ctx.LogInfo "Detaching process domain nodes from process domain cluster..."

                do! processDomains 
                    |> Seq.map (fun processDomain -> processDomain.NodeManager.UnreliableRef)
                    |> Seq.toArray
                    |> DetachNodes
                    |> processDomainClusterManager.PostAsync

                ctx.LogInfo "Destroying process domain nodes..."

                do processDomains
                   |> Seq.map (fun processDomain -> processDomain.KillF)
                   |> Seq.iter (fun killF -> killF())

                let freedPorts =
                    processDomains
                    |> Seq.map (fun processDomain -> processDomain.Port)
                    |> Seq.choose id
                    |> Seq.toList

                ctx.LogInfo "Process domains cleared."

                reply nothing

                return { state with Db = ProcessDomainDb.Create(); PortPool = freedPorts@state.PortPool }
            with e -> 
                //TODO!!! This is an unexpected exception;; trigger system fault
                ctx.LogError e

                return state

//        | SetProcessMonitor processMonitor ->
//            return { state with ProcessMonitor = ReliableActorRef.FromRef processMonitor }

        | AllocateProcessDomainForProcess(RR ctx reply, processId, assemblyIds) ->
            try
                let existingProcessDomain =
                    Query.from state.Db.Process
                    |> Query.innerJoin state.Db.ProcessDomain <@ fun (proc, procDomain) -> proc.ProcessDomain = procDomain.Id @>
                    |> Query.where <@ fun (proc, _) -> proc.Id = processId @>
                    |> Query.toSeq
                    |> Seq.map snd
                    |> Seq.tryHead

                match existingProcessDomain with
                | None ->
                    ctx.LogInfo <| sprintf' "Allocating process domain for process %A..." processId
                        
                    //sort processDomains by number of processes
                    let candidateDomains = 
                        Query.from state.Db.ProcessDomain
                        |> Query.leftOuterJoin state.Db.Process <@ fun (processDomain, cloudProcess) -> processDomain.Id = cloudProcess.ProcessDomain @>
                        |> Query.toSeq
                        |> Seq.map (function processDomain, None -> processDomain, 0 | processDomain, _ -> processDomain, 1)
                        |> Seq.groupBy fst
                        |> Seq.map (fun (processDomainId, instances) -> processDomainId.Id, instances |> Seq.map snd |> Seq.sum)
                        |> Seq.sortBy snd

                    // eirik's note: MBrace now uses Vagrant; Vagrant's PortableAssembly type roughly consists of two things:
                    // PortableAssembly = { AssemblyImage : byte [] ; StaticInitializers : (FieldInfo * byte []) }
                    // the latter component consists of pickled data for static fields in dynamic assemblies generated by things like interpreters.
                    // This might need to be updated even in the case where the assembly itself is already loaded in the processdomain.
                    // Therefore, need to interface with AssemblyManager in every new process, even when process domain matches completely.


                    // ProcessDomain reuse determination algorithm:
                    // A dependent assembly is compatible with a process domain iff
                    //   1. No assembly with identical qualified name is already loaded in it, OR
                    //   2. If an assembly with identical qualified name is already loaded, it should either
                    //      a) be a signed assembly qualified name
                    //      b) be an assembly image of identical SHA256 hashcode.
                    //
                    // A process domain can be reused by a new cloud process iff all its dependencies are compatible with said domain.
                    // If part of the process dependencies are missing from the process domain then the AppDomain is marked for extension.
                    //

                    let selected = 
                        candidateDomains
                        |> Seq.map (fun (pdid, pidCount) -> 
                            let { NodeManager = processDomainNodeManager; LoadedAssemblies = loadedAssemblies; Port = port; ClusterProxyManager = clusterProxyManager; ClusterProxyMap = clusterProxyMap } = state.Db.ProcessDomain.DataMap.[pdid]

                            let loadedAssemblies = assemblyIds |> Seq.map (fun id -> id.FullName, id) |> Map.ofSeq
                            let assemblyCompatibility =
                                assemblyIds 
                                |> Seq.groupBy (fun id -> 
                                    match loadedAssemblies.TryFind id.FullName with 
                                    | Some id' when id.IsStrongAssembly -> Loaded
                                    | Some id' when id.ImageHash = id'.ImageHash -> Loaded
                                    | Some _ -> InCompatible
                                    | None -> NotLoaded)
                                |> Map.ofSeq

                            let lookup p = match assemblyCompatibility.TryFind p with None -> [] | Some s -> Seq.toList s

                            let extensionStatus = 
                                match lookup InCompatible with
                                | [] -> 
                                    match lookup NotLoaded with
                                    | [] -> Reuse
                                    | missing -> Extend missing
                                | _ -> Create

                            pdid, processDomainNodeManager, clusterProxyManager, clusterProxyMap, extensionStatus, pidCount - ((pidCount * reusePriority)/100), port
                        )
                        |> Seq.sortBy (fun (_, _, _, _, _, priority, _) -> priority)
                        |> Seq.map (fun (processDomainId, processDomainNodeManager, clusterProxyManager, clusterProxyMap, strategy, _, port) -> processDomainId, processDomainNodeManager, clusterProxyManager, clusterProxyMap, strategy, port)
                        |> Seq.head

                    match selected with
                    | processDomainId, processDomainNodeManager, clusterProxyManager, clusterProxyMap, Reuse, _ ->
                            
                        ctx.LogInfo <| sprintf "Process %A is allocated to reuse process domain %A" processId processDomainId

                        // eirik : add assemblyManager logic here

                        //FaultPoint
                        //-
                        let! r = processDomainNodeManager <!- fun ch -> Resolve(ch, { Definition = empDef/"common"/"assemblyManager"; InstanceId = 0 })

                        let nodeAssemblyManager = ReliableActorRef.FromRef (r :?> ActorRef<AssemblyManager>)
                        //FaultPoint
                        //-
                        do! nodeAssemblyManager <!- fun ch -> LoadAssembliesSync(ch, assemblyIds)

                        // end added assembly manager logic

                        reply <| Value (processDomainNodeManager.UnreliableRef, clusterProxyManager |> Option.map Actor.ref, clusterProxyMap)

                        return { state with
                                    Db = state.Db |> Database.insert <@ fun db -> db.Process @> { Id = processId; ProcessDomain = processDomainId }
                               }
                    | processDomainId, processDomainNodeManager, clusterProxyManager, clusterProxyMap, Extend extendedAssemblies, port ->
                        ctx.LogInfo <| sprintf' "Process %A is allocated to extended process domain %A" processId processDomainId

                        ctx.LogInfo <| sprintf' "Extending process domain %A..." processDomainId

                        //FaultPoint
                        //-
                        let! r = processDomainNodeManager <!- fun ch -> Resolve(ch, { Definition = empDef/"common"/"assemblyManager"; InstanceId = 0 })

                        let nodeAssemblyManager = ReliableActorRef.FromRef (r :?> ActorRef<AssemblyManager>)
                        //FaultPoint
                        //-
                        do! nodeAssemblyManager <!- fun ch -> LoadAssembliesSync(ch, assemblyIds) // load all assembly id's not just diff

                        reply <| Value (processDomainNodeManager.UnreliableRef, clusterProxyManager |> Option.map Actor.ref, clusterProxyMap)

                        let processDomain = state.Db.ProcessDomain.DataMap.[processDomainId]

                        return { state with
                                    Db = state.Db |> Database.insert <@ fun db -> db.ProcessDomain @> 
                                                        { processDomain with LoadedAssemblies = assemblyIds }
                                                  |> Database.insert <@ fun db -> db.Process @> { Id = processId; ProcessDomain = processDomainId }
                                }
                    | _, _, _, _, Create, _ ->
                        ctx.LogInfo <| sprintf' "A new process domain will be allocated for process %A..." processId

                        let newProcessDomainId = ProcessDomainId.NewGuid()
                            
                        let! processDomainNodeManager, portOpt, killF, state' = 
                            async {
                                try
                                    let portOpt, state' =
                                        match state.PortPool with
                                        | [] -> None, state
                                        | available::rest -> Some available, { state with PortPool = rest }

                                    let! processDomainNodeManager, killF = createProcessDomain ctx processDomainClusterManager newProcessDomainId assemblyIds portOpt
                                    return processDomainNodeManager, portOpt, killF, state'
                                with e ->
                                    ctx.Self <-- DestroyProcessDomain newProcessDomainId
                                    return raise e
                            }

                        let clusterProxyManager, proxyMap =
                            if portOpt.IsNone then
                                let proxyMap = Atom.atom Map.empty
                                let name = sprintf' "clusterProxyManager.%A" newProcessDomainId
                                Actor.bind <| Behavior.stateless (ClusterProxy.clusterProxyBehavior proxyMap)
                                |> Actor.subscribeLog (Default.actorEventHandler Default.fatalActorFailure name)
                                |> Actor.rename name
                                |> Actor.publish [Unidirectional.UTcp()]
                                |> Actor.start
                                |> Some,
                                Some proxyMap
                            else None, None

                        ctx.LogInfo <| sprintf' "Process domain %A created and allocated to process %A." newProcessDomainId processId

                        let processDomainNodeManager' = ReliableActorRef.FromRef processDomainNodeManager

                        reply <| Value (processDomainNodeManager'.UnreliableRef, clusterProxyManager |> Option.map Actor.ref, proxyMap)

                        return { state' with 
                                    Db = state.Db |> Database.insert <@ fun db -> db.ProcessDomain @>
                                                        { Id = newProcessDomainId; NodeManager = processDomainNodeManager'; LoadedAssemblies = assemblyIds ; Port = portOpt; KillF = killF; ClusterProxyManager = clusterProxyManager; ClusterProxyMap = proxyMap }
                                                  |> Database.insert <@ fun db -> db.Process @> { Id = processId; ProcessDomain = newProcessDomainId }
                               }
                | Some processDomain ->
                    
                    reply <| Value (processDomain.NodeManager.UnreliableRef, processDomain.ClusterProxyManager |> Option.map Actor.ref, processDomain.ClusterProxyMap)

                    return state
            with e -> 
                ctx.LogInfo "Process creation failed due to error."
                ctx.LogError e

                reply <| Exception e

                return state

        | DeallocateProcessDomainForProcess processId ->
            return { state with Db = state.Db |> Database.remove <@ fun db -> db.Process @> processId }
    }

