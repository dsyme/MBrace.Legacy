module internal Nessos.MBrace.Client.RuntimeProxy

    open System

    open Nessos.Thespian
    open Nessos.Thespian.ActorExtensions.RetryExtensions

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime

    /// provides basic failover logic when communicating with the runtime from the client
    let rec private runtimeProxyBehaviour (state : ClusterDeploymentInfo) (message : MBraceNode) = async {

        // get updated deployment info from any of the cluster nodes
        let rec tryGetUpdatedState (nodes : NodeDeploymentInfo list) = async {
            match nodes with
            | [] -> return None
            | next :: rest ->
                try
                    let! state' = next.Reference.PostWithReply (fun ch -> GetClusterDeploymentInfo(ch, false))
                    return Some state'
                with
                | :? MessageHandlingException
                | :? CommunicationException
                | :? TimeoutException -> return! tryGetUpdatedState rest
        }

        try
            do! state.MasterNode.Reference.PostRetriable(message, retries = 2)
            return state
        with
        | :? TimeoutException
        | :? CommunicationException as e ->

            let! result = tryGetUpdatedState <| Array.toList state.Nodes

            match result with
            | None ->
                // failover failed, intercept reply channel and forward communication exception
                match message with
                | RuntimeReply r -> r.ReplyUntyped <| Exception e
                | _ -> ()

                return state

            | Some state' -> 
                // state update, retry message submission
                return! runtimeProxyBehaviour state' message
    }

    /// initializes a failover proxy actor for given cluster deployment
    let initRuntimeProxy (state : ClusterDeploymentInfo) = 
        Behavior.stateful state runtimeProxyBehaviour
        |> Actor.bind

    /// connects to an already booted MBrace cluster
    let connect (node : ActorRef<MBraceNode>) = async {
        let! state = node.PostWithReply((fun ch -> GetClusterDeploymentInfo(ch, false)), MBraceSettings.DefaultTimeout)

        // download store activation info if required
        match StoreRegistry.TryGetStoreInfo state.StoreId with
        | Some info -> ()
        | None ->
            let! storeManager = node.PostWithReply (GetStoreManager, MBraceSettings.DefaultTimeout)
            let! info = StoreManager.downloadStore false storeManager
            return ()

        return state
    }

    /// <summary>
    ///     Boots a given collection of MBrace nodes to a runtime with given configuration.
    /// </summary>
    /// <param name="master"></param>
    /// <param name="config"></param>
    let boot (master : ActorRef<MBraceNode>, config : BootConfiguration) = async {
        // upload store activation info if specified
        match config.StoreId with
        | None -> ()
        | Some id ->
            let storeInfo = StoreRegistry.TryGetStoreInfo id |> Option.get
            let! storeMan = master.PostWithReply (GetStoreManager, MBraceSettings.DefaultTimeout)
            do! StoreManager.uploadStore storeInfo storeMan

        // boot delays mostly occuring in local runtimes;
        // create a dynamic timeout scheme based on number of local cores.
        let bootTimeout =
            match MBraceSettings.DefaultTimeout with
            | 0 -> 0
            | dt ->
                let loadFactor = max 1 (config.Nodes.Length / min Environment.ProcessorCount 4)
                min (dt * loadFactor) (max dt 120000)

        return! master.PostWithReply((fun ch -> MasterBoot(ch, config)), bootTimeout)
    }

    /// <summary>
    ///     Boots a given collection of MBrace nodes to a runtime with given configuration;
    ///     then initializes a proxy actor on the deployment configuration.
    /// </summary>
    /// <param name="nodes"></param>
    /// <param name="replicationFactor"></param>
    /// <param name="failoverFactor"></param>
    /// <param name="storeProvider"></param>
    let bootNodes (nodes : ActorRef<MBraceNode> [], replicationFactor, failoverFactor, storeProvider) = async {
        if nodes.Length < 3 then invalidArg "nodes" "insufficient amount of nodes."

        let! nodeInfo = 
            nodes 
            |> Array.map (fun n -> n.PostWithReply((fun ch -> GetNodeDeploymentInfo(ch, false)), MBraceSettings.DefaultTimeout))
            |> Async.Parallel

        match nodeInfo |> Array.tryFind (fun n -> n.State <> Idle) with
        | Some n -> mfailwithf "Node '%O' has already been booted" n.Uri
        | None -> ()

        let masterCandidates = nodeInfo |> Array.filter (fun n -> n.Permissions.HasFlag Permissions.Master)

        if masterCandidates.Length = 0 then
            invalidArg "nodes" "None of the nodes are permitted to be run in the master role."
        if masterCandidates.Length < failoverFactor then
            invalidArg "nodes" "insufficient number of master candidates to satisfy failover factor."
        elif failoverFactor > 0 && replicationFactor = 0 then
            invalidArg "nodes" "A cluster with failover should specify a replication factor of at least one."

        let master = masterCandidates.[0].Reference
        let config = 
            { 
                Nodes = nodes ; 
                ReplicationFactor = replicationFactor ; 
                FailoverFactor = failoverFactor ; 
                StoreId = storeProvider;
            }

        return! boot(master, config)
    }