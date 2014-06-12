﻿module internal Nessos.MBrace.Client.RuntimeProxy

    open System

    open Nessos.Thespian
    open Nessos.Thespian.ActorExtensions.RetryExtensions

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime

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

    let initRuntimeProxy (state : ClusterDeploymentInfo) = 
        Behavior.stateful state runtimeProxyBehaviour
        |> Actor.bind

    let connect (node : ActorRef<MBraceNode>) = async {
        let! state = node.PostWithReplyRetriable((fun ch -> GetClusterDeploymentInfo(ch, false)), 2)
        return initRuntimeProxy state
    }

    let boot (master : ActorRef<MBraceNode>, config : BootConfiguration) = async {
        let! state = master.PostWithReplyRetriable((fun ch -> MasterBoot(ch, config)), 2)
        return initRuntimeProxy state
    }

    let bootNodes (nodes : ActorRef<MBraceNode> [], replicationFactor, failoverFactor) = async {
        if nodes.Length < 3 then invalidArg "nodes" "insufficient amount of nodes."

        let! nodeInfo = nodes |> Array.map (fun n -> n <!- fun ch -> GetNodeDeploymentInfo(ch, false)) |> Async.Parallel

        match nodeInfo |> Array.tryFind (fun n -> n.State <> Idle) with
        | Some n -> mfailwithf "Node '%O' has already been booted" n.Uri
        | None -> ()

        let masterCandidates = nodeInfo |> Array.filter (fun n -> n.Permissions.HasFlag Permissions.Master)

        if masterCandidates.Length = 0 then
            invalidArg "none" "None of the nodes are permitted to be run in the master role."
        if masterCandidates.Length < failoverFactor then
            invalidArg "nodes" "insufficient number of master candidates to satisfy failover factor."
        elif failoverFactor > 0 && replicationFactor = 0 then
            invalidArg "nodes" "A cluster with failover should specify a replication factor of at least one."

        let master = masterCandidates.[0].Reference
        let config = { Nodes = nodes ; ReplicationFactor = replicationFactor ; FailoverFactor = failoverFactor }

        return! boot(master, config)
    }