module Nessos.MBrace.Runtime.Definitions.ActivationStrategy

open Nessos.Thespian
open Nessos.Thespian.Cluster
open Nessos.Thespian.ImemDb

open Nessos.MBrace.Utils

type internal ProcessActivationStrategy() =
    interface IActivationStrategy with
        override __.GetActivationNodesAsync(clusterState: ClusterState, instanceId: int, _) = async { 
            let candidates =
                Query.from clusterState.Db.ClusterActivation
                |> Query.where <@ fun activation -> activation.ActivationReference.Definition = empDef/"process"/"scheduler"/"scheduler" 
                                                    && 
                                                    activation.ActivationReference.Definition <> empDef/"process"/"state"/"continuationsMap"/"local"/"continuationsMap"
                                                    &&
                                                    activation.ActivationReference.Definition <> empDef/"process"/"state"/"taskLog"/"local"/"taskLog"
                                                    &&
                                                    activation.ActivationReference.InstanceId <> instanceId @>
                |> Query.toSeq
                |> Seq.groupBy (fun activation -> activation.NodeManager)
                |> Seq.sortBy (fun (nodeManager, schedulerNodes) -> schedulerNodes |> Seq.length)
                |> Seq.map (fun (_, schedulerNodes) -> schedulerNodes |> Seq.tryHead)
                |> Seq.choose id
                |> Seq.map (fun schedulerNode -> schedulerNode.NodeManager)
                |> Seq.truncate 1

            if Seq.isEmpty candidates then
                let candidates' =
                    Query.from clusterState.Db.ClusterActivation
                    |> Query.where <@ fun activation -> activation.ActivationReference.Definition <> empDef/"process"/"state"/"continuationsMap"/"local"/"continuationsMap"
                                                        &&
                                                        activation.ActivationReference.Definition <> empDef/"process"/"state"/"taskLog"/"local"/"taskLog"
                                                        &&
                                                        activation.ActivationReference.InstanceId <> instanceId @>
                    |> Query.toSeq
                    |> Seq.map (fun activation -> activation.NodeManager)
                    |> Seq.truncate 1

                if Seq.isEmpty candidates' then
                    return 
                        Query.from clusterState.Db.ClusterNode
                        |> Query.toSeq
                        |> Seq.map (fun clusterNode -> clusterNode.NodeManager)
                        |> Seq.truncate 1
                        |> Seq.toList
                else return Seq.toList candidates'
            else return Seq.toList candidates
        }

type internal StateReactivationStrategy() =
    interface IActivationStrategy with
        override __.GetActivationNodesAsync(clusterState: ClusterState, instanceId: int, definition: Definition) = async {
            let schedulerNode =
                Query.from clusterState.Db.ClusterActiveDefinition
                |> Query.where <@ fun cad -> cad.ActivationReference.Definition = empDef/"process"/"scheduler"/"scheduler"
                                             && cad.ActivationReference.InstanceId = instanceId @>
                |> Query.toSeq
                |> Seq.map (fun cad -> cad.NodeManager)
                |> Seq.tryHead

            let nonEmptyNodes =
                let existingDefs =
                    Query.from clusterState.Db.ClusterActiveDefinition
                    |> Query.where <@ fun cad -> cad.ActivationReference.Definition = definition.Path
                                                 && cad.ActivationReference.InstanceId = instanceId @>
                    |> Query.toSeq
                    |> Seq.map (fun cad -> cad.NodeManager)
                    |> Set.ofSeq

                Query.from clusterState.Db.ClusterNode
                |> Query.where <@ fun cn -> not (existingDefs |> Set.contains cn.NodeManager) 
                                            && match schedulerNode with Some node -> cn.NodeManager <> node | _ -> true @>
                |> Query.toSeq
                |> Seq.map (fun cn -> cn.NodeManager)
                |> Seq.distinct

            let emptyNodes =
                Query.from clusterState.Db.ClusterNode
                |> Query.leftOuterJoin clusterState.Db.ClusterActiveDefinition <@ fun (cn, cad) -> cn.NodeManager = cad.NodeManager @>
                |> Query.where <@ fun (cn, cad) -> cad.IsNone && match schedulerNode with Some node -> cn.NodeManager <> node | _ -> true @>
                |> Query.toSeq
                |> Seq.map (fun (cn, _) -> cn.NodeManager)
                |> Seq.distinct

            return 
                Seq.append nonEmptyNodes emptyNodes
                |> Seq.truncate 1
                |> Seq.toList
        }

let processStrategy = new ProcessActivationStrategy() :> IActivationStrategy

let stateReActivationStrategy = new StateReactivationStrategy() :> IActivationStrategy
