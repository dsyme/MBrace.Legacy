module internal Nessos.MBrace.Runtime.Definitions.TaskLog

open System

open Nessos.Thespian
open Nessos.Thespian.Cluster

open ReplicatedState

type TaskMap = Map<TaskId, TaskId option * ActorRef<Worker> * (TaskHeader * ProcessBody)>
type State = ReplicatedState<TaskMap>
type DumpState = ReplicatedState<TaskLogEntry[]>

let private updateFromLogEntries logEntries taskMap =
    logEntries |> Seq.fold (fun taskMap'' (taskId, parentTaskId, worker, taskPayload) -> taskMap'' |> Map.add taskId (parentTaskId, worker, taskPayload)) taskMap

let logEntriesToTaskMap (logEntries: DumpState) = {
    Generation = logEntries.Generation
    State = updateFromLogEntries logEntries.State Map.empty
}

let private taskMapToLogEntries (taskMap: State) = {
    Generation = taskMap.Generation
    State = taskMap.State |> Map.toSeq |> Seq.map (fun (taskId, (parentTaskId, worker, taskPayload)) -> taskId, parentTaskId, worker, taskPayload)
                          |> Seq.toArray
}

let mapState = logEntriesToTaskMap << create

let stateMap logEntries = (logEntriesToTaskMap (create logEntries)).State

let private taskLogBehaviorInner 
                (ctx: BehaviorContext<_>)
                (state: State) 
                (msg: TaskLog) =
    async {
        let taskMap = state.State

        match msg with
        | TaskLog.Log logEntries ->
            let taskMap' = updateFromLogEntries logEntries taskMap

            return update state taskMap'

        | Unlog taskIds ->
            let taskMap' = taskIds |> Seq.fold (fun tm tid -> Map.remove tid tm) taskMap

            return update state taskMap'

        | RetrieveByWorker(R reply, workerId) ->
            let workerLogEntries = taskMap |> Map.toSeq |> Seq.map snd |> Seq.filter (fun (_, worker, _) -> worker.UUId = workerId)
                                           |> Seq.map (fun (parentTaskId, worker, ((processId, taskId), pb)) -> taskId, parentTaskId, worker, ((processId, taskId), pb))
                                           |> Seq.toArray

            reply <| Value workerLogEntries
            
            return same state

        | IsLogged(R reply, taskId) ->
            match taskMap |> Map.tryFind taskId with
            | Some _ -> reply <| Value true
            | None -> reply <| Value false

            return same state

        | Read(RR ctx reply) ->
            reply <| Value (taskMapToLogEntries state).State

            return same state

        | GetSiblingTasks(RR ctx reply, taskId) ->
            match taskMap |> Map.tryFind taskId with
            | Some(Some parentTaskId, _, _) ->
                let rec getChildren tid = seq {
                    let children = taskMap |> Map.toSeq |> Seq.choose (function childTid, (Some ptid, worker, _) when tid = ptid && childTid <> taskId -> Some(childTid, worker) | _ -> None)
                    yield! children
                    yield! children |> Seq.map fst |> Seq.collect getChildren
                }

                getChildren parentTaskId |> Seq.toArray |> Value |> reply
            | _ -> reply <| Value Array.empty

            return same state

        | TaskLog.GetCount(RR ctx reply) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            taskMap.Count |> Value |> reply

            return same state
    }

let private taskLogBehaviorStateful 
                (ctx: BehaviorContext<_>)
                (state: State) 
                (msg: Stateful<TaskLogEntry[]>) =
    async {
        match msg with
        | SetState state' ->
            return logEntriesToTaskMap state'

        | GetState(RR ctx reply) ->
            reply <| Value (taskMapToLogEntries state)

            return same state

        | GetGeneration(RR ctx reply) ->
            reply <| Value (state.Generation, ctx.Self :> ActorRef)

            return same state
    }

let taskLogBehavior (ctx: BehaviorContext<_>) 
                    (taskMap: State)
                    (msg: Choice<TaskLog, Stateful<TaskLogEntry[]>>) =
    async {
//        if ctx.Self.Name.Contains("replica") then
//            ctx.LogInfo <| sprintf' "RECEIVED LOG MESSAGE: %A" msg

        match msg with
        | Choice1Of2 msg -> return! taskLogBehaviorInner ctx taskMap msg
        | Choice2Of2 msg -> return! taskLogBehaviorStateful ctx taskMap msg
    }
