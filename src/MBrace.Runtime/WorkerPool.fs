module internal Nessos.MBrace.Runtime.Definitions.WorkerPool

open Nessos.Thespian
open Nessos.Thespian.Cluster
open Nessos.Thespian.Cluster.BehaviorExtensions

open Nessos.MBrace.Runtime
open Nessos.MBrace.Utils

type State = {
    Pool: Set<ActorRef<Worker>>
// not required any more
//    IdMap: Map<ActorUUID, ActorUUID>
} with static member Empty = Set.empty

let workerPoolBehavior (processId: ProcessId)
                       (selectF: Set<ActorRef<Worker>> -> ActorRef<Worker> option)
                       (ctx: BehaviorContext<_>)
                       (state: State)
                       (msg: WorkerPool) =
    let selectMany (howMany: int) =
        let workers = [| for i in 1..howMany -> selectF state.Pool |]
        if workers |> Array.forall (function Some _ -> true | _ -> false) then
            workers |> Array.map (function Some w -> w | _ -> failwith "Filtering failure.")
                    |> Some
        else None

    async {
        match msg with
        | AddWorker worker ->
            return { state with Pool = state.Pool |> Set.add worker }

        | RemoveWorker worker ->
            return { state with Pool = state.Pool |> Set.remove worker }

        | Select(RR ctx reply) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                //Throws ;; nothing
                selectF state.Pool
                |> Value
                |> reply
            with e -> 
                ctx.LogError e
                reply (Exception e)

            return state

        | SelectMany(RR ctx reply, howMany) ->
            try
                reply << Value << selectMany <| howMany
            with e -> ctx.LogError e

            return state

        | GetAvailableWorkers(RR ctx reply) ->
            try
                state.Pool |> Set.toArray |> Value |> reply
            with e -> ctx.LogError e

            return state

        | GetAvailableWorkerCount(RR ctx reply) ->
            try
                reply (Value state.Pool.Count)
            with e -> ctx.LogError e

            return state

//        | MapUUID(RR ctx reply, uuid) ->
//            try
//                ctx.LogInfo <| sprintf' "IdMap = %A" state.IdMap
//
//                state.IdMap |> Map.find uuid |> Value |> reply
//            with :? System.Collections.Generic.KeyNotFoundException as e ->
//                    reply <| Exception e
//                | e -> ctx.LogError e
//
//            return state
    }

