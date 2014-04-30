module internal Nessos.MBrace.Runtime.Definitions.ContinuationMap

open System

open Nessos.Thespian
open Nessos.Thespian.Cluster
open Nessos.Thespian.Cluster.ReplicatedState

open Nessos.MBrace
open Nessos.MBrace.Client
open Nessos.MBrace.Core
open Nessos.MBrace.Utils

type State = ReplicatedState<Map<ThunkId, ThunkId> * Map<ThunkId, Dump>>
type DumpState = ReplicatedState<(Dump * ThunkId [])[]>

let dumpToState (dump: DumpState) = { 
    Generation = dump.Generation
    State = dump.State |> Seq.fold (fun (pThunkMap, thunkMap) (dump, thunkIds) -> let pThunkId = Guid.NewGuid().ToString() : ThunkId in thunkIds |> Seq.fold (fun pThunkMap thunkId -> pThunkMap |> Map.add thunkId pThunkId) pThunkMap, thunkMap |> Map.add pThunkId dump) (Map.empty, Map.empty)
}

let stateMap = dumpToState << create

let stateToDump (state: State) =
    let pThunkMap, thunkMap = state.State
    let dump =
        pThunkMap 
        |> Map.toSeq 
        |> Seq.groupBy snd
        |> Seq.choose (fun (pThunkId, thunkIds) -> match thunkMap |> Map.tryFind pThunkId with Some dump -> Some(dump, thunkIds |> Seq.map fst |> Seq.toArray) | _ -> None)
        |> Seq.toArray
    { Generation = state.Generation; State = dump }

    

let private continuationMapBehaviorInner (ctx: BehaviorContext<_>) (state: State) (msg: ContinuationMap) =
    async {
        let pThunkMap, thunkMap = state.State
        match msg with
        | SequentialAdd(thunkId, dump) ->
            return (pThunkMap |> Map.add thunkId thunkId, thunkMap |> Map.add thunkId dump)
                   |> update state
        | ParallelAdd(thunkIds, dump) ->
            let pThunkId = Guid.NewGuid().ToString()
            return (thunkIds |> Seq.fold (fun m thunkId -> m |> Map.add thunkId pThunkId) pThunkMap, thunkMap |> Map.add pThunkId dump)
                   |> update state
        | SequentialRemove thunkId ->
            return match pThunkMap.TryFind thunkId with
                   | Some pThunkId -> (pThunkMap |> Map.remove thunkId, thunkMap |> Map.remove pThunkId) |> update state
                   | None -> (pThunkMap, thunkMap) |> update state
        | ParallelRemove thunkId ->
            match pThunkMap.TryFind thunkId with
            | Some pThunkId ->
                let pThunkMap' = pThunkMap |> Map.remove thunkId

                return (pThunkMap',
                       if (pThunkMap' |> Map.toSeq |> Seq.map snd |> Seq.filter ((=) pThunkId) |> Seq.length) = 0 then
                           thunkMap |> Map.remove pThunkId
                       else thunkMap) |> update state
            | _ -> return (pThunkMap, thunkMap) |> update state
        | RemoveAllParallelsOf thunkId -> 
            match pThunkMap.TryFind thunkId with
            | Some pThunkId ->
                //let pThunkMap' = pThunkMap |> Map.toSeq |> Seq.filter (fun (pThunkId, _) -> thunkId <> pThunkId) |> Map.ofSeq
                let pThunkMap' = pThunkMap |> Map.toSeq |> Seq.filter (fun (_, pThunkId') -> pThunkId' <> pThunkId) |> Map.ofSeq

                return (pThunkMap', thunkMap |> Map.remove pThunkId) |> update state
            | _ -> return (pThunkMap, thunkMap) |> update state
        | ContinuationMap.Get(R(reply), thunkId) ->
            match pThunkMap.TryFind thunkId with
            | Some pThunkId ->
                thunkMap |> Map.tryFind pThunkId |> Value |> reply
            | None -> reply <| Value None
            return same state
        | ContinuationMap.GetAll(R(reply)) ->
            pThunkMap 
            |> Map.toSeq 
            |> Seq.groupBy snd
            |> Seq.choose (fun (pThunkId, thunkIds) -> match thunkMap |> Map.tryFind pThunkId with Some dump -> Some(dump, thunkIds |> Seq.map fst |> Seq.toArray) | _ -> None)
            |> Seq.toArray
            |> Value
            |> reply
            
            return same state
        | ContinuationMap.Update(thunkId, dump) ->
            return (pThunkMap, match pThunkMap.TryFind thunkId with
                                | Some pThunkId -> thunkMap |> Map.add pThunkId dump
                                | _ -> 
                                    ctx.LogInfo "UPDATING UNKOWN CONT!!!"
                                    ctx.LogInfo "CORRUPT LOG!!! CORRUPT LOG!!!"
                                    thunkMap) |> update state
        | ContinuationMap.UpdateParallelThunkValue(thunkId, index, value) ->
            try
                //The pattern match here should never fail.
                //The message is sent only for logging the in place update of parallel thunk values
                match thunkMap.[pThunkMap.[thunkId]] with
                | Dump ((ValueExpr (ParallelThunks (thunkValues, _))) :: _)
                | Dump ((ValueExpr (ChoiceThunks(thunkValues, _))) :: _) ->
                    thunkValues.[index] <- Thunk value
                | _ -> failwith "ContinuationMap: Could not reckognize UpdateParallelThunkValue Dump."
            with e ->
                ctx.LogError e
                //TODO!!! kill the process that has caused this error

            return same state
    }

let private continuationMapBehaviorStateful (ctx: BehaviorContext<_>) (state: State) (msg: Stateful<(Dump * ThunkId [])[]>) =
    async {
        match msg with
        | SetState state ->
            return dumpToState state

        | GetState(RR ctx reply) ->
            reply <| Value (stateToDump state)

            return same state

        | GetGeneration(RR ctx reply) ->
            reply <| Value (state.Generation, ctx.Self :> ActorRef)

            return same state
    }

let continuationMapBehavior (ctx: BehaviorContext<_>) (state: State) (msg: Choice<ContinuationMap, Stateful<ContinuationMapDump>>) =
    async {
        match msg with
        | Choice1Of2 payload -> return! continuationMapBehaviorInner ctx state payload
        | Choice2Of2 payload -> return! continuationMapBehaviorStateful ctx state payload
    }
