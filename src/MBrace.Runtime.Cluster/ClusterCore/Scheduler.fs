module internal Nessos.MBrace.Runtime.Definitions.Scheduler

open System

open Nessos.Thespian
open Nessos.Thespian.Cluster

open Nessos.MBrace
open Nessos.MBrace.Runtime
open Nessos.MBrace.Utils
open Nessos.MBrace.Core

//type alias to prevent conflicts with non-cluster types
type private ProcessMonitorDb = CommonTypes.ProcessMonitorDb

type State = {
    ContinuationMap: ActorRef<AsyncReplicated<ContinuationMap, ContinuationMapDump>>
    TaskManager: ActorRef<TaskManager>
} with static member New continuationMap = { ContinuationMap = continuationMap; TaskManager = ActorRef.empty() }


let schedulerBehavior (processMonitor: ActorRef<Replicated<ProcessMonitor, ProcessMonitorDb>>)
                      (ctx: BehaviorContext<Scheduler>)
                      (state: State)
                      (msg: Scheduler) = 

    /// dependency injection! fix this!
    let coreConfig = Store.StoreRegistry.DefaultStoreInfo.Primitives

    let newRef (processId : int) (value : 'T) = 
        coreConfig.CloudRefProvider.Create<'T>("temp" + (string processId), Guid.NewGuid().ToString(), value)

    let taskManager = state.TaskManager
    let continuationMap = state.ContinuationMap
    async {
        match msg with
        | SetTaskManager taskManager ->
            return { state with TaskManager = taskManager }

        | NewProcess(RR ctx reply as confirmationChannel, processId, exprImage) ->
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                ctx.LogInfo <| sprintf "NewProcess %A" processId

                let package =
                    try
                        let pkg = Serialization.Deserialize<CloudComputation> exprImage
                        Choice1Of2 pkg

                    with ex -> Choice2Of2 ex

                match package with
                | Choice1Of2 package ->
                    ctx.LogInfo "Scheduler: Starting process..."

                    //the task manager will confirm the creation of the root task
                    let! cref = newRef processId package.Functions
                    taskManager <-- CreateRootTask(confirmationChannel, processId, ProcessBody (package.ReturnType, [Guid.NewGuid().ToString()], cref, Dump [package.GetCloudExpr()]))
                | Choice2Of2 e -> 
                    reply nothing
                         
                    ctx.LogInfo "Scheduler: Deserialization error. Process not started."

                    do! processMonitor <!- fun ch -> Replicated(ch, Choice1Of2 <| CompleteProcess(processId, InitError e))
            with e ->
                ctx.LogError e
                reply (Exception e)

            return state

        | Scheduler.TaskResult((processId, taskId) as taskHeader, TaskSuccess(ProcessBody(resultType, ((thunkId :: restOfThunkIdsStack) as thunkIdsStack), functions, resultDump) as pb)) ->
            match resultDump with
            | Dump ([((ValueExpr (Obj (_))) | (ValueExpr (Exc (_)))) as value]) -> 
                let! continuation = continuationMap <!- fun ch -> AsyncSingular(Choice1Of2 <| Get(ch, thunkId))

                match continuation with
                //Choice thunks; on a Some result return, else wait for all Nones and return None
                | Some ((Dump ((ValueExpr (ChoiceThunks (thunkValues, elementType))) :: stack)) as dumpCell) ->
                    let i = thunkValues |> Array.tryFindIndex (fun thunkValue -> match thunkValue with ThunkId thunkId' when thunkId = thunkId' -> true | _ -> false)

                    let! isValid = taskManager <!- fun ch -> IsValidTask(ch, taskId)
                    if isValid then
                        match value with
                        | (ValueExpr (Exc _)) -> 
                            let childProcessBody = ProcessBody(resultType, restOfThunkIdsStack, functions, Dump (value :: stack))
                            do! taskManager <!- fun ch -> CancelSiblingTasks(ch, taskId)
                            do! taskManager <!- fun ch -> CreateTasks(ch, taskHeader, [childProcessBody])
                        | (ValueExpr (Obj (ObjValue null, _))) -> //This is a check for a None value; Nones are represented as nulls, therefore this checks for an (utyped) None
                            match i with
                            | Some index ->
                                // in place update 
                                thunkValues.[index] <- Thunk value                            
                                continuationMap <-- AsyncReplicated(Choice1Of2 <| ContinuationMap.UpdateParallelThunkValue(thunkId, index, value))
                                //ON FAULT
                                //\forall x \in thunkValues. x = (Thunk _)
                                //Thus there may be no index for the result thunkId to update in the retry.
                                //When there is no index we go straigt to the completion check.
                            | _ -> ()

                            // completion check
                            if thunkValues |> Array.forall (fun thunkValue -> match thunkValue with Thunk _ -> true | _ -> false) then
                                // add result on the stack and continue
                                let childProcessBody = ProcessBody(resultType, restOfThunkIdsStack, functions, Dump (value :: stack))
                                do! taskManager <!- fun ch -> CreateTasks(ch, taskHeader, [childProcessBody])
                            else
                                taskManager <-- LeafTaskComplete taskId

                            continuationMap <-- AsyncReplicated(Choice1Of2 <| ParallelRemove thunkId)

                        | (ValueExpr (Obj (_))) -> //We got the first Some result. Thus
                            let childProcessBody = ProcessBody(resultType, restOfThunkIdsStack, functions, Dump (value :: stack))

                            do! taskManager <!- fun ch -> CancelSiblingTasks(ch, taskId)

                            do! taskManager <!- fun ch -> CreateTasks(ch, taskHeader, [childProcessBody])
                        | _ -> failwith "SEVERE SYSTEM ERROR"
                    else
                        taskManager <-- LeafTaskComplete taskId
                        continuationMap <-- AsyncReplicated(Choice1Of2 <| ParallelRemove thunkId)

                // Parallel thunks on the stack waiting
                | Some ((Dump ((ValueExpr (ParallelThunks (thunkValues, elementType))) :: stack) as dumpCell)) ->
                    let i = thunkValues |> Array.tryFindIndex (fun thunkValue -> match thunkValue with ThunkId thunkId' when thunkId = thunkId' -> true | _ -> false)

                    let! isValid = taskManager <!- fun ch -> IsValidTask(ch, taskId)
                    if isValid then
                        match value with
                        | (ValueExpr (Exc _)) -> 
                            //task produced exception; cancel siblings and continue
                            let childProcessBody = ProcessBody(resultType, restOfThunkIdsStack, functions, Dump (value :: stack))
                            do! taskManager <!- fun ch -> CancelSiblingTasks(ch, taskId)
                            do! taskManager <!- fun ch -> CreateTasks(ch, taskHeader, [childProcessBody])
                        | _ ->
                            match i with
                            | Some index ->
                                // in place update 
                                thunkValues.[index] <- Thunk value
                                //and update the continuation map
                                //--
                                //FAST UPDATE: Send the value and the in place update is performed in the map
                                continuationMap <-- AsyncReplicated(Choice1Of2 <| ContinuationMap.UpdateParallelThunkValue(thunkId, index, value))

                                //ON FAULT
                                //\forall x \in thunkValues. x = (Thunk _)
                                //Thus there may be no index for the result thunkId to update in the retry.
                                //When there is no index we go straight to the completion check.
                            | _ -> ()

                        // completion check
                        if thunkValues |> Array.forall (fun thunkValue -> match thunkValue with Thunk _ -> true | _ -> false) then
                            ctx.LogInfo <| sprintf' "PARALLEL THUNK CONTINUATION - COMPLETE (%A, %A)" processId taskId
                            let result = ParallelThunks (thunkValues, elementType)

                            // add result on the stack and continue
                            let childProcessBody = ProcessBody(resultType, restOfThunkIdsStack, functions, Dump ((ValueExpr result) :: stack))
                            do! taskManager <!- fun ch -> CreateTasks(ch, taskHeader, [childProcessBody])
                        else
                            ctx.LogInfo <| sprintf' "PARALLEL THUNK CONTINUATION - INCOMPLETE (%A, %A)" processId taskId

                            taskManager <-- LeafTaskComplete taskId
                            
                        continuationMap <-- AsyncReplicated(Choice1Of2 <| ParallelRemove thunkId)
                    else
                        taskManager <-- LeafTaskComplete taskId
                        continuationMap <-- AsyncReplicated(Choice1Of2 <| ParallelRemove thunkId)
                | None -> 
                    //The computation is finished when there is a task logged by the taskId
                    //If there is no task logged by that id, then it is the result of a cancelled task
                    //that managed to get to the actor's queue before it was cancelled.
                    let! isValid = taskManager <!- fun ch -> IsValidTask(ch, taskId)

                    if isValid then
                        match pb with 
                        | ProcessBody(resultType, [_], _, Dump([(ValueExpr (Obj (CloudRefValue (CloudRef value), valueType)))]))
                        | ProcessBody(resultType, [_], _, Dump([(ValueExpr (Obj (ObjValue value, valueType)))])) ->
                                
                            if resultType = valueType || resultType.IsInstanceOfType(value) then
                                ctx.LogInfo "Completing process with value result..."
                                do! processMonitor <!- fun ch -> Replicated(ch, Choice1Of2 <| CompleteProcess(processId, ValueResult(box value) |> ProcessResultImage.OfResult))
                            else
                                ctx.LogInfo "Completing process with invalid result type..."
                                let e = new SystemException("Failed to recognise result. Severe system error. Contact M-Brace support.")
                                do! processMonitor <!- fun ch -> Replicated(ch, Choice1Of2 <| CompleteProcess(processId, e :> exn |> Fault))

                        | ProcessBody(_, [_], _, Dump([(ValueExpr (Exc (exn, context)))])) ->
                            ctx.LogInfo "Completing process with exception result..."
                            do! processMonitor <!- fun ch -> Replicated(ch, Choice1Of2 <| CompleteProcess(processId, ExceptionResult (CloudException (exn, processId, ?context = context) :> exn, None) |> ProcessResultImage.OfResult))
                        | _ ->
                                
                            let msg = "Failed to recognise result. Severe system error. Contact M-Brace support."
                            do! processMonitor <!- fun ch -> Replicated(ch, Choice1Of2 <| CompleteProcess(processId, new SystemException(msg) :> exn |> Fault))

                        do! taskManager <!- fun ch -> FinalTaskComplete(ch, taskId)

                        processMonitor <-- Singular(Choice1Of2 <| ProcessMonitor.DestroyProcess processId)
                    else
                        ctx.LogInfo "Received unexpected result (probably due to cancellation). Ignoring..."

                | value -> 
                    ctx.LogEvent(Nessos.Thespian.LogLevel.Error, "Scheduler: Unreckognized result. Terminating...")
                        
                    do! processMonitor <!- fun ch -> Replicated(ch, Choice1Of2 <| CompleteProcess(processId, new SystemException("Scheduler state corrupted. Severe system error. Contact M-Brace support.") :> exn |> Fault))

                    processMonitor <-- Singular(Choice1Of2 <| ProcessMonitor.DestroyProcess processId)
                
            | Dump ((ValueExpr (ParallelValue (cloudExprRefs, elementType))) :: stack) ->
                //logger.LogInfo <| sprintf "PARALLEL THUNKS pid: %A, jid: %A" processId jobId
                let thunkIds = cloudExprRefs |> Array.map (fun _ -> Guid.NewGuid().ToString())

                // update the ContinuationsMap
                let continuationDump = Dump ((ValueExpr (ParallelThunks(thunkIds |> Array.map ThunkValue.ThunkId, elementType))) :: stack)
                        
                continuationMap <-- AsyncReplicated(Choice1Of2 <| ParallelAdd(thunkIds, continuationDump))

                // post the parallel jobs
                let childProcessBodies = cloudExprRefs 
                                         |> Seq.zip thunkIds 
                                         |> Seq.map (fun (thunkId', cloudExprRef) -> ProcessBody(resultType,
                                                                                                 thunkId' :: thunkIdsStack, 
                                                                                                 functions, 
                                                                                                 Dump ([cloudExprRef])))
                                         |> Seq.toList

                do! taskManager <!- fun ch -> CreateTasks(ch, taskHeader, childProcessBodies)
            | Dump ((ValueExpr (ChoiceValue (cloudExprRefs, elementType))) :: stack) ->
                //logger.LogInfo <| sprintf "PARALLEL THUNKS pid: %A, jid: %A" processId jobId
                let thunkIds = cloudExprRefs|> Array.map (fun _ -> Guid.NewGuid().ToString())

                // update the ContinuationsMap
                let continuationDump = Dump ( (ValueExpr (ChoiceThunks (thunkIds |> Array.map ThunkValue.ThunkId, elementType))) :: stack)

                continuationMap <-- AsyncReplicated(Choice1Of2 <| ParallelAdd(thunkIds, continuationDump))

                // post the parallel jobs
                let childProcessBodies = cloudExprRefs 
                                                |> Seq.zip thunkIds 
                                                |> Seq.map (fun (thunkId', cloudExprRef) -> ProcessBody(resultType,
                                                                                                        thunkId' :: thunkIdsStack, 
                                                                                                        functions, 
                                                                                                        Dump ([cloudExprRef])))
                                                |> Seq.toList

                do! taskManager <!- fun ch -> CreateTasks(ch, taskHeader, childProcessBodies)
            | Dump (GetWorkerCountExpr :: stack) ->
                let! workerCount = taskManager <!- TaskManager.GetWorkerCount
                        
                let processBody = ProcessBody(resultType, thunkIdsStack, functions, Dump ((ValueExpr (Obj (ObjValue workerCount, typeof<int>))) :: stack))
                do! taskManager <!- fun ch -> CreateTasks(ch, taskHeader, [processBody])
            | value -> 
                do! processMonitor <!- fun ch -> Replicated(ch, Choice1Of2 <| CompleteProcess(processId, new SystemException("Scheduler state corrupted. Severe system error. Contact M-Brace support.") :> exn |> Fault))
                throwInvalidState value

            return state

        | Scheduler.TaskResult((processId, taskId), TaskFailure taskFailureException) ->
            ctx.LogInfo <| sprintf' "(%A, %A) has failed. Terminating process." processId taskId
            do! processMonitor <!- fun ch -> Replicated(ch, Choice1Of2 <| CompleteProcess(processId, Fault taskFailureException)) //if this message send fails, then there is a total fail

            processMonitor <-- Singular(Choice1Of2 <| ProcessMonitor.DestroyProcess processId)

            return state

        | _ -> return failwith "Invalid Request"
    }

