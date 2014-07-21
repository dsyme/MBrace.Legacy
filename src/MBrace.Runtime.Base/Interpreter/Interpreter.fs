namespace Nessos.MBrace.Runtime.Interpreter

    open System
    open System.Collections
    open System.Reflection

    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.CloudExpr
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Interpreter.Utils
    open Nessos.MBrace.Runtime.Interpreter.DebugInfo

    module Interpreter =

        /// <summary>
        ///     evalutes the symbolic stack sequentially until a cloud primitive is encountered.
        /// </summary>
        /// <param name="storeConfig"></param>
        /// <param name="taskConfig"></param>
        /// <param name="traceEnabled"></param>
        /// <param name="stack"></param>
        let evaluateSequential (storeConfig : StoreInfo) (taskConfig : TaskConfiguration)
                                (traceEnabled : bool) (stack : CloudExpr list) : Async<CloudExpr list> =

            let trapExc (f : obj -> CloudExpr) (value : obj) (objF : ObjFunc) : CloudExpr = 
                try f value
                with
                // Special case for handling Unquote Exceptions
                | TargetInvocationException ex
                | ex -> 
                    ValueExpr (Exc (ex, Some <| extractInfo taskConfig.Functions value objF))
            
            let rec eval (traceEnabled : bool) (stack : CloudExpr list) =
                async {

                    if traceEnabled then dumpTraceInfo taskConfig stack

                    match stack with
                    | ReturnExpr (value, t) :: rest -> 
                        return! eval traceEnabled <| ValueExpr (Obj (ObjValue value, t)) :: rest  
                    | DelayExpr (f, objF) :: DoEndDelayExpr _ :: rest | DelayExpr (f, objF) :: rest -> 
                        return! eval traceEnabled <| (trapExc (fun _ -> f ()) () objF)  :: (DoEndDelayExpr objF) :: rest
                    | BindExpr (cloudExpr, f, objF) :: rest -> 
                        return! eval traceEnabled <| cloudExpr :: DoBindExpr (f, objF) :: rest
                    | TryWithExpr (cloudExpr, f, objF) :: rest ->
                        return! eval traceEnabled <| cloudExpr :: DoTryWithExpr (f, objF) :: rest
                    | TryFinallyExpr (cloudExpr, f) :: rest ->
                        return! eval traceEnabled <| cloudExpr :: DoTryFinallyExpr f :: rest
                    | ForExpr (values, f, objF) :: rest ->
                        return! eval traceEnabled <| DoForExpr (values, 0, f, objF) :: rest
                    | WhileExpr (gruardF, cloudExpr) :: rest ->
                        return! eval traceEnabled <| DoWhileExpr (gruardF, cloudExpr) :: rest
                    | CombineExpr (firstExpr, secondExpr) :: rest ->
                        return! eval traceEnabled <| firstExpr :: DoCombineExpr secondExpr :: rest
                    | DisposableBindExpr (value, t, bindF, objF) :: rest ->
                        return! eval traceEnabled <| ValueExpr (Obj (ObjValue (value :> obj), t)) :: DoBindExpr (bindF, objF) :: DoDisposableBindExpr value :: rest
                    | OfAsyncExpr asyncContainer :: rest ->
                        let invoker =
                            {   
                                new IAsyncConsumer<Async<CloudExpr>> with
                                    member self.Invoke<'T>(asyncComputation : Async<'T>) = 
                                        async {
                                            try
                                                let! value = asyncComputation
                                                return ValueExpr (Obj (ObjValue value, typeof<'T>))
                                            with ex -> return ValueExpr (Exc (ex, None))
                                        } 
                            }

                        let! cloudExpr = asyncContainer.Unpack invoker
                        return! eval traceEnabled <| cloudExpr  :: rest

                    | NewRefByNameExpr (container, value, t) :: rest ->
                        let id = Guid.NewGuid().ToString()
                        let! exec = Async.Catch <| storeConfig.CloudRefProvider.Create(container, id, t, value)
                        match exec with
                        | Choice1Of2 result ->
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue result, result.GetType())) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    | GetRefByNameExpr (container, id, t) :: rest ->
                        let! cref = Async.Catch <| storeConfig.CloudRefProvider.GetExisting(container, id)
                        match cref with
                        | Choice1Of2 cref ->
                            if cref.Type <> t then
                                return! eval traceEnabled <| ValueExpr (Exc (new MBraceException(sprintf "CloudRef type mismatch. Internal type %s, got : %s" cref.Type.AssemblyQualifiedName t.AssemblyQualifiedName), None)) :: rest
                            else
                                return! eval traceEnabled <| ValueExpr (Obj (ObjValue cref, cref.Type)) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest
                    | GetRefsByNameExpr (container) :: rest ->
                        let! exec = Async.Catch <| storeConfig.CloudRefProvider.GetContainedRefs container
                        match exec with
                        | Choice1Of2 refs ->
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue refs, typeof<ICloudRef []>)) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    | NewMutableRefByNameExpr (container, id, value, t) :: rest ->
                        let! exec = storeConfig.MutableCloudRefProvider.Create(container, id, t, value) |> Async.Catch
                        match exec with
                        | Choice1Of2 result ->
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue result, result.GetType())) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    | ReadMutableRefExpr(mref, ty) :: rest ->
                        let! exec = Async.Catch <| mref.ReadValue()
                        match exec with
                        | Choice1Of2 result ->
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue result, mref.Type)) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    | SetMutableRefExpr(mref, value) :: rest ->
                        let! exec = Async.Catch <| mref.TryUpdate value
                        match exec with
                        | Choice1Of2 result -> 
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue result, typeof<bool>)) :: rest
                        | Choice2Of2 ex -> 
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    | ForceSetMutableRefExpr(mref, value) :: rest ->
                        let! exec = Async.Catch <| mref.ForceUpdate value
                        match exec with
                        | Choice1Of2 result -> 
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue result, typeof<bool>)) :: rest
                        | Choice2Of2 ex -> 
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest
                    
                    | GetMutableRefByNameExpr (container, id, t) :: rest ->
                        let! mref = Async.Catch <| storeConfig.MutableCloudRefProvider.GetExisting(container, id)
                        match mref with
                        | Choice1Of2 mref ->
                            if mref.Type <> t then
                                return! eval traceEnabled <| ValueExpr (Exc (new Exception(sprintf "MutableCloudRef type mismatch. Internal type %s, got : %s" mref.Type.AssemblyQualifiedName t.AssemblyQualifiedName), None)) :: rest
                            else 
                                return! eval traceEnabled <| ValueExpr (Obj (ObjValue mref, mref.Type)) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    | GetMutableRefsByNameExpr (container) :: rest ->
                        let! exec = Async.Catch <| storeConfig.MutableCloudRefProvider.GetContainedRefs(container)
                        match exec with
                        | Choice1Of2 refs ->
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue refs, typeof<IMutableCloudRef []>)) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest
                    
                    | FreeMutableRefExpr(mref) :: rest ->
                        let! exec = Async.Catch <| mref.Dispose()
                        match exec with
                        | Choice1Of2 () ->
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    | NewCloudFile(container, id, serializer) :: rest ->
                        let! exec = Async.Catch <| storeConfig.CloudFileProvider.Create(container, id, serializer)
                        match exec with
                        | Choice1Of2 file ->
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue file, typeof<ICloudFile>)) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    | GetCloudFile(container, id) :: rest ->
                        let! exec = Async.Catch <| storeConfig.CloudFileProvider.GetExisting(container, id)
                        match exec with
                        | Choice1Of2 file ->
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue file, typeof<ICloudFile>)) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    | GetCloudFiles(container) :: rest ->
                        let! exec = Async.Catch <| storeConfig.CloudFileProvider.GetContainedFiles container
                        match exec with
                        | Choice1Of2 files ->
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue files, typeof<ICloudFile []>)) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest
                    
                    | ReadCloudFile(file, deserialize, t) :: rest ->
                        let! exec = Async.Catch <| async { let! stream = file.Read() in return! deserialize stream }
                        match exec with
                        | Choice1Of2 o ->
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue o, t)) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    | LogExpr msg :: rest ->
                        try taskConfig.Logger.LogUserInfo(msg, taskConfig.TaskId) with _ -> ()
                        return! eval traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest

                    | TraceExpr cloudExpr :: rest ->
                        return! eval true <| cloudExpr :: DoEndTraceExpr :: rest

                    | NewCloudSeqByNameExpr (container, values, t) :: rest ->
                        let id = Guid.NewGuid().ToString()
                        let! exec = Async.Catch <| storeConfig.CloudSeqProvider.Create(container, id, t, values)
                        match exec with
                        | Choice1Of2 cloudSeq ->
                            return! eval traceEnabled <| (ValueExpr (Obj (ObjValue cloudSeq, typeof<ICloudSeq>))) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    | GetCloudSeqByNameExpr (container, id, t) :: rest ->
                        let! cseq = Async.Catch <| storeConfig.CloudSeqProvider.GetExisting(container, id)
                        match cseq with
                        | Choice1Of2 cseq ->
                            if t <> cseq.Type
                            then return! eval traceEnabled <| ValueExpr (Exc (new Exception(sprintf "CloudSeq type mismatch. Internal type %s, got : %s" cseq.Type.AssemblyQualifiedName t.AssemblyQualifiedName), None)) :: rest
                            else return! eval traceEnabled <| ValueExpr (Obj (ObjValue cseq, cseq.Type)) :: rest
                        | Choice2Of2 exn ->
                            return! eval traceEnabled <| ValueExpr (Exc (new NonExistentObjectStoreException(container, id), None)) :: rest

                    | GetCloudSeqsByNameExpr (container) :: rest ->
                        let! exec = Async.Catch <| storeConfig.CloudSeqProvider.GetContainedSeqs(container)
                        match exec with
                        | Choice1Of2 seqs ->
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue seqs, typeof<ICloudSeq []>)) :: rest
                        | Choice2Of2 ex ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, None)) :: rest

                    // DO Expr
                    // Monadic Bind
                    | ValueExpr (Obj (ObjValue value, _)) :: DoBindExpr (f, objF) :: rest -> 
                        return! eval traceEnabled <| trapExc f value objF :: DoEndBindExpr (value, objF) :: rest
                    | ValueExpr (Obj _) as value :: DoTryWithExpr _ :: rest -> 
                        return! eval traceEnabled <| value :: rest
                    | ValueExpr (Obj (_, _)) as value :: DoTryFinallyExpr f :: rest -> 
                        let result = trapExc (fun _ -> ValueExpr (Obj (ObjValue (f () :> obj), typeof<unit>))) () f
                        match result with
                        | ValueExpr (Obj (ObjValue (:? unit), t)) ->
                            return! eval traceEnabled <| value :: rest
                        | ValueExpr (Exc (ex, ctx)) -> 
                            return! eval traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                        | _ -> return invalidOp <| sprintf "Invalid tryFinallyF result %A" result

                    | DoForExpr (values, n, f, objF) :: rest | ValueExpr (Obj _) :: DoForExpr (values, n, f, objF) :: rest -> 
                        if n = values.Length then
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest
                        else
                            return! eval traceEnabled <| trapExc (fun _ -> f values.[n]) () f :: DoForExpr (values, n + 1,  f, objF) :: rest

                    | (DoWhileExpr (guardF, bodyExpr) as doWhileExpr) :: rest | ValueExpr (Obj _) :: (DoWhileExpr (guardF, bodyExpr) as doWhileExpr) :: rest ->
                        let result = trapExc (fun _ -> ValueExpr (Obj (ObjValue (guardF () :> obj), typeof<bool>))) () guardF
                        match result with
                        | ValueExpr (Obj (ObjValue (:? bool as value), t)) ->
                            if value then
                                return! eval traceEnabled <| bodyExpr :: doWhileExpr :: rest
                            else
                                return! eval traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest
                        | ValueExpr (Exc (ex, ctx)) ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                        | _ -> return invalidOp <| sprintf "Invalid guardF result %A" result

                    | DoCombineExpr secondExpr :: rest ->
                        return! eval traceEnabled <| secondExpr :: rest
                    | _ :: DoCombineExpr secondExpr :: rest ->
                        return! eval traceEnabled <| secondExpr :: rest

                    | ValueExpr (Obj (ObjValue value, _)) as valueExpr :: DoDisposableBindExpr cloudDisposable :: rest ->
                        let! valueExpr = 
                            async {
                                try
                                    do! cloudDisposable.Dispose()
                                    return valueExpr 
                                with ex -> return ValueExpr (Exc (ex, None)) 
                            }
                        return! eval traceEnabled <| valueExpr :: rest

                    | cloudExpr :: DoEndTraceExpr :: rest -> 
                        let traceEnabled' = rest |> List.exists (fun cloudExpr' -> match cloudExpr' with DoEndTraceExpr -> true | _ -> false)
                        return! eval traceEnabled' <| cloudExpr :: rest
                    | cloudExpr :: DoEndDelayExpr _ :: rest -> 
                        return! eval traceEnabled <| cloudExpr :: rest
                    | cloudExpr :: DoEndBindExpr _ :: rest -> 
                        return! eval traceEnabled <| cloudExpr :: rest
                    | cloudExpr :: DoEndTryWithExpr _ :: rest -> 
                        return! eval traceEnabled <| cloudExpr :: rest
                    | [ValueExpr (Obj (value, t))] -> return stack // return
                    | ValueExpr (ParallelThunks (thunkValues, elementType)) :: rest ->
                        let arrayOfResults = Array.CreateInstance(elementType, thunkValues.Length)
                        Array.Copy(thunkValues |> Array.map (fun thunkValue -> 
                                                                match thunkValue with 
                                                                | Thunk ((ValueExpr (Obj (CloudRefValue cloudRef, _)))) -> cloudRef.Value 
                                                                | Thunk ((ValueExpr (Obj (ObjValue value, _)))) -> value | _ -> throwInvalidState thunkValue),
                                    arrayOfResults, thunkValues.Length)
                        return! eval traceEnabled <| ValueExpr (Obj (ObjValue arrayOfResults, arrayOfResults.GetType())) :: rest

                    | ValueExpr (Obj (CloudRefValue (CloudRef value), t)) :: rest ->
                        return! eval traceEnabled <| ValueExpr (Obj (ObjValue value, t)) :: rest
                    | GetProcessIdExpr :: rest ->
                        return! eval traceEnabled <| ValueExpr (Obj (ObjValue taskConfig.ProcessId, typeof<int>)) :: rest
                    | GetTaskIdExpr :: rest ->
                        return! eval traceEnabled <| ValueExpr (Obj (ObjValue taskConfig.TaskId, typeof<string>)) :: rest
                    // unwind the stack
                    | ValueExpr (Exc (ex, ctx)) :: DoTryWithExpr (f, objF) :: rest ->
                        return! eval traceEnabled <| trapExc (fun _ -> f ex) () f :: DoEndTryWithExpr (ex, objF) :: rest
                    | ValueExpr (Exc (ex, ctx)) :: DoTryFinallyExpr f :: rest ->
                        let result = trapExc (fun _ -> ValueExpr (Obj (ObjValue (f () :> obj), typeof<unit>))) () f
                        match result with
                        | ValueExpr (Obj (ObjValue (:? unit as value), t)) ->
                            return! eval traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                        | ValueExpr (Exc (ex', ctx)) -> 
                            return! eval traceEnabled <| ValueExpr (Exc (ex', ctx)) :: rest
                        | _ -> return invalidOp <| sprintf "Invalid tryFinallyF result %A" result

                    | ValueExpr (Exc (ex, ctx)) as excExpr :: DoDisposableBindExpr cloudDisposable :: rest ->
                        let! valueExpr = 
                            async {
                                try
                                    do! cloudDisposable.Dispose()
                                    return excExpr 
                                with ex -> return ValueExpr (Exc (ex, None)) 
                            }
                        return! eval traceEnabled <| excExpr :: rest

                    | ValueExpr (Exc (ex, ctx)) :: _  :: rest ->
                        return! eval traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                    | [ValueExpr (Exc (ex, ctx))] -> return stack // return
                    // Primitives Expr 
                    | GetWorkerCountExpr :: rest -> return stack
                    | LocalExpr _ :: rest -> return stack 
                    | ParallelExpr (_, _) :: rest -> return stack
                    | ChoiceExpr _ :: rest -> return stack
                    | _ -> return throwInvalidState stack
                }
            eval traceEnabled stack

        /// <summary>
        ///     Evaluates a cloud expression using thread-parallel semantics.
        /// </summary>
        /// <param name="storeConfig"></param>
        /// <param name="taskConfig"></param>
        /// <param name="getCurrentTask"></param>
        /// <param name="cloner"></param>
        /// <param name="traceEnabled"></param>
        /// <param name="stack"></param>
        let evaluateLocal (storeConfig : StoreInfo) (taskConfig : TaskConfiguration)
                                (getCurrentTask : unit -> TaskId)
                                (traceEnabled : bool)  (stack : CloudExpr list) : Async<Value> =
            
            /// Serialize and deserialize a CloudExpr to force ``call by value`` semantics
            /// on parallel/choice expressions and ensure consistency between distributed execution
            /// and local/shared-memory scenarios
            let deepClone (expr : CloudExpr) = Nessos.FsPickler.FsPickler.Clone expr
            
            let rec eval (traceEnabled : bool) (stack : CloudExpr list) = 
                async {
                    let! stack' = evaluateSequential storeConfig { taskConfig with TaskId = getCurrentTask () } traceEnabled stack
                    match stack' with 
                    | [ValueExpr value] -> return value
                    | GetWorkerCountExpr :: rest -> 
                        return! eval traceEnabled <| ReturnExpr (Environment.ProcessorCount, typeof<int>) :: rest
                    | LocalExpr cloudExpr :: rest -> 
                        return! eval traceEnabled <| cloudExpr :: rest
                    | ParallelExpr (parExprs, elementType) :: rest ->
                        let parExprs = Array.map deepClone parExprs
                        let evalParExpr parExpr = async {
                            let! result = eval traceEnabled [parExpr]
                            return
                                match result with
                                | Obj(ObjValue value,t) -> Choice1Of2 value
                                | Exc _ as exc -> Choice2Of2 exc
                                | _ -> throwInvalidState result
                        }

                        let! result = parExprs |> Array.map evalParExpr |> Async.ParGeneric
                        match result with
                        | Choice1Of2 values ->
                            let arrayOfResults = Array.CreateInstance(elementType, values.Length)
                            Array.Copy(values, arrayOfResults, values.Length)
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue arrayOfResults, elementType)) :: rest

                        | Choice2Of2 exc -> return! eval traceEnabled <| ValueExpr exc :: rest

                    | ChoiceExpr (choiceExprs, elementType) :: rest ->
                        let choiceExprs = Array.map deepClone choiceExprs
                        let evalChoiceExpr choiceExpr = async {
                            let! result = eval traceEnabled [choiceExpr]
                            return
                                match result with
                                // 'None' values are represented with null in the CLR
                                | Obj (ObjValue null, t) -> Choice1Of2 ()
                                // both 'Some result' and 'exception' have the same cancellation semantics
                                | Obj _ -> Choice2Of2 result
                                | Exc _ -> Choice2Of2 result
                                | _ -> throwInvalidState result 
                        }
                        let! result = choiceExprs |> Array.map evalChoiceExpr |> Async.ParGeneric
                        match result with
                        | Choice1Of2 _ -> // all children returned 'None'
                            return! eval traceEnabled <| ValueExpr (Obj (ObjValue None, elementType)) :: rest
                        | Choice2Of2 value -> // one child returned with obj or exc : just push to stuck and carry on
                            return! eval traceEnabled <| ValueExpr value :: rest

                    | _ -> return throwInvalidState stack
                }

            eval traceEnabled stack

        

        /// <summary>
        ///     Evaluates a cloud workflow using thread-parallel execution semantics
        /// </summary>
        /// <param name="storeConfig"></param>
        /// <param name="cloner"></param>
        /// <param name="processId"></param>
        /// <param name="logger"></param>
        /// <param name="computation"></param>
        let evaluateLocalWrapped (storeConfig : StoreInfo) (logger : ICloudLogger) 
                                                        (processId : ProcessId) (computation : Cloud<'T>) =
            async {

                let getCurrentTaskId () = string System.Threading.Thread.CurrentThread.ManagedThreadId
                let taskConfig =
                    {
                        ProcessId = processId
                        TaskId = getCurrentTaskId ()
                        Logger = logger
                        Functions = []
                    }
                
                let! result = evaluateLocal storeConfig taskConfig getCurrentTaskId false [CloudExprHelpers.Unwrap computation]

                return
                    match result with
                    | Obj (ObjValue value, t) -> value :?> 'T
                    | Exc (ex, ctx) when (ex :? MBraceException) -> raise ex
                    | Exc (ex, ctx) -> raise <| new CloudException(ex, 0, ?context = ctx)
                    | _ -> throwInvalidState result
            }