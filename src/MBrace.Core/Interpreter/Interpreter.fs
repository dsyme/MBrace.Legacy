namespace Nessos.MBrace.Core

    open System
    open System.Collections
    open System.Reflection

    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns

    open Nessos.MBrace
    open Nessos.MBrace.Core.Utils
    open Nessos.MBrace.Core.DumpExtractors

    module Interpreter =

        /// raise exception with given object
        let inline internal throwInvalidState (value : 'T) = failwithf "invalid state %A" value

        /// extract the untyped cloud tree
        let extractCloudExpr (cloud : Cloud) : CloudExpr = cloud.CloudExpr

        /// <summary>
        ///     evalutes the symbolic stack sequentially until a cloud primitive is encountered.
        /// </summary>
        /// <param name="config"></param>
        /// <param name="logger"></param>
        /// <param name="functions"></param>
        /// <param name="traceEnabled"></param>
        /// <param name="stack"></param>
        let evaluateSequential (config : CoreConfiguration) (logger : ICloudLogger) (functions : FunctionInfo list) 
                                (traceEnabled : bool) (stack : CloudExpr list) : Async<CloudExpr list> =
            
            let rec run (traceEnabled : bool) (stack : CloudExpr list) =

                let trapExc (f : obj -> CloudExpr) (value : obj) (objF : ObjFunc) : CloudExpr = 
                    try
                        f value
                    with 
                        // Special case for handling Unquote Exceptions
                       :? TargetInvocationException as ex ->
                           match ex.InnerException with
                           | :? TargetInvocationException as ex ->
                               ValueExpr (Exc (ex.InnerException, Some <| extractInfo functions value objF))
                           | ex -> ValueExpr (Exc (ex, Some <| extractInfo functions value objF))
                     | ex ->                         
                        ValueExpr (Exc (ex, Some <| extractInfo functions value objF))

                async {
                    if traceEnabled then dumpTraceInfo functions logger stack

                    match stack with
                    | ReturnExpr (value, t) :: rest -> 
                        return! run traceEnabled <| ValueExpr (Obj (ObjValue value, t)) :: rest  
                    | DelayExpr (f, objF) :: DoEndDelayExpr _ :: rest | DelayExpr (f, objF) :: rest -> 
                        return! run traceEnabled <| (trapExc (fun _ -> f ()) () objF)  :: (DoEndDelayExpr objF) :: rest
                    | BindExpr (cloudExpr, f, objF) :: rest -> 
                        return! run traceEnabled <| cloudExpr :: DoBindExpr (f, objF) :: rest
                    | TryWithExpr (cloudExpr, f, objF) :: rest ->
                        return! run traceEnabled <| cloudExpr :: DoTryWithExpr (f, objF) :: rest
                    | TryFinallyExpr (cloudExpr, f) :: rest ->
                        return! run traceEnabled <| cloudExpr :: DoTryFinallyExpr f :: rest
                    | ForExpr (values, f, objF) :: rest ->
                        return! run traceEnabled <| DoForExpr (values, 0, f, objF) :: rest
                    | WhileExpr (gruardF, cloudExpr) :: rest ->
                        return! run traceEnabled <| DoWhileExpr (gruardF, cloudExpr) :: rest
                    | CombineExpr (firstExpr, secondExpr) :: rest ->
                        return! run traceEnabled <| firstExpr :: DoCombineExpr secondExpr :: rest
                    | DisposableBindExpr (value, t, bindF, objF) :: rest ->
                        return! run traceEnabled <| ValueExpr (Obj (ObjValue (value :> obj), t)) :: DoBindExpr (bindF, objF) :: DoDisposableBindExpr value :: rest
                    | OfAsyncExpr cloudAsync :: rest ->
                        let asyncCompRef : Async<CloudExpr> ref = ref Unchecked.defaultof<_>
                        let polyMorphicMethodAsync =
                                {   new IPolyMorphicMethodAsync with
                                        member self.Invoke<'T>(asyncComputation : Async<'T>) = 
                                            asyncCompRef :=    
                                                async {
                                                    try
                                                        let! value = asyncComputation
                                                        return ValueExpr (Obj (ObjValue value, typeof<'T>))
                                                    with ex -> return ValueExpr (Exc (ex, None))
                                                } 
                                }
                        
                        cloudAsync.UnPack polyMorphicMethodAsync
                        let! cloudExpr = asyncCompRef.Value
                        return! run traceEnabled <| cloudExpr  :: rest

                    | NewRefByNameExpr (container, value, t) :: rest ->
                        let id = Guid.NewGuid().ToString()
                        let! exec = Async.Catch <| config.CloudRefProvider.Create(container, id, t, value)
                        match exec with
                        | Choice1Of2 result ->
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue result, result.GetType())) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot create Container: %s, Name: %s" container id, ex), None)) :: rest

                    | GetRefByNameExpr (container, id, t) :: rest ->
                        let! cref = Async.Catch <| config.CloudRefProvider.GetExisting(container, id)
                        match cref with
                        | Choice1Of2 cref ->
                            if cref.Type <> t then
                                return! run traceEnabled <| ValueExpr (Exc (new MBraceException(sprintf "CloudRef type mismatch. Internal type %s, got : %s" cref.Type.AssemblyQualifiedName t.AssemblyQualifiedName), None)) :: rest
                            else
                                return! run traceEnabled <| ValueExpr (Obj (ObjValue cref, cref.Type)) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot find CloudRef with Container: %s, Name: %s" container id, ex), None)) :: rest
                    | GetRefsByNameExpr (container) :: rest ->
                        let! exec = Async.Catch <| config.CloudRefProvider.GetContainedRefs container
                        match exec with
                        | Choice1Of2 refs ->
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue refs, typeof<ICloudRef []>)) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot access Container: %s" container, ex), None)) :: rest

                    | NewMutableRefByNameExpr (container, id, value, t) :: rest ->
                        let! exec = config.MutableCloudRefProvider.Create(container, id, t, value) |> Async.Catch
                        match exec with
                        | Choice1Of2 result ->
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue result, result.GetType())) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot create Container: %s, Name: %s" container id, ex), None)) :: rest
                    | ReadMutableRefExpr(mref, ty) :: rest ->
                        let! exec = Async.Catch <| mref.ReadValue()
                        match exec with
                        | Choice1Of2 result ->
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue result, mref.Type)) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot locate Container: %s, Name: %s" mref.Container mref.Name, ex), None)) :: rest
                    | SetMutableRefExpr(mref, value) :: rest ->
                        let! exec = Async.Catch <| mref.TryUpdate value
                        match exec with
                        | Choice1Of2 result -> 
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue result, typeof<bool>)) :: rest
                        | Choice2Of2 ex -> 
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot update Container: %s, Name: %s" mref.Container mref.Name, ex), None)) :: rest

                    | ForceSetMutableRefExpr(mref, value) :: rest ->
                        let! exec = Async.Catch <| mref.ForceUpdate value
                        match exec with
                        | Choice1Of2 result -> 
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue result, typeof<bool>)) :: rest
                        | Choice2Of2 ex -> 
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot update Container: %s, Name: %s" mref.Container mref.Name, ex), None)) :: rest
                    
                    | GetMutableRefByNameExpr (container, id, t) :: rest ->
                        let! mref = Async.Catch <| config.MutableCloudRefProvider.GetExisting(container, id)
                        match mref with
                        | Choice1Of2 mref ->
                            if mref.Type <> t then
                                return! run traceEnabled <| ValueExpr (Exc (new Exception(sprintf "MutableCloudRef type mismatch. Internal type %s, got : %s" mref.Type.AssemblyQualifiedName t.AssemblyQualifiedName), None)) :: rest
                            else 
                                return! run traceEnabled <| ValueExpr (Obj (ObjValue mref, mref.Type)) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot read MutableCloudref with Container: %s, Name: %s" container id, ex), None)) :: rest
                    | GetMutableRefsByNameExpr (container) :: rest ->
                        let! exec = Async.Catch <| config.MutableCloudRefProvider.GetContainedRefs(container)
                        match exec with
                        | Choice1Of2 refs ->
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue refs, typeof<IMutableCloudRef []>)) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot access Container: %s" container, ex), None)) :: rest
                    
                    | FreeMutableRefExpr(mref) :: rest ->
                        let! exec = Async.Catch <| mref.Dispose()
                        match exec with
                        | Choice1Of2 () ->
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot delete MutableCloudRef %A" mref, ex), None)) :: rest

                    | NewCloudFile(container, id, serializer) :: rest ->
                        let! exec = Async.Catch <| config.CloudFileProvider.Create(container, id, serializer)
                        match exec with
                        | Choice1Of2 file ->
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue file, typeof<ICloudFile>)) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled  <| ValueExpr (Exc (new StoreException(sprintf "Cannot create CloudFile, Container: %s, Name: %s" container id, ex), None)) :: rest
                    | GetCloudFile(container, id) :: rest ->
                        let! exec = Async.Catch <| config.CloudFileProvider.GetExisting(container, id)
                        match exec with
                        | Choice1Of2 file ->
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue file, typeof<ICloudFile>)) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled  <| ValueExpr (Exc (new StoreException(sprintf "Cannot get CloudFile, Container: %s, Name: %s" container id, ex), None)) :: rest
                    | GetCloudFiles(container) :: rest ->
                        let! exec = Async.Catch <| config.CloudFileProvider.GetContainedFiles container
                        match exec with
                        | Choice1Of2 files ->
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue files, typeof<ICloudFile []>)) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled  <| ValueExpr (Exc (new StoreException(sprintf "Cannot get CloudFiles, Container: %s" container, ex), None)) :: rest
                    
                    | ReadCloudFile(file, deserialize, t) :: rest ->
                        let! exec = Async.Catch <| async { let! stream = file.Read() in return! deserialize stream }
                        match exec with
                        | Choice1Of2 o ->
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue o, t)) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled  <| ValueExpr (Exc (new StoreException(sprintf "Cannot read CloudFile: %A" file, ex), None)) :: rest                        

                    | LogExpr msg :: rest ->
                        logger.LogUserInfo msg
                        return! run traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest
                    | TraceExpr cloudExpr :: rest ->
                        return! run true <| cloudExpr :: DoEndTraceExpr :: rest
                    | NewCloudSeqByNameExpr (container, values, t) :: rest ->
                        let id = Guid.NewGuid().ToString()
                        let! exec = Async.Catch <| config.CloudSeqProvider.Create(container, id, t, values)
                        match exec with
                        | Choice1Of2 cloudSeq ->
                            return! run traceEnabled <| (ValueExpr (Obj (ObjValue cloudSeq, typeof<ICloudSeq>))) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot create Container: %s, Name: %s" container id, ex), None)) :: rest
                    | GetCloudSeqByNameExpr (container, id, t) :: rest ->
                        let! cseq = Async.Catch <| config.CloudSeqProvider.GetExisting(container, id)
                        match cseq with
                        | Choice1Of2 cseq ->
                            if t <> cseq.Type
                            then return! run traceEnabled <| ValueExpr (Exc (new Exception(sprintf "CloudSeq type mismatch. Internal type %s, got : %s" cseq.Type.AssemblyQualifiedName t.AssemblyQualifiedName), None)) :: rest
                            else return! run traceEnabled <| ValueExpr (Obj (ObjValue cseq, cseq.Type)) :: rest
                        | Choice2Of2 exn ->
                            return! run traceEnabled <| ValueExpr (Exc (new NonExistentObjectStoreException(container, id), None)) :: rest
                    | GetCloudSeqsByNameExpr (container) :: rest ->
                        let! exec = Async.Catch <| config.CloudSeqProvider.GetContainedSeqs(container)
                        match exec with
                        | Choice1Of2 seqs ->
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue seqs, typeof<ICloudSeq []>)) :: rest
                        | Choice2Of2 ex ->
                            return! run traceEnabled <| ValueExpr (Exc (new StoreException(sprintf "Cannot access Container: %s" container, ex), None)) :: rest
                    // DO Expr
                    // Monadic Bind
                    | ValueExpr (Obj (ObjValue value, _)) :: DoBindExpr (f, objF) :: rest -> 
                        return! run traceEnabled <| trapExc f value objF :: DoEndBindExpr (value, objF) :: rest
                    | ValueExpr (Obj _) as value :: DoTryWithExpr _ :: rest -> 
                        return! run traceEnabled <| value :: rest
                    | ValueExpr (Obj (_, _)) as value :: DoTryFinallyExpr f :: rest -> 
                        let result = trapExc (fun _ -> ValueExpr (Obj (ObjValue (f () :> obj), typeof<unit>))) () f
                        match result with
                        | ValueExpr (Obj (ObjValue (:? unit), t)) ->
                            return! run traceEnabled <| value :: rest
                        | ValueExpr (Exc (ex, ctx)) -> 
                            return! run traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                        | _ -> return raise <| new InvalidOperationException(sprintf "Invalid tryFinallyF result %A" result)
                    | DoForExpr (values, n, f, objF) :: rest | ValueExpr (Obj _) :: DoForExpr (values, n, f, objF) :: rest -> 
                        if n = values.Length then
                            return! run traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest
                        else
                            return! run traceEnabled <| trapExc (fun _ -> f values.[n]) () f :: DoForExpr (values, n + 1,  f, objF) :: rest
                    | (DoWhileExpr (guardF, bodyExpr) as doWhileExpr) :: rest | ValueExpr (Obj _) :: (DoWhileExpr (guardF, bodyExpr) as doWhileExpr) :: rest ->
                        let result = trapExc (fun _ -> ValueExpr (Obj (ObjValue (guardF () :> obj), typeof<bool>))) () guardF
                        match result with
                        | ValueExpr (Obj (ObjValue (:? bool as value), t)) ->
                            if value then
                                return! run traceEnabled <| bodyExpr :: doWhileExpr :: rest
                            else
                                return! run traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest
                        | ValueExpr (Exc (ex, ctx)) ->
                            return! run traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                        | _ -> return raise <| new InvalidOperationException(sprintf "Invalid guardF result %A" result)
                    | DoCombineExpr secondExpr :: rest ->
                        return! run traceEnabled <| secondExpr :: rest
                    | _ :: DoCombineExpr secondExpr :: rest ->
                        return! run traceEnabled <| secondExpr :: rest
                    | ValueExpr (Obj (ObjValue value, _)) as valueExpr :: DoDisposableBindExpr cloudDisposable :: rest ->
                        let! valueExpr = 
                            async {
                                try
                                    do! cloudDisposable.Dispose()
                                    return valueExpr 
                                with ex -> return ValueExpr (Exc (ex, None)) 
                            }
                        return! run traceEnabled <| valueExpr :: rest
                    | cloudExpr :: DoEndTraceExpr :: rest -> 
                        let traceEnabled' = rest |> List.exists (fun cloudExpr' -> match cloudExpr' with DoEndTraceExpr -> true | _ -> false)
                        return! run traceEnabled' <| cloudExpr :: rest
                    | cloudExpr :: DoEndDelayExpr _ :: rest -> 
                        return! run traceEnabled <| cloudExpr :: rest
                    | cloudExpr :: DoEndBindExpr _ :: rest -> 
                        return! run traceEnabled <| cloudExpr :: rest
                    | cloudExpr :: DoEndTryWithExpr _ :: rest -> 
                        return! run traceEnabled <| cloudExpr :: rest
                    | [ValueExpr (Obj (value, t))] -> return stack // return
                    | ValueExpr (ParallelThunks (thunkValues, elementType)) :: rest ->
                        let arrayOfResults = Array.CreateInstance(elementType, thunkValues.Length)
                        Array.Copy(thunkValues |> Array.map (fun thunkValue -> 
                                                                match thunkValue with 
                                                                | Thunk ((ValueExpr (Obj (CloudRefValue cloudRef, _)))) -> cloudRef.Value 
                                                                | Thunk ((ValueExpr (Obj (ObjValue value, _)))) -> value | _ -> throwInvalidState thunkValue),
                                    arrayOfResults, thunkValues.Length)
                        return! run traceEnabled <| ValueExpr (Obj (ObjValue arrayOfResults, arrayOfResults.GetType())) :: rest
                    | ValueExpr (Obj (CloudRefValue (CloudRef value), t)) :: rest ->
                        return! run traceEnabled <| ValueExpr (Obj (ObjValue value, t)) :: rest
                    // unwind the stack
                    | ValueExpr (Exc (ex, ctx)) :: DoTryWithExpr (f, objF) :: rest ->
                        return! run traceEnabled <| trapExc (fun _ -> f ex) () f :: DoEndTryWithExpr (ex, objF) :: rest
                    | ValueExpr (Exc (ex, ctx)) :: DoTryFinallyExpr f :: rest ->
                        let result = trapExc (fun _ -> ValueExpr (Obj (ObjValue (f () :> obj), typeof<unit>))) () f
                        match result with
                        | ValueExpr (Obj (ObjValue (:? unit as value), t)) ->
                            return! run traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                        | ValueExpr (Exc (ex', ctx)) -> 
                            return! run traceEnabled <| ValueExpr (Exc (ex', ctx)) :: rest
                        | _ -> return raise <| new InvalidOperationException(sprintf "Invalid tryFinallyF result %A" result)
                    | ValueExpr (Exc (ex, ctx)) as excExpr :: DoDisposableBindExpr cloudDisposable :: rest ->
                        let! valueExpr = 
                            async {
                                try
                                    do! cloudDisposable.Dispose()
                                    return excExpr 
                                with ex -> return ValueExpr (Exc (ex, None)) 
                            }
                        return! run traceEnabled <| excExpr :: rest
                    | ValueExpr (Exc (ex, ctx)) :: _  :: rest ->
                        return! run traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                    | [ValueExpr (Exc (ex, ctx))] -> return stack // return
                    // Primitives Expr 
                    | GetWorkerCountExpr :: rest -> return stack
                    | GetProcessIdExpr :: rest -> return stack
                    | GetTaskIdExpr :: rest -> return stack
                    | LocalExpr _ :: rest -> return stack 
                    | ParallelExpr (_, _) :: rest -> return stack
                    | ChoiceExpr _ :: rest -> return stack
                    | _ -> return raise <| new InvalidOperationException(sprintf "Invalid state %A" stack)
                }
            run traceEnabled stack


        /// <summary>
        ///     Evaluates a cloud expression using thread-parallel semantics.
        /// </summary>
        /// <param name="config"></param>
        /// <param name="logger"></param>
        /// <param name="processId"></param>
        /// <param name="taskF"></param>
        /// <param name="functions"></param>
        /// <param name="traceEnabled"></param>
        /// <param name="stack"></param>
        let evaluateLocal (config : CoreConfiguration) (logger : ICloudLogger)
                                (processId : ProcessId) (taskF : unit -> string)
                                (functions : FunctionInfo list) (traceEnabled : bool) 
                                (stack : CloudExpr list) : Async<Value> =
            
            /// Serialize and deserialize a CloudExpr to force ``call by value`` semantics
            /// on parallel/choice expressions and ensure consistency between distributed execution
            /// and local/shared-memory scenarios
            let deepClone (expr : CloudExpr) = config.Cloner.Clone expr
            
            let rec runLocal (traceEnabled : bool) (stack : CloudExpr list) = 
                async {
                    let! stack' = evaluateSequential config logger functions traceEnabled stack
                    match stack' with 
                    | [ValueExpr value] -> return value
                    | GetWorkerCountExpr :: rest -> 
                        return! runLocal traceEnabled <| ReturnExpr (Environment.ProcessorCount, typeof<int>) :: rest
                    | GetProcessIdExpr :: rest ->
                        return! runLocal traceEnabled <| ValueExpr (Obj (ObjValue processId, typeof<int>)) :: rest
                    | GetTaskIdExpr :: rest ->
                        let threadId = taskF ()
                        return! runLocal traceEnabled <| ValueExpr (Obj (ObjValue threadId, typeof<string>)) :: rest
                    | LocalExpr cloudExpr :: rest -> 
                        return! runLocal traceEnabled <| cloudExpr :: rest
                    | ParallelExpr (cloudExprs, elementType) :: rest ->
                        let cloudExprs = Array.map deepClone cloudExprs
                        let! values = cloudExprs |> Array.map (fun cloudExpr -> runLocal traceEnabled [cloudExpr]) |> Async.Parallel
                        match values |> Array.tryPick (fun value -> match value with Exc (ex, ctx) -> Some ex | _ -> None) with
                        | Some _ -> 
                            let results = values |> Array.map (fun value -> match value with Obj (value, _) -> ValueResult value | Exc (ex, ctx) -> ExceptionResult (ex, ctx) | _ -> raise <| new InvalidOperationException(sprintf "Invalid state %A" value))
                            let parallelException = new Nessos.MBrace.ParallelCloudException(processId, results) :> exn
                            return! runLocal traceEnabled <| ValueExpr (Exc (parallelException, None)) :: rest
                        | None -> 
                            let arrayOfResults = Array.CreateInstance(elementType, values.Length)
                            Array.Copy(values |> Array.map (fun value -> match value with Obj (ObjValue value, t) -> value | _ -> throwInvalidState value), arrayOfResults, values.Length)
                            return! runLocal traceEnabled <| ValueExpr (Obj (ObjValue arrayOfResults, elementType)) :: rest

                    | ChoiceExpr (choiceExprs, elementType) :: rest ->
                        let cloudExprs = Array.map deepClone choiceExprs
                        let evalChoiceExpr choiceExpr = async {
                            let! result = runLocal traceEnabled [choiceExpr]
                            match result with
                            | Obj (ObjValue value, t) ->
                                if value = null then // value is option type and we use the fact that None is represented as null
                                    return None
                                else
                                    return Some result
                            | Exc (ex, ctx) -> return Some result
                            | _ -> return raise <| new InvalidOperationException(sprintf "Invalid state %A" result)
                        }
                        let! result = choiceExprs |> Array.map evalChoiceExpr |> Async.Choice
                        match result with
                        | Some (value) ->
                            return! runLocal traceEnabled <| ValueExpr (value) :: rest
                        | None -> 
                            return! runLocal traceEnabled <| ValueExpr (Obj (ObjValue None, elementType)) :: rest
                    | _ -> return raise <| new InvalidOperationException(sprintf "Invalid state %A" stack)
                }

            runLocal traceEnabled stack

        
        /// <summary>
        ///     Evaluates a cloud workflow using thread-parallel execution semantics
        /// </summary>
        /// <param name="config"></param>
        /// <param name="logger"></param>
        /// <param name="traceEnabled"></param>
        /// <param name="computation"></param>
        let evaluateLocalWrapped (config : CoreConfiguration) (logger : ICloudLogger) traceEnabled (computation : Cloud<'T>) =
            async {
                let processId = 0
                let getTaskId () = sprintf "localthread-%d" System.Threading.Thread.CurrentThread.ManagedThreadId
                let! result = evaluateLocal config logger processId getTaskId [] traceEnabled [Cloud.unwrapExpr computation]

                match result with
                | Obj (ObjValue value, t) -> return value :?> 'T
                | Exc (ex, ctx) when (ex :? MBraceException) -> return raise ex
                | Exc (ex, ctx) -> return raise <| new CloudException(ex, 0, ?context = ctx)
                | _ -> return raise <| new InvalidOperationException(sprintf "Invalid result %A" result)
            }