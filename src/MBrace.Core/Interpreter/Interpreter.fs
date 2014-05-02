namespace Nessos.MBrace.Core

    open System
    open System.Collections
    open System.Reflection

    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns

    open Nessos.Thespian.PowerPack
    
    open Nessos.MBrace
    //open Nessos.MBrace.Runtime
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Store

    type InterpreterConfiguration =
        {
            CloudSeqStore         : ICloudSeqStore
            CloudRefStore         : ICloudRefStore
            CloudFileStore        : ICloudFileStore
            IMutableCloudRefStore : IMutableCloudRefStore
            Serializer            : Nessos.FsPickler.FsPickler
        }

    module internal Interpreter =
        let logger = IoC.Resolve<ILogger>()
        let cloudSeqStoreLazy = lazy ( IoC.Resolve<ICloudSeqStore>() ) 
        let cloudFileStoreLazy = lazy ( IoC.Resolve<ICloudFileStore>() ) 
        let cloudRefStoreLazy = lazy ( IoC.Resolve<ICloudRefStore>() )
        let mutablecloudrefstorelazy = lazy ( IoC.Resolve<IMutableCloudRefStore>() )
        let cloudLogStoreLazy = lazy ( IoC.Resolve<StoreLogger>() )

        //increasing number as user/trace log id
        let private logIdCounter = new AtomicCounter()

        // Monadic Trampoline Interpreter
        let rec internal run (processId : int) (taskId : string) 
                                (functions : FunctionInfo list) (traceEnabled : bool)
                                (stack : CloudExpr list) : Async<CloudExpr list> = 
            let rec run' (traceEnabled : bool) (stack : CloudExpr list) =
                // Helper Functions
                let userLog msg = 
                        let id = logIdCounter.Incr()
                        UserLog { DateTime = DateTime.Now; Message = msg; ProcessId = processId; TaskId = taskId; Id = id }

                let tryToExtractInfo (typeName : string) = 
                    match typeName with
                    | RegexMatch "(.+)@(\d+)" [funcName; line] -> Some (funcName, line)
                    | _ -> None
                let rec tryToExtractVars (varName : string) (expr : Expr) = 
                    match expr with
                    | ExprShape.ShapeVar(v) -> []
                    | ExprShape.ShapeLambda(v, Let (targetVar, Var(_), body)) when v.Name = varName -> [targetVar.Name] @ tryToExtractVars varName body
                    | Let (targetVar, TupleGet(Var v, _), body) when v.Name = varName -> [targetVar.Name] @ tryToExtractVars varName body
                    | ExprShape.ShapeLambda(v, body) -> tryToExtractVars varName body
                    | ExprShape.ShapeCombination(a, args) -> args |> List.map (tryToExtractVars varName) |> List.concat

                let extractInfo (value : obj) (objF : obj) : CloudDumpContext = 
                    // check for special closure types that contain no user call-site info
                    if objF.GetType().Name.StartsWith("Invoke@") then
                        { File = ""; Start = (0, 0); End = (0, 0); CodeDump = ""; FunctionName = ""; Vars = [||] }
                    else
                        // try to extract extra info
                        let funcName, line =
                            match tryToExtractInfo <| objF.GetType().Name with
                            | Some (funcName, line) -> (funcName, line)
                            | None -> ("", "")
                        let funcInfoOption = functions |> Seq.tryFind (fun funInfo -> funInfo.MethodInfo.Name = funcName)
                        // construct environment
                        let vars = objF.GetType().GetFields() 
                                    |> Array.map (fun fieldInfo -> (fieldInfo.Name, fieldInfo.GetValue(objF)))
                                    |> Array.filter (fun (name, _) -> not <| name.EndsWith("@"))
                        let argName = objF.GetType().GetMethods().[0].GetParameters().[0].Name
                        let createVars varName = Array.append [|(varName, value)|] vars
                        let vars' = 
                            if argName = "unitVar" && value = null then 
                                vars
                            else if argName.StartsWith("_arg") then
                                match funcInfoOption with
                                | Some funcInfo ->
                                    match (argName, funcInfo.Expr) ||> tryToExtractVars with
                                    | _ :: _ as vars -> vars |> List.rev |> String.concat ", " |> createVars 
                                    | [] -> createVars argName
                                | None -> createVars argName
                            else createVars argName
                        let file = 
                            match funcInfoOption with
                            | Some funInfo -> funInfo.File
                            | None -> ""
                        { File = file; Start = (int line, 0); End = (0, 0); CodeDump = ""; FunctionName = funcName; Vars = vars' }
                let trapExc (f : obj -> CloudExpr) (value : obj) (objF : ObjFunc) : CloudExpr = 
                    try
                        f value
                    with 
                        // Special case for handling Unquote Exceptions
                       :? TargetInvocationException as ex ->
                           match ex.InnerException with
                           | :? TargetInvocationException as ex ->
                               ValueExpr (Exc (ex.InnerException, Some <| extractInfo value objF))
                           | ex -> ValueExpr (Exc (ex, Some <| extractInfo value objF))
                     | ex ->                         
                        ValueExpr (Exc (ex, Some <| extractInfo value objF))

                //////////////////////////////////////////
                let dumpTraceInfo stack = 
                    let extractScopeInfo stack =
                        stack 
                        |> List.tryPick (fun cloudExpr -> 
                            match cloudExpr with
                            | DoEndDelayExpr objF -> Some (() :> obj, objF)
                            | DoEndBindExpr (value, objF) -> Some (value, objF)
                            | DoEndTryWithExpr (value, objF) -> Some (value, objF)
                            | _ -> None)
                    let hasNoTraceInfo (objF : obj) = 
                        match objF.GetType().Name |> tryToExtractInfo with
                        | Some (funcName, _) -> 
                            match functions |> List.tryFind (fun functionInfo -> functionInfo.MethodInfo.Name = funcName) with
                            | Some functionInfo -> 
                                let attribute = functionInfo.MethodInfo.GetCustomAttribute(typeof<NoTraceInfoAttribute>)
                                if attribute = null then
                                    false
                                else
                                    true
                            | None -> false
                        | _ -> false
                    let rec logDump msg info = 
                        match info with
                        | Some (value : obj, objF : obj) ->
                            // check for NoTraceInfoAttribute
                            if not <| hasNoTraceInfo objF then 
                                // continue
                                let dumpContext = extractInfo value objF
                                let opt f x = if f x then None else Some x
                                let entry = Trace { 
                                        File = opt String.IsNullOrEmpty dumpContext.File
                                        Function = opt String.IsNullOrEmpty dumpContext.FunctionName
                                        Line = Some <| (fst dumpContext.Start)
                                        Message = msg
                                        DateTime = DateTime.Now
                                        Environment = dumpContext.Vars
                                                      |> Seq.map (fun (a,b) -> (a, sprintf' "%A" b))
                                                      |> Map.ofSeq
                                        ProcessId = processId
                                        TaskId = taskId
                                        Id = logIdCounter.Incr()
                                    }
                                cloudLogStoreLazy.Value.LogEntry(processId, entry)
                            else ()
                        | None -> ()

                    match stack with 
                    | DelayExpr _ :: DoTryWithExpr _ :: _ -> () // ignore
                    | ValueExpr _ :: DoEndDelayExpr _ :: DoTryWithExpr _ :: _ -> () // ignore
                    | ValueExpr _ :: DoEndTryWithExpr (ex, objF) :: _ -> logDump "with block end" <| Some (ex, objF)
                    | ValueExpr (Exc (ex, _)) :: DoTryWithExpr (_, objF) :: _ -> logDump "with block begin" <| Some (ex :> obj, objF)
                    | ValueExpr (Exc (ex, _)) :: _ ->  
                        logDump (sprintf' "unwind-stack ex = %s: %s" (ex.GetType().Name) ex.Message) (stack |> extractScopeInfo)
                    | DelayExpr (_, objF) :: DoWhileExpr _ :: _ -> logDump "while block begin" <| Some (() :> obj, objF)
                    | ValueExpr _ :: DoEndDelayExpr objF :: DoWhileExpr _  :: _ -> logDump "while block end" <| Some (() :> obj, objF)
                    | _ :: DoForExpr (values, n, _, objF) :: _ -> logDump "for loop block" <| Some (values.[n - 1], objF)
                    | ReturnExpr (value, t) :: _ -> logDump (sprintf' "return %A" value) (stack |> extractScopeInfo)
                    | DelayExpr (_, objF) :: _ -> logDump "cloud { ... } begin" <| Some (() :> obj, objF)
                    | BindExpr (_, _, _) :: rest -> logDump (sprintf' "let! begin") (stack |> extractScopeInfo)
                    | ValueExpr (Obj (ObjValue value, _)) :: DoBindExpr (_, objF) :: _ -> logDump "let! continue" <| Some (value, objF) 
                    | ValueExpr (Obj _) :: DoEndDelayExpr objF :: _ -> logDump "cloud { ... } end" <| Some (() :> obj, objF) 
                    | _ -> ()
                /////////////////////////////////////////
                async {
                    if traceEnabled then
                        dumpTraceInfo stack 
                    match stack with
                    // Expr
                    | ReturnExpr (value, t) :: rest -> 
                        return! run' traceEnabled <| ValueExpr (Obj (ObjValue value, t)) :: rest  
                    | DelayExpr (f, objF) :: DoEndDelayExpr _ :: rest | DelayExpr (f, objF) :: rest -> 
                        return! run' traceEnabled <| (trapExc (fun _ -> f ()) () objF)  :: (DoEndDelayExpr objF) :: rest
                    | BindExpr (cloudExpr, f, objF) :: rest -> 
                        return! run' traceEnabled <| cloudExpr :: DoBindExpr (f, objF) :: rest
                    | TryWithExpr (cloudExpr, f, objF) :: rest ->
                        return! run' traceEnabled <| cloudExpr :: DoTryWithExpr (f, objF) :: rest
                    | TryFinallyExpr (cloudExpr, f) :: rest ->
                        return! run' traceEnabled <| cloudExpr :: DoTryFinallyExpr f :: rest
                    | ForExpr (values, f, objF) :: rest ->
                        return! run' traceEnabled <| DoForExpr (values, 0, f, objF) :: rest
                    | WhileExpr (gruardF, cloudExpr) :: rest ->
                        return! run' traceEnabled <| DoWhileExpr (gruardF, cloudExpr) :: rest
                    | CombineExpr (firstExpr, secondExpr) :: rest ->
                        return! run' traceEnabled <| firstExpr :: DoCombineExpr secondExpr :: rest
                    | DisposableBindExpr (value, t, bindF, objF) :: rest ->
                        return! run' traceEnabled <| ValueExpr (Obj (ObjValue (value :> obj), t)) :: DoBindExpr (bindF, objF) :: DoDisposableBindExpr value :: rest
                    | [QuoteExpr expr] when expr.Type.IsGenericType && expr.Type.GetGenericTypeDefinition() = typedefof<ICloud<_>> ->
                        let cloudExpr : CloudExpr = 
                            try
                                let cloudExprWrap : CloudExprWrap = Swensen.Unquote.Operators.evalRaw expr 
                                cloudExprWrap.CloudExpr
                            with ex -> ValueExpr (Exc (ex, None))
                        return! run' traceEnabled <| [cloudExpr]
                    | QuoteExpr expr :: rest ->     
                        let cloudExpr : CloudExpr = 
                            try                
                                let result = Swensen.Unquote.Operators.evalRaw expr
                                ValueExpr (Obj (result, expr.Type))
                            with ex -> ValueExpr (Exc (ex, None)) 
                        return! run' traceEnabled <| cloudExpr :: rest
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
                        return! run' traceEnabled <| cloudExpr  :: rest

                    | NewRefByNameExpr (container, value, t) :: rest ->
                        let id = Guid.NewGuid().ToString()
                        let! exec = containAsync <| cloudRefStoreLazy.Value.Create(container, id, value, t)
                        match exec with
                        | Choice1Of2 result ->
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue result, result.GetType())) :: rest
                        | Choice2Of2 ex ->
                            return! run' traceEnabled <| ValueExpr (Exc (new Nessos.MBrace.StoreException(sprintf' "Cannot create Container: %s, Name: %s" container id, ex), None)) :: rest
                    | GetRefByNameExpr (container, id, t) :: rest ->
                        let! exists = cloudRefStoreLazy.Value.Exists(container, id)
                        if exists then
                            let! ty = cloudRefStoreLazy.Value.GetRefType(container, id)
                            if t <> ty 
                            then return! run' traceEnabled <| ValueExpr (Exc (new MBraceException(sprintf' "CloudRef type mismatch. Internal type %s, got : %s" ty.AssemblyQualifiedName t.AssemblyQualifiedName), None)) :: rest
                            else 
                                let cloudRefType = typedefof<PersistableCloudRef<_>>.MakeGenericType [| t |]
                                let cloudRef = Activator.CreateInstance(cloudRefType, [| id :> obj; container :> obj; t :> obj |])
                                return! run' traceEnabled <| ValueExpr (Obj (ObjValue cloudRef, cloudRefType)) :: rest
                        else
                            return! run' traceEnabled <| ValueExpr (Exc (new Nessos.MBrace.NonExistentObjectStoreException(container, id), None)) :: rest
                    | GetRefsByNameExpr (container) :: rest ->
                        let! exec = containAsync <| cloudRefStoreLazy.Value.GetRefs(container)
                        match exec with
                        | Choice1Of2 refs ->
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue refs, typeof<ICloudRef []>)) :: rest
                        | Choice2Of2 ex ->
                            return! run' traceEnabled <| ValueExpr (Exc (new Nessos.MBrace.StoreException(sprintf' "Cannot access Container: %s" container, ex), None)) :: rest

                    | NewMutableRefByNameExpr (container, id, value, t) :: rest ->
                        let! exists = containAsync <| mutablecloudrefstorelazy.Value.Exists(container, id) 
                        match exists with
                        | Choice1Of2 false ->
                            let! exec =
                                mutablecloudrefstorelazy.Value.Create(container, id, value, t)
                                |> containAsync
                            match exec with
                            | Choice1Of2 result ->
                                return! run' traceEnabled <| ValueExpr (Obj (ObjValue result, result.GetType())) :: rest
                            | Choice2Of2 ex ->
                                return! run' traceEnabled <| ValueExpr (Exc (new StoreException(sprintf' "Cannot create Container: %s, Name: %s" container id, ex), None)) :: rest
                        | Choice1Of2 true ->
                            return!  run' traceEnabled <| ValueExpr (Exc (new StoreException(sprintf' "Cannot create Container: %s, Name: %s. It already exists." container id), None)) :: rest
                        | Choice2Of2 ex ->
                                return! run' traceEnabled <| ValueExpr (Exc (new StoreException(sprintf' "Cannot create Container: %s, Name: %s" container id, ex), None)) :: rest
                    
                    | ReadMutableRefExpr(mref, ty) :: rest ->
                        let! exec = containAsync <| mutablecloudrefstorelazy.Value.Read(mref)
                        match exec with
                        | Choice1Of2 result ->
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue result, mref.Type)) :: rest
                        | Choice2Of2 ex ->
                            let! exists = containAsync <| mutablecloudrefstorelazy.Value.Exists(mref.Container, mref.Name)
                            match exists with
                            | Choice1Of2 false -> return! run' traceEnabled <| ValueExpr (Exc (new NonExistentObjectStoreException(mref.Container, mref.Name), None)) :: rest
                            | _ -> return! run' traceEnabled <| ValueExpr (Exc (new StoreException(sprintf' "Cannot locate Container: %s, Name: %s" mref.Container mref.Name, ex), None)) :: rest

                    | SetMutableRefExpr(mref, value) :: rest ->
                        let! exec = containAsync <| mutablecloudrefstorelazy.Value.Update(mref, value)
                        match exec with
                        | Choice1Of2 result -> 
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue result, typeof<bool>)) :: rest
                        | Choice2Of2 ex -> 
                            return! run' traceEnabled <| ValueExpr (Exc (new StoreException(sprintf' "Cannot update Container: %s, Name: %s" mref.Container mref.Name, ex), None)) :: rest

                    | ForceSetMutableRefExpr(mref, value) :: rest ->
                        let! exec = containAsync <| mutablecloudrefstorelazy.Value.ForceUpdate(mref, value)
                        match exec with
                        | Choice1Of2 result -> 
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue result, typeof<bool>)) :: rest
                        | Choice2Of2 ex -> 
                            return! run' traceEnabled <| ValueExpr (Exc (new StoreException(sprintf' "Cannot update Container: %s, Name: %s" mref.Container mref.Name, ex), None)) :: rest
                    
                    | GetMutableRefByNameExpr (container, id, t) :: rest ->
                        let! exists = mutablecloudrefstorelazy.Value.Exists(container, id)
                        if exists then
                            let! ty = mutablecloudrefstorelazy.Value.GetRefType(container, id)
                            if t <> ty 
                            then return! run' traceEnabled <| ValueExpr (Exc (new Exception(sprintf' "MutableCloudRef type mismatch. Internal type %s, got : %s" ty.AssemblyQualifiedName t.AssemblyQualifiedName), None)) :: rest
                            else 
                                let cloudRefType = typedefof<MutableCloudRef<_>>.MakeGenericType [| t |]
                                let cloudRef = Activator.CreateInstance(cloudRefType, [| id :> obj; container :> obj; "" :> obj; t :> obj |])
                                return! run' traceEnabled <| ValueExpr (Obj (ObjValue cloudRef, cloudRefType)) :: rest
                        else
                            return! run' traceEnabled <| ValueExpr (Exc (new NonExistentObjectStoreException(container, id), None)) :: rest
                    
                    | GetMutableRefsByNameExpr (container) :: rest ->
                        let! exec = containAsync <| mutablecloudrefstorelazy.Value.GetRefs(container)
                        match exec with
                        | Choice1Of2 refs ->
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue refs, typeof<IMutableCloudRef []>)) :: rest
                        | Choice2Of2 ex ->
                            return! run' traceEnabled <| ValueExpr (Exc (new StoreException(sprintf' "Cannot access Container: %s" container, ex), None)) :: rest
                    
                    | FreeMutableRefExpr(mref) :: rest ->
                        let! exec = containAsync <| mutablecloudrefstorelazy.Value.Delete(mref.Container, mref.Name)
                        match exec with
                        | Choice1Of2 () ->
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest
                        | Choice2Of2 ex ->
                            return! run' traceEnabled <| ValueExpr (Exc (new StoreException(sprintf' "Cannot delete MutableCloudRef %A" mref, ex), None)) :: rest

                    | NewCloudFile(container, id, serialize) :: rest ->
                        let! exists = containAsync <| cloudFileStoreLazy.Value.Exists(container, id)
                        match exists with
                        | Choice1Of2 false ->
                            let! exec = containAsync <| cloudFileStoreLazy.Value.Create(container, id, serialize)
                            match exec with
                            | Choice1Of2 file ->
                                return! run' traceEnabled <| ValueExpr (Obj (ObjValue file, typeof<ICloudFile>)) :: rest
                            | Choice2Of2 ex ->
                                return! run' traceEnabled  <| ValueExpr (Exc (new StoreException(sprintf' "Cannot create CloudFile, Container: %s, Name: %s" container id, ex), None)) :: rest
                        | _ ->
                            return! run' traceEnabled  <| ValueExpr (Exc (new StoreException(sprintf' "Cannot create CloudFile, Container: %s, Name: %s. It already exists." container id), None)) :: rest
                    
                    | GetCloudFile(container, id) :: rest ->
                        let! exec = containAsync <| cloudFileStoreLazy.Value.GetFile(container, id)
                        match exec with
                        | Choice1Of2 file ->
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue file, typeof<ICloudFile>)) :: rest
                        | Choice2Of2 ex ->
                            let! exec = containAsync <| cloudFileStoreLazy.Value.Exists(container, id)
                            match exec with
                            | Choice1Of2 false -> 
                                return! run' traceEnabled  <| ValueExpr (Exc (new NonExistentObjectStoreException(container, id), None)) :: rest
                            | _ -> 
                                return! run' traceEnabled  <| ValueExpr (Exc (new StoreException(sprintf' "Cannot get CloudFile, Container: %s, Name: %s" container id, ex), None)) :: rest
                    
                    | GetCloudFiles(container) :: rest ->
                        let! exec = containAsync <| cloudFileStoreLazy.Value.GetFiles(container)
                        match exec with
                        | Choice1Of2 files ->
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue files, typeof<ICloudFile []>)) :: rest
                        | Choice2Of2 ex ->
                            return! run' traceEnabled  <| ValueExpr (Exc (new StoreException(sprintf' "Cannot get CloudFiles, Container: %s" container, ex), None)) :: rest
                    
                    | ReadCloudFile(file, deserialize, t) :: rest ->
                        let! exec = containAsync <| cloudFileStoreLazy.Value.Read(file, deserialize)
                        match exec with
                        | Choice1Of2 o ->
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue o, t)) :: rest
                        | Choice2Of2 ex ->
                            let! exists = containAsync <| cloudFileStoreLazy.Value.Exists(file.Container, file.Name)
                            match exists with
                            | Choice1Of2 false -> return! run' traceEnabled  <| ValueExpr (Exc (new NonExistentObjectStoreException(file.Container, file.Name), None)) :: rest                        
                            | _ -> return! run' traceEnabled  <| ValueExpr (Exc (new StoreException(sprintf' "Cannot read CloudFile: %A" file, ex), None)) :: rest                        

                    | ReadCloudFileAsSeq(file, deserialize, t) :: rest ->
                        let! exec = containAsync <| cloudFileStoreLazy.Value.ReadAsSeq(file, deserialize, t)
                        match exec with
                        | Choice1Of2 o ->
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue o, t)) :: rest
                        | Choice2Of2 ex ->
                            let! exists = containAsync <| cloudFileStoreLazy.Value.Exists(file.Container, file.Name)
                            match exists with
                            | Choice1Of2 false -> return! run' traceEnabled  <| ValueExpr (Exc (new NonExistentObjectStoreException(file.Container, file.Name), None)) :: rest                        
                            | _ -> return! run' traceEnabled  <| ValueExpr (Exc (new StoreException(sprintf' "Cannot read CloudFile: %A" file, ex), None)) :: rest                        


                    | LogExpr msg :: rest ->
                        let entry = userLog msg
                        cloudLogStoreLazy.Value.LogEntry(processId, entry)
                        return! run' traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest
                    | TraceExpr cloudExpr :: rest ->
                        return! run' true <| cloudExpr :: DoEndTraceExpr :: rest
                    | NewCloudSeqByNameExpr (container, values, t) :: rest ->
                        let id = Guid.NewGuid().ToString()
                        let! exec = containAsync <| cloudSeqStoreLazy.Value.Create(values, container, id, t)
                        match exec with
                        | Choice1Of2 cloudSeq ->
                            return! run' traceEnabled <| (ValueExpr (Obj (ObjValue cloudSeq, typeof<ICloudSeq>))) :: rest
                        | Choice2Of2 ex ->
                            return! run' traceEnabled <| ValueExpr (Exc (new StoreException(sprintf' "Cannot create Container: %s, Name: %s" container id, ex), None)) :: rest
                    | GetCloudSeqByNameExpr (container, id, t) :: rest ->
                        let! exists = cloudSeqStoreLazy.Value.Exists(container, id)
                        if exists then
                            let cloudSeqTy = typedefof<CloudSeq<_>>.MakeGenericType [| t |]
                            let cloudSeq = Activator.CreateInstance(cloudSeqTy, [| id :> obj; container :> obj |])
                            let ty = (cloudSeq :?> ICloudSeq).Type
                            if t <> ty 
                            then return! run' traceEnabled <| ValueExpr (Exc (new Exception(sprintf' "CloudSeq type mismatch. Internal type %s, got : %s" ty.AssemblyQualifiedName t.AssemblyQualifiedName), None)) :: rest
                            else return! run' traceEnabled <| ValueExpr (Obj (ObjValue cloudSeq, cloudSeqTy)) :: rest
                        else
                            return! run' traceEnabled <| ValueExpr (Exc (new NonExistentObjectStoreException(container, id), None)) :: rest
                    | GetCloudSeqsByNameExpr (container) :: rest ->
                        let! exec = containAsync <| cloudSeqStoreLazy.Value.GetSeqs(container)
                        match exec with
                        | Choice1Of2 seqs ->
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue seqs, typeof<ICloudSeq []>)) :: rest
                        | Choice2Of2 ex ->
                            return! run' traceEnabled <| ValueExpr (Exc (new StoreException(sprintf' "Cannot access Container: %s" container, ex), None)) :: rest
                    | GetProcessIdExpr :: rest ->
                        return! run' traceEnabled <| ValueExpr (Obj (ObjValue processId, typeof<int>)) :: rest
                    | GetTaskIdExpr :: rest ->
                        return! run' traceEnabled <| ValueExpr (Obj (ObjValue taskId, typeof<string>)) :: rest
                    // DO Expr
                    // Monadic Bind
                    | ValueExpr (Obj (ObjValue value, _)) :: DoBindExpr (f, objF) :: rest -> 
                        return! run' traceEnabled <| trapExc f value objF :: DoEndBindExpr (value, objF) :: rest
                    | ValueExpr (Obj _) as value :: DoTryWithExpr _ :: rest -> 
                        return! run' traceEnabled <| value :: rest
                    | ValueExpr (Obj (_, _)) as value :: DoTryFinallyExpr f :: rest -> 
                        let result = trapExc (fun _ -> ValueExpr (Obj (ObjValue (f () :> obj), typeof<unit>))) () f
                        match result with
                        | ValueExpr (Obj (ObjValue (:? unit), t)) ->
                            return! run' traceEnabled <| value :: rest
                        | ValueExpr (Exc (ex, ctx)) -> 
                            return! run' traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                        | _ -> return raise <| new InvalidOperationException(sprintf "Invalid tryFinallyF result %A" result)
                    | DoForExpr (values, n, f, objF) :: rest | ValueExpr (Obj _) :: DoForExpr (values, n, f, objF) :: rest -> 
                        if n = values.Length then
                            return! run' traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest
                        else
                            return! run' traceEnabled <| trapExc (fun _ -> f values.[n]) () f :: DoForExpr (values, n + 1,  f, objF) :: rest
                    | (DoWhileExpr (guardF, bodyExpr) as doWhileExpr) :: rest | ValueExpr (Obj _) :: (DoWhileExpr (guardF, bodyExpr) as doWhileExpr) :: rest ->
                        let result = trapExc (fun _ -> ValueExpr (Obj (ObjValue (guardF () :> obj), typeof<bool>))) () guardF
                        match result with
                        | ValueExpr (Obj (ObjValue (:? bool as value), t)) ->
                            if value then
                                return! run' traceEnabled <| bodyExpr :: doWhileExpr :: rest
                            else
                                return! run' traceEnabled <| ValueExpr (Obj (ObjValue (), typeof<unit>)) :: rest
                        | ValueExpr (Exc (ex, ctx)) ->
                            return! run' traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                        | _ -> return raise <| new InvalidOperationException(sprintf "Invalid guardF result %A" result)
                    | DoCombineExpr secondExpr :: rest ->
                        return! run' traceEnabled <| secondExpr :: rest
                    | _ :: DoCombineExpr secondExpr :: rest ->
                        return! run' traceEnabled <| secondExpr :: rest
                    | ValueExpr (Obj (ObjValue value, _)) as valueExpr :: DoDisposableBindExpr cloudDisposable :: rest ->
                        let! valueExpr = 
                            async {
                                try
                                    do! cloudDisposable.Dispose()
                                    return valueExpr 
                                with ex -> return ValueExpr (Exc (ex, None)) 
                            }
                        return! run' traceEnabled <| valueExpr :: rest
                    | cloudExpr :: DoEndTraceExpr :: rest -> 
                        let traceEnabled' = rest |> List.exists (fun cloudExpr' -> match cloudExpr' with DoEndTraceExpr -> true | _ -> false)
                        return! run' traceEnabled' <| cloudExpr :: rest
                    | cloudExpr :: DoEndDelayExpr _ :: rest -> 
                        return! run' traceEnabled <| cloudExpr :: rest
                    | cloudExpr :: DoEndBindExpr _ :: rest -> 
                        return! run' traceEnabled <| cloudExpr :: rest
                    | cloudExpr :: DoEndTryWithExpr _ :: rest -> 
                        return! run' traceEnabled <| cloudExpr :: rest
                    | [ValueExpr (Obj (value, t))] -> return stack // return
                    | ValueExpr (ParallelThunks (thunkValues, elementType)) :: rest ->
                        let arrayOfResults = Array.CreateInstance(elementType, thunkValues.Length)
                        Array.Copy(thunkValues |> Array.map (fun thunkValue -> 
                                                                match thunkValue with 
                                                                | Thunk ((ValueExpr (Obj (CloudRefValue cloudRef, _)))) -> cloudRef.Value 
                                                                | Thunk ((ValueExpr (Obj (ObjValue value, _)))) -> value | _ -> throwInvalidState thunkValue),
                                    arrayOfResults, thunkValues.Length)
                        return! run' traceEnabled <| ValueExpr (Obj (ObjValue arrayOfResults, arrayOfResults.GetType())) :: rest
                    | ValueExpr (Obj (CloudRefValue (CloudRef value), t)) :: rest ->
                        return! run' traceEnabled <| ValueExpr (Obj (ObjValue value, t)) :: rest
                    // unwind the stack
                    | ValueExpr (Exc (ex, ctx)) :: DoTryWithExpr (f, objF) :: rest ->
                        return! run' traceEnabled <| trapExc (fun _ -> f ex) () f :: DoEndTryWithExpr (ex, objF) :: rest
                    | ValueExpr (Exc (ex, ctx)) :: DoTryFinallyExpr f :: rest ->
                        let result = trapExc (fun _ -> ValueExpr (Obj (ObjValue (f () :> obj), typeof<unit>))) () f
                        match result with
                        | ValueExpr (Obj (ObjValue (:? unit as value), t)) ->
                            return! run' traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                        | ValueExpr (Exc (ex', ctx)) -> 
                            return! run' traceEnabled <| ValueExpr (Exc (ex', ctx)) :: rest
                        | _ -> return raise <| new InvalidOperationException(sprintf "Invalid tryFinallyF result %A" result)
                    | ValueExpr (Exc (ex, ctx)) as excExpr :: DoDisposableBindExpr cloudDisposable :: rest ->
                        let! valueExpr = 
                            async {
                                try
                                    do! cloudDisposable.Dispose()
                                    return excExpr 
                                with ex -> return ValueExpr (Exc (ex, None)) 
                            }
                        return! run' traceEnabled <| excExpr :: rest
                    | ValueExpr (Exc (ex, ctx)) :: _  :: rest ->
                        return! run' traceEnabled <| ValueExpr (Exc (ex, ctx)) :: rest
                    | [ValueExpr (Exc (ex, ctx))] -> return stack // return
                    // Primitives Expr 
                    | GetWorkerCountExpr :: rest -> return stack
                    | LocalExpr _ :: rest -> return stack 
                    | ParallelExpr (_, _) :: rest -> return stack
                    | ChoiceExpr _ :: rest -> return stack
                    | _ -> return raise <| new InvalidOperationException(sprintf "Invalid state %A" stack)
                }
            run' traceEnabled stack

        and internal runLocal (processId : int) (taskId : string) 
                                (functions : FunctionInfo list) (traceEnabled : bool)
                                (stack : CloudExpr list)
                                (serializer : Nessos.FsPickler.FsPickler) : Async<Value> = 
            
            /// Serialize and deserialize a CloudExpr to force ``call by value`` semantics
            /// on parallel/choice expressions and ensure consistency between distributed execution
            /// and local/shared-memory scenarios
            let deepClone : CloudExpr -> CloudExpr = 
                fun x ->
                    use mem = new System.IO.MemoryStream()
                    serializer.Serialize(mem, x)
                    mem.Position <- 0L
                    serializer.Deserialize<CloudExpr>(mem)
            
            let rec runLocal' (traceEnabled : bool) (stack : CloudExpr list) = 
                async {
                    let! stack' = run processId taskId functions traceEnabled stack 
                    match stack' with 
                    | [ValueExpr value] -> return value
                    | GetWorkerCountExpr :: rest -> 
                        return! runLocal' traceEnabled <| ReturnExpr (Environment.ProcessorCount, typeof<int>) :: rest
                    | LocalExpr cloudExpr :: rest -> 
                        return! runLocal' traceEnabled <| cloudExpr :: rest
                    | ParallelExpr (cloudExprs, elementType) :: rest ->
                        let cloudExprs = Array.map deepClone cloudExprs
                        let! values = cloudExprs |> Array.map (fun cloudExpr -> runLocal processId taskId functions traceEnabled [cloudExpr] serializer) |> Async.Parallel
                        match values |> Array.tryPick (fun value -> match value with Exc (ex, ctx) -> Some ex | _ -> None) with
                        | Some _ -> 
                            let parallelException = new Nessos.MBrace.ParallelCloudException(processId, values |> Array.map (fun value -> match value with Obj (value, _) -> ValueResult value | Exc (ex, ctx) -> ExceptionResult (ex, ctx) | _ -> raise <| new InvalidOperationException(sprintf "Invalid state %A" value))) :> exn
                            return! runLocal' traceEnabled <| ValueExpr (Exc (parallelException, None)) :: rest
                        | None -> 
                            let arrayOfResults = Array.CreateInstance(elementType, values.Length)
                            Array.Copy(values |> Array.map (fun value -> match value with Obj (ObjValue value, t) -> value | _ -> throwInvalidState value), arrayOfResults, values.Length)
                            return! runLocal' traceEnabled <| ValueExpr (Obj (ObjValue arrayOfResults, elementType)) :: rest

                    | ChoiceExpr (choiceExprs, elementType) :: rest ->
                        let cloudExprs = Array.map deepClone choiceExprs
                        let! result = 
                                choiceExprs |> Array.map (fun choiceExpr ->
                                                            async {
                                                                let! result = runLocal processId taskId functions traceEnabled [choiceExpr] serializer
                                                                match result with
                                                                | Obj (ObjValue value, t) ->
                                                                    if value = null then // value is option type and we use the fact that None is represented as null
                                                                        return None
                                                                    else
                                                                        return Some result
                                                                | Exc (ex, ctx) -> return Some result
                                                                | _ -> return raise <| new InvalidOperationException(sprintf "Invalid state %A" result)
                                                            })
                                            |> Async.Choice
                        match result with
                        | Some (value) ->
                            return! runLocal' traceEnabled <| ValueExpr (value) :: rest
                        | None -> 
                            return! runLocal' traceEnabled <| ValueExpr (Obj (ObjValue None, elementType)) :: rest
                    | _ -> return raise <| new InvalidOperationException(sprintf "Invalid state %A" stack)
                }
            runLocal' traceEnabled stack
              
        and runLocalWrapper (computation : ICloud<'T>) (serializer : Nessos.FsPickler.FsPickler) =
            async {
                let! result = runLocal 0 "" [] false [unWrapCloudExpr computation] serializer
                match result with
                | Obj (ObjValue value, t) -> return value :?> 'T
                | Exc (ex, ctx) -> return raise ex
                | _ -> return raise <| new InvalidOperationException(sprintf "Invalid result %A" result)
            }