namespace Nessos.MBrace.Core

    open System
    open System.IO
    open System.Reflection
    open System.Diagnostics
    open System.Runtime.Serialization
    open System.CodeDom.Compiler
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Metadata
    open Microsoft.FSharp.Core.LanguagePrimitives.IntrinsicFunctions
    open Microsoft.FSharp.Compiler.CodeDom

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Reflection
    open Nessos.MBrace.Utils.Quotations
    // Suppress compiler warning regarding UnboxGeneric
    #nowarn "1204"

    module QuotationCompiler =
        (********************************Helper top level functions********************)
        // val inc : (int -> unit -> int)
        let inc n = 
            let counter = ref n
            fun () -> counter := !counter + 1; !counter
        let (|TypeCheck|_|) (check : Type) (input : Type) = if input = check then Some input else None
        let unitType = typeof<unit>
        let cloudBuilderType = typeof<CloudBuilder>
        let (|VariableType|_|) (typeofVariable : Type) (variable : Var) = if variable.Type = typeofVariable then Some variable else None

        let rec decompile (expr : Expr) : string =
            (********************************Helper Inner functions********************)
            let rec prettyPrintType (t : Type) = 
                if t.IsGenericParameter then
                    sprintf "'%s" t.Name
                else
                    let fullName = t.FullName.Replace("+", ".") // for cases with nested types, replace + with .  
                    if t.GetGenericArguments().Length = 0 then
                        fullName
                    else
                        let genericArguments = t.GetGenericArguments()
                        let concatGenericArguments () = String.Join(",", genericArguments |> Array.map prettyPrintType)
                        match tryMe (fun () -> let entity = FSharpEntity.FromType(t) in entity, entity.DisplayName), t with
                        | Some (entity, _), Named (typeCon, [|argType; resultType|]) when typeCon = typedefof<_ -> _> -> 
                            sprintf' "(%s -> %s)" (prettyPrintType argType) (prettyPrintType resultType)
                        | Some (entity, displayName), _ -> 
                            let unmangledName = sprintf' "%s.%s" entity.Namespace displayName
                            sprintf' "%s<%s>" unmangledName <| concatGenericArguments ()
                        | None, Named (typeCon, [|arg1; arg2|]) when typeCon = typedefof<(_ * _)> -> 
                            sprintf' "(%s * %s)" (prettyPrintType arg1) (prettyPrintType arg2)
                        | None, Named (typeCon, [|arg1; arg2; arg3|]) when typeCon = typedefof<(_ * _ * _)> -> 
                            sprintf' "(%s * %s * %s)" (prettyPrintType arg1) (prettyPrintType arg2) (prettyPrintType arg3)
                        | None, Named (typeCon, [|arg1; arg2; arg3; arg4|]) when typeCon = typedefof<(_ * _ * _ * _)> -> 
                            sprintf' "(%s * %s * %s * %s)" (prettyPrintType arg1) (prettyPrintType arg2) (prettyPrintType arg3) (prettyPrintType arg4)
                        | None, Array (arrayType, n) ->
                            sprintf' "%s %s" (prettyPrintType arrayType) (String.replicate n "[]")
                        | None, _ -> 
                            let typeDefeninition = fullName
                            let unmangledName = typeDefeninition.Substring(0, typeDefeninition.IndexOf("`"))
                            sprintf' "%s<%s>" unmangledName <| concatGenericArguments ()
                        

            let tryGetMemberOrValue (entity : FSharpEntity) (memberInfo : MemberInfo) = tryMe (fun () -> entity.MembersOrValues) |> (Option.bind <| Seq.tryFind (fun memberOrValue -> memberOrValue.CompiledName = memberInfo.Name))
            let methodSourceName (memberInfo : MemberInfo) (tryGerEntity : FSharpEntity option) =
                match tryGerEntity with
                | Some entity ->
                    let tryMemberOrValue = tryGetMemberOrValue entity memberInfo
                    match tryMemberOrValue with 
                    | Some memberOrValue ->
                        Some memberOrValue.DisplayName
                    | None -> None
                | None ->
                    memberInfo.GetCustomAttributes(true)
                    |> Array.tryPick 
                            (function
                                | :? CompilationSourceNameAttribute as csna -> Some(csna)
                                | _ -> None)
                    |> (function | Some(csna) -> Some csna.SourceName | None -> None)

            let concatExprs exprs seperator = 
                exprs |> List.map decompile |> (fun (value : string list) -> String.Join(seperator, value))

            let generateCallSite instanceExprOption (memberInfo : MemberInfo) (args : Expr list) = 
                let tryGetEntity = tryMe (fun () -> FSharpEntity.FromType(memberInfo.DeclaringType))
                let tryGetDisplayName = tryGetEntity |> Option.bind (fun entity -> tryMe (fun () -> entity.DisplayName))
                let objectString =
                    match instanceExprOption with
                    | Some instanceExpr -> sprintf' "(%s)" <| decompile  instanceExpr
                    | None ->
                            match tryGetEntity, tryGetDisplayName with
                            | Some entity, Some entityDisplayName -> 
                                if entity.Namespace = "" then
                                    sprintf' "%s" entityDisplayName
                                else
                                    sprintf' "%s.%s" entity.Namespace entityDisplayName
                            | _, _ -> prettyPrintType memberInfo.DeclaringType

                let argumentsString = 
                    match memberInfo with
                    | :? PropertyInfo -> ""
                    | _ ->
                        let tupledArgs = sprintf' "(%s)" (concatExprs args ", ")
                        match tryGetEntity with
                        | Some entity ->
                            match tryGetMemberOrValue entity memberInfo with
                            | Some memberOrValue when memberOrValue.CurriedParameterGroups.Count > 1 ->
                                " " + (concatExprs args " ")
                            | Some _ | None -> tupledArgs
                        | None -> tupledArgs
                    
                match tryGetEntity with
                | Some entity -> 
                    match methodSourceName memberInfo tryGetEntity with
                    | Some methodString -> sprintf' "%s.%s%s" objectString methodString argumentsString
                    // Super special case for UnionCase... like weired Item Properties in Nested types
                    | None when memberInfo.MemberType = MemberTypes.Property && memberInfo.Name.StartsWith("Item") -> 
                        sprintf' "(%s).GetType().GetProperty(\"%s\").GetValue(%s, null) :?> %s" objectString memberInfo.Name objectString (prettyPrintType (memberInfo :?> PropertyInfo).PropertyType)
                    | None -> sprintf' "%s.%s%s" objectString memberInfo.Name argumentsString
                | None -> sprintf' "%s.%s%s" objectString memberInfo.Name argumentsString
                
            (**************************************************************************)
            match expr with
            // general expressions
            | Patterns.Value(value, TypeCheck unitType _) -> "()"
            | Patterns.Value(value, t) -> sprintf' "%A" value
            | SpecificCall <@ (+) @> (_, _, [first; second]) -> sprintf' "(%s + %s)" (decompile first) (decompile second)
            | SpecificCall <@ (-) @> (_, _, [first; second]) -> sprintf' "(%s - %s)" (decompile first) (decompile second)
            | SpecificCall <@ (*) @> (_, _, [first; second]) -> sprintf' "(%s * %s)" (decompile first) (decompile second)
            | SpecificCall <@ (/) @> (_, _, [first; second]) -> sprintf' "(%s / %s)" (decompile first) (decompile second)
            | SpecificCall <@ (=) @> (_, _, [first; second]) -> sprintf' "(%s = %s)" (decompile first) (decompile second)
            | SpecificCall <@ (<>) @> (_, _, [first; second]) -> sprintf' "(%s <> %s)" (decompile first) (decompile second)
            | SpecificCall <@ (>) @> (_, _, [first; second]) -> sprintf' "(%s > %s)" (decompile first) (decompile second)
            | SpecificCall <@ (<) @> (_, _, [first; second]) -> sprintf' "(%s < %s)" (decompile first) (decompile second)
            | SpecificCall <@ (>=) @> (_, _, [first; second]) -> sprintf' "(%s >= %s)" (decompile first) (decompile second)
            | SpecificCall <@ (<=) @> (_, _, [first; second]) -> sprintf' "(%s <= %s)" (decompile first) (decompile second)
            | SpecificCall <@ (&&) @> (_, _, [first; second]) -> sprintf' "(%s && %s)" (decompile first) (decompile second)
            | SpecificCall <@ (||) @> (_, _, [first; second]) -> sprintf' "(%s || %s)" (decompile first) (decompile second)
            | SpecificCall <@ (|>) @> (_, _, [first; second]) -> sprintf' "(%s |> %s)" (decompile first) (decompile second)
            | SpecificCall <@ (<|) @> (_, _, [first; second]) -> sprintf' "(%s <| %s)" (decompile first) (decompile second)
            | SpecificCall <@ (||>) @> (_, _, [first; second; third]) -> sprintf' "((%s, %s) ||> %s)" (decompile first) (decompile second) (decompile third)
            | SpecificCall <@ (<||) @> (_, _, [first; second; third]) -> sprintf' "(%s <|| (%s, %s))" (decompile first) (decompile second) (decompile third)
            | SpecificCall <@ (>>) @> (_, _, [first; second]) -> sprintf' "(%s >> %s)" (decompile first) (decompile second)
            | SpecificCall <@ (<<) @> (_, _, [first; second]) -> sprintf' "(%s << %s)" (decompile first) (decompile second)
            | SpecificCall <@ not @> (_, _, [first]) -> sprintf' "(not %s)" (decompile first) 
            | SpecificCall <@ (!) @> (_, _, [first]) -> sprintf' "!%s" (decompile first)
            | SpecificCall <@ (:=) @> (_, _, [first; second]) -> sprintf' "%s := %s" (decompile first) (decompile second)
            | SpecificCall <@ (..) @> (_, _, [first; second]) -> sprintf' "{ %s .. %s }" (decompile first) (decompile second)
            | SpecificCall <@ UnboxGeneric @> (_, [typeArg], [arg]) -> 
                sprintf' "(%s :?> %s)" (decompile arg) (prettyPrintType typeArg)
            | SpecificCall <@ GetArray @> (_, _, [first; second]) -> 
                sprintf' "%s.[%s]" (decompile first) (decompile second)
            | SpecificCall <@ SetArray @> (_, _, [first; second; third]) -> 
                sprintf' "%s.[%s] <- %s" (decompile first) 
                                        (decompile second)
                                        (decompile third)
            | IfThenElse (boolExpr, thenExpr, elseExpr) -> sprintf' "(if %s then %s else %s)" (decompile boolExpr) (decompile  thenExpr ) (decompile  elseExpr)
            | WhileLoop (guardExpr, bodyExpr) -> 
                sprintf' "(while %s do %s)" (decompile guardExpr) (decompile  bodyExpr) 

            | Call (instanceExprOption, methodInfo, args) -> 
                sprintf' "(%s)" (generateCallSite instanceExprOption methodInfo args) 
            | PropertyGet (instanceExprOption, propertyInfo, args) -> 
                sprintf' "(%s)" (generateCallSite instanceExprOption propertyInfo args) 

            | NewObject(constructorInfo, args) -> sprintf' "(new %s(%s))" (prettyPrintType constructorInfo.DeclaringType) (concatExprs args ", ")
            | NewTuple(exprs) -> sprintf' "(%s)" (concatExprs exprs ", ") 
            | NewArray(arrayType, elements) -> sprintf' "[|%s|]" (concatExprs elements "; ")
            | NewUnionCase(caseInfo, [head; tail]) 
                when caseInfo.DeclaringType.IsGenericType && caseInfo.DeclaringType.GetGenericTypeDefinition() = typedefof<list<_>> 
                -> sprintf' "%s :: %s" (decompile head) (decompile tail)
            | NewUnionCase(caseInfo, []) 
                when caseInfo.DeclaringType.IsGenericType && caseInfo.DeclaringType.GetGenericTypeDefinition() = typedefof<list<_>> 
                -> "[]" 
            | NewUnionCase(caseInfo, []) -> sprintf' "%s.%s" (prettyPrintType caseInfo.DeclaringType) caseInfo.Name 
            | NewUnionCase(caseInfo, exprs) -> 
                sprintf' "(%s.%s (%s))" (prettyPrintType caseInfo.DeclaringType) caseInfo.Name (concatExprs exprs ", ")
                    
            // Specialization for performance
            | UnionCaseTest (expr, caseInfo) when expr.Type.IsGenericType && expr.Type.GetGenericTypeDefinition() = typedefof<_ option> ->
                if caseInfo.Name = "Some" then
                    sprintf' "(match %s with Some _ -> true | _ -> false)" (decompile expr)
                else
                    sprintf' "(match %s with None -> true | _ -> false)" (decompile expr)
            | UnionCaseTest (expr, caseInfo) ->
                sprintf' "(let ___reuse___ = %s in ___reuse___.GetType().GetProperty(\"Is%s\").GetValue(___reuse___, null) :?> bool)" (decompile expr) caseInfo.Name 
            | TypeTest (expr, typeTest) -> sprintf' "%s :? %s" (decompile expr) (prettyPrintType typeTest)
            | Coerce (expr, typeTest) -> 
                match expr.Type, typeTest with
                | Named (exprTypeCon, [|_; _|]), Named (typeTestCon, _) when typeTestCon = typedefof<_ -> _> &&
                                                                                exprTypeCon.BaseType.GetGenericTypeDefinition() = typedefof<FSharpFunc<_, _>> ->
                    sprintf' "(%s :> obj :?> %s)" (decompile expr) (prettyPrintType typeTest)
                | _ when typeTest.IsAssignableFrom(expr.Type) ->
                    sprintf' "(%s :> %s)" (decompile expr) (prettyPrintType typeTest)
                | _ ->
                    sprintf' "(%s :?> %s)" (decompile expr) (prettyPrintType typeTest)
            | Sequential(first, second) -> sprintf' "%s; %s" (decompile first ) (decompile second)
            | Let (variable, bindExpr, bodyExpr) -> 
                if variable.IsMutable then
                    sprintf' "(let mutable (%s : %s) = %s in %s)" variable.Name (prettyPrintType variable.Type) (decompile bindExpr)  (decompile bodyExpr)
                else
                    sprintf' "(let (%s : %s) = %s in %s)" variable.Name (prettyPrintType variable.Type) (decompile bindExpr)  (decompile bodyExpr)
            | TupleGet (expr, index) -> 
                let genericArgs = (expr.Type.GetGenericArguments() |> Array.map prettyPrintType |> (fun types -> String.Join(", ", types)))
                sprintf' "((%s :> obj :?> System.Tuple<%s>).Item%s)" (decompile expr) genericArgs (string (index + 1))
            | VarSet (var, expr) -> sprintf' "%s <- %s" var.Name (decompile expr)
            | Application (left, right) -> sprintf' "(%s |> %s)" (decompile right) (decompile left) // swap application order... to help type inference 
            | ExprShape.ShapeVar(var) when var.Name = "builder@" -> "builder" // special case
            | ExprShape.ShapeVar(var) -> var.Name
            | ExprShape.ShapeLambda(var, body) when var.Name = "builder@"  -> sprintf' "(fun builder -> %s)" (decompile body) // special case
            | ExprShape.ShapeLambda(var, body) -> sprintf' "(fun (%s : %s) -> %s)" var.Name (prettyPrintType var.Type) (decompile body)
            | _ -> failwithf "Cannot match expr: %A" expr

        let lambdaLift (expr : Expr) (argVarName : string) (vars : seq<Var>) : Expr =
            let objsVar = new Var(argVarName, typeof<obj[]>)
            
            let (expr', _) =
                vars |> Seq.fold (fun (accExpr, i) var ->
                    let accExpr' =
                        Expr.Let(var, 
                            (fun objs index -> Expr.Coerce(<@@ let (___objs___ : obj []) = %%objs in ___objs___.[%%index] @@>, var.Type)) (Expr.Var objsVar) (Expr.Value i), accExpr)
                    (accExpr', i + 1)
                ) (expr, 0) 

            expr'

        let rec substituteConstValuesWithVars (inc : unit -> int) (expr : Expr) : Expr * (Var * obj) list =
            match expr with
            | Patterns.Value (value, t) -> 
                if [typeof<int>; typeof<string>; typeof<bool>] |> List.exists (fun t' -> t = t') then
                    (Expr.Value (value, t), []) // Safe case... to be written in the binary
                else
                    let newVar = new Var(sprintf' "___constant%d___" <| inc(), t) 
                    (Expr.Var(newVar), [newVar, value])
            | ExprShape.ShapeVar(v) -> (Expr.Var(v), [])
            | ExprShape.ShapeLambda(v, body) -> 
                let body', constantVars = substituteConstValuesWithVars inc body 
                (Expr.Lambda(v, body'), constantVars)
            | ExprShape.ShapeCombination(a, args) -> 
                let result = args |> List.map (substituteConstValuesWithVars inc)
                let expr' = ExprShape.RebuildShapeCombination(a, result |> List.map fst)
                let constantVars = result |> List.map snd |> List.concat
                (expr', constantVars)

        let lambdaLiftConstantsAndFreeVars (expr : Expr) = 
            let (expr', constantVars) = substituteConstValuesWithVars (inc 0) expr
            let expr'' = (expr', "___constant___", constantVars |> List.map fst |> Seq.ofList) |||> lambdaLift  
            let expr''' = (expr'', "___arg___", expr.GetFreeVars()) |||> lambdaLift  
            (expr''', constantVars)
    
        let compileToFSharpString (expr : Expr) : string = 
            let (expr', _) = lambdaLiftConstantsAndFreeVars (expr)
            expr' |> decompile 

        // a local cache for compiled IL
        let internal cacheCompiledIdMapAtom : Atom<Map<string, OptimizedClosures.FSharpFunc<obj[], obj[], obj>>> = Atom.atom Map.empty
        // Batch mode compile to IL
        let batchCompileToIL (funIdExprs : (string * Expr) []) : (string * (obj[] -> obj)) [] = 
            let funIdVarsSources = funIdExprs |> Array.map (fun (funId, expr) ->
                                                                let (expr', constantVars) = lambdaLiftConstantsAndFreeVars (expr)
                                                                (funId, constantVars, expr' |> decompile))
            
            let memoized, toBeCompiled = funIdVarsSources |> Array.partition (fun (_, _, source) -> cacheCompiledIdMapAtom.Value.ContainsKey source)
            let memoizedResults = 
                memoized |> Array.map (fun (funId, constantVars, source) ->
                                         (funId, let func = Map.find source cacheCompiledIdMapAtom.Value in (fun objs -> func.Invoke (constantVars |> List.map snd |> List.toArray, objs))))
            if toBeCompiled.Length <> 0 then
                let compiler = new FSharpCodeProvider() 
                let code index source = 
                        sprintf' 
                            "namespace Test 
                                    type TestFunc%d() = 
                                        inherit OptimizedClosures.FSharpFunc<obj[], obj[], obj>() 
                                        override self.Invoke(_ : obj[]) : obj[] -> obj = raise <| new System.NotImplementedException()
                                        override self.Invoke(___constant___ : obj[], ___arg___ : obj[]) : obj = (%s) :> _" index source
                                
            
                //printfn "%s" code
                let options = new CompilerParameters() 
                let ignoreAssemblies = [typeof<int>.Assembly; typeof<unit>.Assembly]
                for assembly in AppDomain.CurrentDomain.GetAssemblies() do
                    if not assembly.IsDynamic && not (ignoreAssemblies |> List.exists (fun ignoreAssembly -> ignoreAssembly = assembly)) 
                        && assembly.Location <> "" then
                        if assembly.GlobalAssemblyCache then
                            options.ReferencedAssemblies.Add(assembly.GetName().Name) |> ignore
                        else
                            options.ReferencedAssemblies.Add(assembly.Location) |> ignore

                options.ReferencedAssemblies.Add("FSharp.PowerPack.dll") |> ignore
                //options.ReferencedAssemblies.Add("FSharp.Core.dll") |> ignore
                options.ReferencedAssemblies.Add("System.Core.dll") |> ignore
                //options.OutputAssembly <- "c:\\myDll.dll"
                options.CompilerOptions <- "--optimize --platform:anycpu"
                options.GenerateInMemory <- true
                // avoid trashing the temp folder with compiler generated files
                options.TempFiles <- new TempFileCollection(Directory.GetCurrentDirectory())

                let code = toBeCompiled |> Array.mapi (fun index (_, _, source) -> code index source) |> (fun code -> String.concat Environment.NewLine code)
                let cr = compiler.CompileAssemblyFromSource(options, code) 
                let errors = [| for error in cr.Errors do yield error |] 
                let onlyErrors = errors |> Array.filter (fun error -> not error.IsWarning) 
                if onlyErrors.Length > 0 then
                    let errorsWithNumberZero = onlyErrors |> Array.filter (fun error -> error.ErrorNumber = "0")
                    if errorsWithNumberZero.Length > 0 then
                        failwithf "Compilation Failed (with some errors 0) Code: (%s) Errors: %A" code errors
                    else
                        failwithf "Compilation Failed Code: (%s) Errors: %A" code errors


                let results = 
                    toBeCompiled |> Array.mapi (fun index (funId, constantVars, source) ->
                                    let ass = cr.CompiledAssembly 
                                    let testFunc = ass.GetType(sprintf' "Test.TestFunc%d" index) 
            
                                    let func = Activator.CreateInstance(testFunc, [||]) :?> OptimizedClosures.FSharpFunc<obj[], obj[], obj>
                                    // update cache
                                    Atom.swap cacheCompiledIdMapAtom (fun cacheCompiledIdMap -> Map.add source func cacheCompiledIdMap)
                                    let constants = constantVars |> List.map snd |> List.toArray
                                    (funId, (fun objs -> func.Invoke (constants, objs))))
                Array.append memoizedResults results
            else
                memoizedResults
                
        let compileToIL (funIdExpr : (string * Expr)) : (string * (obj[] -> obj)) = 
            batchCompileToIL [|funIdExpr|] |> (fun results -> results.[0])

        // a local cache for compiled Exprs
        let cacheCompiledExprsAtom : Atom<Map<string, (obj[] -> obj) option>> = Atom.atom Map.empty
        let logger : Lazy<ILogger> = lazy (IoC.Resolve<ILogger>())
        let compile funId functionName expr = 
            let stopwatch = System.Diagnostics.Stopwatch()
            stopwatch.Start()
            let result = 
                try
                    Some <| compileToIL (funId, expr) 
                with exn ->
                    logger.Value.LogInfo (sprintf' "Interpreter: Failed to compile function: %s, ProcessId: %d, exn: %A" functionName (Process.GetCurrentProcess().Id) exn)
                    None
            //logger.Value.LogInfo(sprintf' "Compiled fragment: %s in function: %s, ProcessId: %d, result: %A, Time: %A" funId functionName (Process.GetCurrentProcess().Id) result stopwatch.Elapsed)
            result
#if QUOTATIONS_COMPILER
        let compileToILActor = MailboxProcessor<string * string * Expr>.Start(fun actor ->
                                    let rec loop () =
                                        async {
                                            if actor.CurrentQueueLength = 0 then
                                                do! Async.Sleep(5000)
                                            else
                                                let msgs = new System.Collections.Generic.List<(string * string * Expr)>()
                                                for i in [1..actor.CurrentQueueLength] do
                                                    let! (funId, functionName, expr) = actor.Receive()
                                                    msgs.Add((funId, functionName, expr))

                                                match tryMe (fun () -> msgs.ToArray() |> Array.map (fun (funId, _, expr) -> (funId, expr)) |> batchCompileToIL) with
                                                | Some funcs ->
                                                    for (funId, f) in funcs do
                                                        Atom.swap cacheCompiledExprsAtom (fun cacheCompiledExprs' -> Map.add funId (Some f) cacheCompiledExprs')
                                                | None ->
                                                    // Batch failed try one at a time
                                                    for (funId, functionName, expr) in msgs do
                                                        match compile funId functionName expr with
                                                        | Some (funId, f) -> 
                                                            // update maps
                                                            Atom.swap cacheCompiledExprsAtom (fun cacheCompiledExprs' -> Map.add funId (Some f) cacheCompiledExprs') 
                                                        | None ->
                                                            Atom.swap cacheCompiledExprsAtom (fun cacheCompiledExprs' -> Map.add funId None cacheCompiledExprs') 

                                            return! loop ()
                                        }
                                    loop ())
#endif
