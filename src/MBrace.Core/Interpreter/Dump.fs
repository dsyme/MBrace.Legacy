namespace Nessos.MBrace.Core

    open System
    open System.Reflection

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Core.Utils

    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    
    module internal DumpExtractors =

        let tryExtractInfo (typeName : string) = 
            match typeName with
            | RegexMatch "(.+)@(\d+)" [funcName; line] -> Some (funcName, line)
            | _ -> None

        let rec tryExtractVars (varName : string) (expr : Expr) = 
            match expr with
            | ExprShape.ShapeVar(v) -> []
            | ExprShape.ShapeLambda(v, Let (targetVar, Var(_), body)) when v.Name = varName -> [targetVar.Name] @ tryExtractVars varName body
            | Let (targetVar, TupleGet(Var v, _), body) when v.Name = varName -> [targetVar.Name] @ tryExtractVars varName body
            | ExprShape.ShapeLambda(v, body) -> tryExtractVars varName body
            | ExprShape.ShapeCombination(a, args) -> args |> List.map (tryExtractVars varName) |> List.concat

        let extractInfo (functions : FunctionInfo list) (value : obj) (objF : obj) : CloudDumpContext = 
            // check for special closure types that contain no user call-site info
            if objF.GetType().Name.StartsWith("Invoke@") then
                { File = ""; Start = (0, 0); End = (0, 0); CodeDump = ""; FunctionName = ""; Vars = [||] }
            else
                // try to extract extra info
                let funcName, line =
                    match tryExtractInfo <| objF.GetType().Name with
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
                            match (argName, funcInfo.Expr) ||> tryExtractVars with
                            | _ :: _ as vars -> vars |> List.rev |> String.concat ", " |> createVars 
                            | [] -> createVars argName
                        | None -> createVars argName
                    else createVars argName
                let file = 
                    match funcInfoOption with
                    | Some funInfo -> funInfo.File
                    | None -> ""
                { File = file; Start = (int line, 0); End = (0, 0); CodeDump = ""; FunctionName = funcName; Vars = vars' }


        let dumpTraceInfo (functions : FunctionInfo list) (logger : ICloudLogger) stack = 
            let extractScopeInfo stack =
                stack 
                |> List.tryPick (fun cloudExpr -> 
                    match cloudExpr with
                    | DoEndDelayExpr objF -> Some (() :> obj, objF)
                    | DoEndBindExpr (value, objF) -> Some (value, objF)
                    | DoEndTryWithExpr (value, objF) -> Some (value, objF)
                    | _ -> None)
            let hasNoTraceInfo (objF : obj) = 
                match objF.GetType().Name |> tryExtractInfo with
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
                        let dumpContext = extractInfo functions value objF
                        let opt f x = if f x then None else Some x
                        let traceInfo = { 
                                File = opt String.IsNullOrEmpty dumpContext.File
                                Function = opt String.IsNullOrEmpty dumpContext.FunctionName
                                Line = Some <| (fst dumpContext.Start)
//                                DateTime = DateTime.Now
                                Environment = dumpContext.Vars
                                                |> Seq.map (fun (a,b) -> (a, sprintf "%A" b))
                                                |> Map.ofSeq
//                                ProcessId = processId
//                                TaskId = taskId
//                                Id = logIdCounter.Next()
                            }

                        logger.LogTraceInfo (msg, traceInfo)
//                        config.CloudLogger.LogTraceInfo(processId, entry)
                    else ()
                | None -> ()

            match stack with 
            | DelayExpr _ :: DoTryWithExpr _ :: _ -> () // ignore
            | ValueExpr _ :: DoEndDelayExpr _ :: DoTryWithExpr _ :: _ -> () // ignore
            | ValueExpr _ :: DoEndTryWithExpr (ex, objF) :: _ -> logDump "with block end" <| Some (ex, objF)
            | ValueExpr (Exc (ex, _)) :: DoTryWithExpr (_, objF) :: _ -> logDump "with block begin" <| Some (ex :> obj, objF)
            | ValueExpr (Exc (ex, _)) :: _ ->  
                logDump (sprintf "unwind-stack ex = %s: %s" (ex.GetType().Name) ex.Message) (stack |> extractScopeInfo)
            | DelayExpr (_, objF) :: DoWhileExpr _ :: _ -> logDump "while block begin" <| Some (() :> obj, objF)
            | ValueExpr _ :: DoEndDelayExpr objF :: DoWhileExpr _  :: _ -> logDump "while block end" <| Some (() :> obj, objF)
            | _ :: DoForExpr (values, n, _, objF) :: _ -> logDump "for loop block" <| Some (values.[n - 1], objF)
            | ReturnExpr (value, t) :: _ -> logDump (sprintf "return %A" value) (stack |> extractScopeInfo)
            | DelayExpr (_, objF) :: _ -> logDump "cloud { ... } begin" <| Some (() :> obj, objF)
            | BindExpr (_, _, _) :: rest -> logDump (sprintf "let! begin") (stack |> extractScopeInfo)
            | ValueExpr (Obj (ObjValue value, _)) :: DoBindExpr (_, objF) :: _ -> logDump "let! continue" <| Some (value, objF) 
            | ValueExpr (Obj _) :: DoEndDelayExpr objF :: _ -> logDump "cloud { ... } end" <| Some (() :> obj, objF) 
            | _ -> ()