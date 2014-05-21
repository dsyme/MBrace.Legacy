namespace Nessos.MBrace.Core

    open Nessos.MBrace.C
    open Nessos.MBrace.Core.Utils

    module internal InterpreterUtils =

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