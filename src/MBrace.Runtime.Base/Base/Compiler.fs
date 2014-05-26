namespace Nessos.MBrace.Runtime

    open System
    open System.Reflection

    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Microsoft.FSharp.Quotations.ExprShape

    open Nessos.MBrace
    open Nessos.MBrace.Core

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Reflection
    open Nessos.MBrace.Runtime.CloudUtils

    open Nessos.MBrace.Runtime

    [<AutoOpen>]
    module private Compiler =

        type CompilerInfo =
            | Warning of string
            | Error of string

        let printLocation (metadata : ExprMetadata) =
            sprintf "%s(%d,%d)" metadata.File metadata.StartRow metadata.StartCol

        // checks given expression tree, generating mbrace-related errors and warnings:
        // 1. checks if calls to cloud blocks are accompanied with a [<Cloud>] attribute
        // 2. checks if top-level bindings in the cloud monad are serializable
        // 3. checks that cloud block methods are static
        // 4. checks that cloud blocks do not make calls to the mbrace client API.
        let checkExpression (name : string option) (expr : Expr) =

            let gathered = ref []

            let blockName =
                match name with
                | None -> "Cloud block"
                | Some name -> sprintf "Cloud block '%s'" name

            let log errorType (node : Expr) fmt =
                let prefix =
                    match ExprMetadata.TryParse node with
                    | None -> ""
                    | Some m -> printLocation m

                Printf.ksprintf(fun msg -> gathered := errorType (sprintf "%s: %s" prefix msg) :: gathered.Value) fmt

            let checkCurrentNode (e : Expr) =
                match e with
                // cloud { ... }
                | CloudBuilderExpr body ->
                    let bindings = gatherTopLevelCloudBindings body
                    for v, metadata in bindings do
                        if not <| Serialization.DefaultPickler.IsSerializableType v.Type then
                            log Error e "%s has binding '%s' of type '%s' that is not serializable." blockName v.Name v.Type.Name
                
                // let! x = <external cloud expression>
                | CloudMemberInfo (memberInfo, methodBase) ->
                    // referenced cloud expression is not a reflected definition
                    if not memberInfo.IsReflectedDefinition then
                        log Error e "%s depends on '%s' which lacks [<Cloud>] attribute." blockName memberInfo.Name

                    // fail if cloud expression is not static method
                    elif not methodBase.IsStatic then
                        log Error e "%s references non-static cloud workflow '%s'. This is not supported." blockName memberInfo.Name

                | MemberInfo (m,_) ->
                    // check if cloud expression references inappropriate MBrace libraries
                    let assembly = match m with :? Type as t -> t.Assembly | m -> m.DeclaringType.Assembly
                    let assemblyName = assembly.GetName()
                    let isProhibitedAssembly = 
                        assemblyName.Name.StartsWith "Nessos.MBrace.Runtime"
                        || assemblyName.Name.StartsWith "Nessos.MBrace.Client"

                    if isProhibitedAssembly then
                        log Error e "%s references runtime method '%s'." blockName m.Name

                | _ -> ()


            Expr.iter checkCurrentNode expr

            gathered.Value


        let checkTopLevelQuotation (expr : Expr) =
            let metadata = ExprMetadata.TryParse expr
            checkExpression None expr

        let checkFunctionInfo (f : FunctionInfo) =
            checkExpression (Some f.FunctionName) f.Expr

        let rec tryGetName (expr : Expr) =
            match expr with 
            | MemberInfo (m,_) -> Some m.Name
            | ShapeLambda(_,body) -> tryGetName body
            | _ -> None

        /// the main compiler method
        let compile (expr : Expr) =
            
            // gather function info
            let functions = getFunctionInfo expr

            let errors =
                [
                    yield! checkTopLevelQuotation expr

                    for f in functions do yield! checkFunctionInfo f
                ]

            functions, errors


    type CloudComputationPackage private (name : string, expr : Expr, returnType : Type, functions : FunctionInfo list, info : CompilerInfo list) =

        member __.Name = name
        member __.Expr = expr
        member __.Functions = functions
        member __.Errors = info |> List.choose (function Error m -> Some m | _ -> None)
        member __.Warnings = info |> List.choose (function Warning m -> Some m | _ -> None)
        member __.ReturnType = returnType
        member __.CloudExpr = 
            let block = Swensen.Unquote.Operators.evalRaw expr
            Interpreter.extractCloudExpr block

        static member Compile (expr : Expr<Cloud<'T>>, ?name : string) = 
            let name = 
                match name with
                | Some name -> name
                | None -> defaultArg (Compiler.tryGetName expr) ""

            let functions, errors = Compiler.compile expr
            
            new CloudComputationPackage(name, expr, typeof<'T>, functions, errors)