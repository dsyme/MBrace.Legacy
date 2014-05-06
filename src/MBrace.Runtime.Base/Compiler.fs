namespace Nessos.MBrace.Core

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.CloudUtils
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Reflection
    open Nessos.MBrace.Utils.Quotations

    open System
    open System.Collections
    open System.Linq
    open System.Reflection
    open System.Runtime.Serialization
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Core.LanguagePrimitives.IntrinsicFunctions
    open System.CodeDom.Compiler

    module internal Compiler = 
        
//        exception CompilerException of string
        type CompilerException = Nessos.MBrace.CompilerException
        type CompilerState = CompilerState<Expr, Function list>

//
//        let (|MethodInfoWithOutDeclaringTypeFix|_|) (methodInfo : MethodInfo) = 
//            match methodInfo.DeclaringType with
//            | Named (typeDef, _) when typeDef = typedefof<Fix<_, _>> -> None
//            | _ -> Some methodInfo

//        let (|PropertyInfoWithOutDeclaringTypeFix|_|) (propertyInfo : PropertyInfo) = 
//            match (|MethodInfoWithOutDeclaringTypeFix|_|) <| propertyInfo.GetGetMethod() with
//            | Some _ -> Some propertyInfo
//            | None -> None

        // val ( |MethodInfoWithReturnTypeCloud|_| ) : MethodInfo -> MethodInfo option
        let (|MethodInfoWithReturnTypeCloud|_|) (methodInfo : MethodInfo) = 
            let rec checkForCloud (t : Type) =
                match t with
                | Named (typeDef, typeArgs) 
                    when typeDef = typedefof<ICloud<_>> -> [t]
                | Named (typeDef, typeArgs) -> 
                    typeArgs |> Array.map checkForCloud |> Seq.concat |> Seq.toList
                | Param (_) -> []
                | Array (elementType, _) -> checkForCloud elementType
                | Ptr (_, _) -> []

            match checkForCloud methodInfo.ReturnType with
            | _ :: _ ->
                if (methodInfo.IsGenericMethod && 
                                methodInfo.GetGenericArguments() 
                                |> Array.exists (fun argType -> checkForCloud argType |> List.exists (fun _ -> true))) then 
                    None
                else 
                    Some methodInfo
            | [] -> None

        // val ( |PropertyInfoWithPropertyTypeCloud|_| ) : PropertyInfo -> PropertyInfo option
        let (|PropertyInfoWithPropertyTypeCloud|_|) (propertyInfo : PropertyInfo) = 
            let methodInfo = propertyInfo.GetGetMethod()
            if methodInfo = null then None
            else
                match (|MethodInfoWithReturnTypeCloud|_|) methodInfo with
                | Some _ -> Some propertyInfo
                | None -> None

        let unCurryArgs (t: Type) (args : Expr list) : Expr list =
            match t with
            | Named (typeDef, typeArgs) 
                    when typeDef = typedefof<Tuple<_, _>> ||
                         typeDef = typedefof<Tuple<_, _, _>> ||
                         typeDef = typedefof<Tuple<_, _, _, _>> ||
                         typeDef = typedefof<Tuple<_, _, _, _, _>> ||
                         typeDef = typedefof<Tuple<_, _, _, _, _, _>> ||
                         typeDef = typedefof<Tuple<_, _, _, _, _, _, _>> ||
                         typeDef = typedefof<Tuple<_, _, _, _, _, _, _, _>> -> [Expr.NewTuple args]
            | _ -> args

        let rec compile (pkg : CloudPackage) : (Type * Expr * ICloud * Function list) = 

            let rec compile (expr : Expr) : CompilerState = 

//                    printfn "patchCloudExpr %A" expr
//                    printfn "--------" 
                match expr with

                | PropertyGet (None, PropertyInfoWithPropertyTypeCloud (PropertyGetterWithReflectedDefinition(body) as propertyInfo), []) -> 
                    state {
                        let unitVar = new Var("___unit___", typeof<unit>)
                        let! _ = callExpression (propertyInfo.GetGetMethod()) unitVar body [Expr.Value(())] body.CustomAttributes
                        return expr
                    }

                | Call (None, MethodInfoWithReturnTypeCloud (MethodWithReflectedDefinition(ShapeLambda (variable, body) as expr') as methodInfo), args) ->
                    state {
                        let! _ = callExpression methodInfo variable body args expr'.CustomAttributes
                        return expr
                    }

//                | Call (expressionOption, MethodInfoWithReturnTypeCloud(MethodInfoWithOutDeclaringTypeFix(methodInfo)), args) as callExpression ->
//                    raise <| CompilerException(sprintf' "Function %s missing [<Cloud>] attribute" methodInfo.Name)
//
//                | PropertyGet (expressionOption, PropertyInfoWithPropertyTypeCloud(PropertyInfoWithOutDeclaringTypeFix(propertyInfo)), args) as getExpression ->
//                    raise <| CompilerException(sprintf' "Property get %s missing [<Cloud>] attribute" propertyInfo.Name)

                | ExprShape.ShapeLambda(var, body) -> 
                    state { 
                        let! body' = compile body // generateFSharpExpr to cache expr
                        return Expr.Lambda(var, body')
                    }
                | ExprShape.ShapeVar(_) as varExpr -> state { return varExpr }
                | ExprShape.ShapeCombination(obj, args) -> state { let! exprs = sequence (List.map compile args) in return ExprShape.RebuildShapeCombination(obj, exprs) }

                
            and callExpression (methodInfo : MethodInfo) (variable : Var) (body : Expr) (args : Expr list) (customAttributes : Expr list): CompilerState = 
                state { 
                    let args = unCurryArgs variable.Type args
                    // collect functon metadata 
                    let functionName = methodInfo.Name
                    let functionMetadata = 
                        match customAttributes with
                        | [ NewTuple [_; NewTuple [Value (file, _); Value (startRow, _); Value (startColumn, _); Value (endRow, _); Value(endCol, _)]] ] -> 
                            { MethodInfo = methodInfo; File = file :?> string; StartRow = startRow :?> int; StartCol = startColumn :?> int; EndRow = endRow :?> int; EndCol = endCol :?> int; Expr = body }
                        | _ -> { MethodInfo = methodInfo; File = ""; StartRow = 0; StartCol = 0; EndRow = 0; EndCol = 0; Expr = body }

                    // get current state, after recursion
                    let! functions = getState
                    if functions |> Seq.exists (fun funInfo -> funInfo.MethodInfo.Name = functionName) then
                        return body
                    else
                        // update functions 
                        do! setState <| functionMetadata :: functions

                        // recursive call with updated state
                        let! body' = compile body
                        return body'
                }
            try
                let (_, functions) = execute (compile pkg.Expr) []
                pkg.ReturnType, pkg.Expr, pkg.Eval() ,functions
            with | :? Nessos.MBrace.CompilerException  as ex -> reraise ()
                 | ex -> raise <| Nessos.MBrace.CompilerException("Compiler exception", ex)


       
