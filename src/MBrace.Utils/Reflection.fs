namespace Nessos.MBrace.Utils

    open System
    open System.Collections
    open System.Collections.Generic
    open System.Text.RegularExpressions
    open System.Reflection
    open System.Runtime.Serialization

    open Microsoft.FSharp.Reflection
    open Microsoft.FSharp.Core.OptimizedClosures

    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Microsoft.FSharp.Quotations.ExprShape

    open Nessos.MBrace.Utils.String


    module Reflection =

        let (|Named|Array|Ptr|Param|) (t : System.Type) =
            if t.IsGenericType
            then Named(t.GetGenericTypeDefinition(), t.GetGenericArguments())
            elif t.IsGenericParameter
            then Param(t, t.GenericParameterPosition)
            elif not t.HasElementType
            then Named(t, [||])
            elif t.IsArray
            then
                let et = t.GetElementType()
                let rank =
                    match t.GetArrayRank() with
                    | 1 when et.MakeArrayType() = t -> None
                    | n -> Some n
                Array(et, rank)
            elif t.IsByRef
            then Ptr(true, t.GetElementType())
            elif t.IsPointer
            then Ptr(false, t.GetElementType())
            else failwith "MSDN says this can’t happen"

        /// matches against lambda types, returning a tuple ArgType [] * ResultType
        let (|FSharpFunc|_|) : Type -> _ =
            let fsFunctionTypes =
                hset [
                    typedefof<FSharpFunc<_,_>>
                    typedefof<FSharpFunc<_,_,_>>
                    typedefof<FSharpFunc<_,_,_,_>>
                    typedefof<FSharpFunc<_,_,_,_,_>>
                    typedefof<FSharpFunc<_,_,_,_,_,_>>
                ]

            let rec tryGetFSharpFunc =
                function
                | Named (t, args) when fsFunctionTypes.Contains t ->
                    let l = args.Length
                    Some(args.[0..l-2], args.[l-1])
                | t ->
                    if t.BaseType <> null then 
                        tryGetFSharpFunc t.BaseType
                    else None

            let rec collect (t : Type) =
                match tryGetFSharpFunc t with
                | None -> None
                | Some(args, rest) ->
                    match collect rest with
                    | Some(args', codomain) -> Some(Array.append args args', codomain)
                    | None -> Some (args, rest)

            collect

        let (|FsTuple|_|) (t : Type) =
            if FSharpType.IsTuple t then
                Some(FSharpType.GetTupleElements t)
            else None

        type Assembly with
            static member TryFind(name : string) =
                AppDomain.CurrentDomain.GetAssemblies()
                |> Array.tryFind (fun a -> try a.FullName = name || a.GetName().Name = name with _ -> false)

        type MemberInfo with
            member m.IsReflectedDefinition =
                let rec traverse (m : MemberInfo) =
                    if m.GetCustomAttributes(typeof<ReflectedDefinitionAttribute>, false).Length <> 0 then true
                    else
                        match m.DeclaringType with
                        | null -> false
                        | t when Microsoft.FSharp.Reflection.FSharpType.IsModule t -> traverse t
                        | _ -> false

                traverse m

    [<RequireQualifiedAccess>]
    module Expr =

        /// erases reflected type information from expression
        let erase (e : Expr) =
            match e with
            | ShapeVar v -> Expr.Var v
            | ShapeLambda (v, body) -> Expr.Lambda(v, body)
            | ShapeCombination (o, exprs) -> RebuildShapeCombination(o, exprs)

        /// Define a unique variable name
        let var<'T> =
            let t = typeof<'T>
            let id = sprintf' "%A:%A" t <| Guid.NewGuid()
            new Var(id, t)
    
        /// recursively substitutes the branches of a quotation based on given rule
        let rec substitute patchF expr = 
            match defaultArg (patchF expr) expr with
            | ExprShape.ShapeVar(v) -> Expr.Var(v)
            | ExprShape.ShapeLambda(v, body) -> Expr.Lambda(v, substitute patchF body)
            | ExprShape.ShapeCombination(a, args) -> 
                let args' = List.map (substitute patchF) args
                ExprShape.RebuildShapeCombination(a, args')

        /// iterates through a quotation
        let rec iter (iterF : Expr -> unit) expr =
            do iterF expr
            match expr with
            | ExprShape.ShapeVar _ -> ()
            | ExprShape.ShapeLambda(v, body) -> do iterF (Expr.Var v) ; iter iterF body
            | ExprShape.ShapeCombination(_, exprs) -> List.iter (iter iterF) exprs

        let rec fold (foldF : 'State -> Expr -> 'State) state expr =
            let state' = foldF state expr
            let children =
                match expr with
                | ShapeVar _ -> []
                | ShapeLambda(v, body) -> [Expr.Var v ; body]
                | ShapeCombination(_, exprs) -> exprs
        
            List.fold (fold foldF) state' children

        /// gathers all reflected definitions used within given expression tree
        let getReflectedDefinitions (expr : Expr) =
            
            let gathered = new Dictionary<Choice<MethodInfo, PropertyInfo>, Expr> ()

            let tryGetReflectedDefinition (id : Choice<MethodInfo, PropertyInfo>) =
                if gathered.ContainsKey id then []
                else
                    let meth = 
                        match id with 
                        | Choice1Of2 m -> m 
                        | Choice2Of2 p -> p.GetGetMethod(true)

                    match Expr.TryGetReflectedDefinition meth with
                    | None -> []
                    | Some e -> gathered.Add(id, e) ; [e]

            let rec traverse (stack : Expr list) =
                // identify new reflected definitions and expand
                let newExprs =
                    match stack with
                    | Call(_, m, _) :: _ -> tryGetReflectedDefinition <| Choice1Of2 m
                    | PropertyGet(_, p, _) :: _
                    | PropertySet(_, p, _, _) :: _ -> tryGetReflectedDefinition <| Choice2Of2 p
                    | _ -> []

                // push newly discovered reflected definitions onto the evaluation stack
                match stack with
                | ShapeVar _ :: rest -> traverse <| newExprs @ rest
                | ShapeLambda(_, body) :: rest -> traverse <| body :: newExprs @ rest
                | ShapeCombination(_, exprs) :: rest -> traverse <| exprs @ newExprs @ rest
                | [] -> ()

            do traverse [expr]

            gathered 
            |> Seq.map (function (KeyValue(k,v)) -> (k,v))
            |> Seq.toList