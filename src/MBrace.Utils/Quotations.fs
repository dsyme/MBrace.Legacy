module Nessos.MBrace.Utils.Quotations

    open System
    open System.Reflection

    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Microsoft.FSharp.Quotations.ExprShape

    open Nessos.MBrace.Utils.Reflection

    [<RequireQualifiedAccess>]
    module Expr =

        let cast<'T> e = Expr.Cast<'T> e

        /// erase reflected type from quotation
        let erase (e : Expr) =
            match e with
            | ShapeVar v -> Expr.Var v
            | ShapeLambda (v, body) -> Expr.Lambda(v, body)
            | ShapeCombination (o, exprs) -> RebuildShapeCombination(o, exprs)

        let var<'T> =
            let t = typeof<'T>
            let id = sprintf' "%A:%A" t <| Guid.NewGuid()
            new Var(id, t)

        // Curries expr, which must take arguments in the order given by var list
        // with values given by args
        let curry (vars : Var list, expr : Expr) (args : (int * Expr) list) =

            let appMap = Map.ofList args

            let varOrValue = vars |> List.mapi (fun i v -> match appMap.TryFind i with None -> Choice1Of2 v | Some p -> Choice2Of2 p)
            let vars' = varOrValue |> List.choose (function Choice1Of2 p -> Some p | _ -> None)
            let args' = varOrValue |> List.map (function Choice1Of2 p -> [ Expr.Var p ] | Choice2Of2 e -> [e] )

            let body = Expr.Applications(expr, args')

            let mkLambda expr var = Expr.Lambda(var,expr)

            vars', List.fold mkLambda body vars'
    
        /// recursively substitutes the branches of a quotation based on given rule
        let rec substitute patchF expr = 
            match defaultArg (patchF expr) expr with
            | ExprShape.ShapeVar(v) -> Expr.Var(v)
            | ExprShape.ShapeLambda(v, body) -> Expr.Lambda(v, substitute patchF body)
            | ExprShape.ShapeCombination(a, args) -> 
                let args' = List.map (substitute patchF) args
                ExprShape.RebuildShapeCombination(a, args')

        let rec iter (iterF : Expr -> unit) expr =
            do iterF expr
            match expr with
            | ExprShape.ShapeVar _ -> ()
            | ExprShape.ShapeLambda(v, body) -> do iterF (Expr.Var v) ; iter iterF body
            | ExprShape.ShapeCombination(_, exprs) -> List.iter (iter iterF) exprs
        
        let rec foldSeq (yieldF : Expr -> 'T seq) expr =
            seq {
                yield! yieldF expr

                match expr with
                | ShapeVar _ -> ()
                | ShapeLambda (v, body) ->
                    yield! foldSeq yieldF (Expr.Var v)
                    yield! foldSeq yieldF body
                | ShapeCombination (_, exprs) -> 
                    for e in exprs do yield! foldSeq yieldF e
            }

        let rec fold (foldF : 'State -> Expr -> 'State) state expr =
            let state' = foldF state expr
            let children =
                match expr with
                | ShapeVar _ -> []
                | ShapeLambda(v, body) -> [Expr.Var v ; body]
                | ShapeCombination(_, exprs) -> exprs
        
            List.fold (fold foldF) state' children



    //
    //  Quotation and QuotationsTree types: constructs metadata out of a quotation and nested reflected definition methods
    //

    type Quotation = { Expr : Expr ; ReflectedMethod : MethodOrProperty option }
    with
        member __.Name = __.ReflectedMethod |> Option.map (fun m -> m.Name)

    and QuotationsTree = QBranch of Quotation * QuotationsTree list

    and MethodOrProperty = 
        | PropertyGetter of PropertyInfo 
        | MethodCall of MethodInfo
    with
        member self.MemberInfo = match self with PropertyGetter p -> p :> MemberInfo | MethodCall m -> m :> MemberInfo
        member self.MethodInfo = match self with PropertyGetter p -> p.GetGetMethod(true) | MethodCall m -> m
        member self.ReturnType = match self with PropertyGetter p -> p.PropertyType | MethodCall m -> m.ReturnType
        member self.DeclaringType = self.MemberInfo.DeclaringType
        member self.Name = self.MemberInfo.Name
        member self.ModuleName = rootModuleOrNamespace self.DeclaringType
        
        member self.IsProperty = match self with PropertyGetter _ -> true | _ -> false
        member self.IsMethod   = match self with MethodCall _ -> true | _ -> false
        member self.HasAttribute<'T when 'T :> Attribute> () =
            self.MemberInfo.GetCustomAttributes<'T> () |> Seq.isEmpty |> not
        
        member self.IsReflectedDefinition = 
            if self.MemberInfo.GetCustomAttributes(typeof<ReflectedDefinitionAttribute>,false).Length <> 0 then true
            else
                let rec traverse (t : Type) =
                    if t.GetCustomAttributes(typeof<ReflectedDefinitionAttribute>,false).Length <> 0 then true
                    elif t.DeclaringType <> null then traverse t.DeclaringType
                    else false

                traverse self.DeclaringType

        member self.TryGetReflectedDefinition() = Expr.TryGetReflectedDefinition self.MethodInfo


    let (|MethodOrProperty|_|) =
        function
        | PropertyGet(_,p,_) -> PropertyGetter p |> Some
        | Call(_,m,_) -> MethodCall m |> Some
        | _ -> None

    let (|ReflectedMoP|_|) (meth : MethodOrProperty) =
        if meth.IsReflectedDefinition then Some meth else None

    let rec (|LambdaMoP|_|) =
        function
        | MethodOrProperty m -> Some m
        | ShapeLambda(_,expr) -> (|LambdaMoP|_|) expr
        | _ -> None


    [<RequireQualifiedAccess>]
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module QuotationsTree =
        /// <summary>build a tree of reflected definition calls based on original expr</summary>
        /// <param name="traverseP">this predicate decides whether given reflected method should be further traversed.</param>
        /// <param name="expr">the original quotation.</param>
        let create (traverseP : MethodOrProperty -> bool) (expr : Expr) =
            let traversed = new System.Collections.Generic.HashSet<MethodOrProperty> ()

            let rec tryEvaluateMethod (m : MethodOrProperty) : QuotationsTree option =
                match m.TryGetReflectedDefinition() with
                | None -> None
                | Some expr ->
                    let quotation = { Expr = expr ; ReflectedMethod = Some m }
                    let children = traverseQuotation expr
                    Some <| QBranch (quotation, children)

            and traverseQuotation (e : Expr) : QuotationsTree list =
                let traverseF (e : Expr) =
                    seq {
                        match e with
                        | MethodOrProperty m when not <| traversed.Contains m && traverseP m ->
                            traversed.Add m |> ignore

                            match tryEvaluateMethod m with
                            | None -> ()
                            | Some q -> yield q
                        | _ -> ()
                    }

                Expr.foldSeq traverseF e |> Seq.toList

            let quotation = { Expr = expr ; ReflectedMethod = None }
            let children = traverseQuotation expr
            QBranch(quotation, children)

        /// iterates through a quotation tree
        let rec iter (f : Quotation -> unit) (QBranch(quotation,children)) =
            do f quotation
            List.iter (iter f) children