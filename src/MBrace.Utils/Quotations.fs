module Nessos.MBrace.Utils.Quotations

    open System
    open System.Collections.Generic
    open System.Reflection

    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Microsoft.FSharp.Quotations.ExprShape

    open Nessos.MBrace.Utils.Reflection


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


//        [<ReflectedDefinition>]
//        let rec odd x = if x = 0 then true else even (x-1)
//        and [<ReflectedDefinition>] even x = if x = 0 then false else odd (x-1)
//
//        gatherReflectedDefinitions <@ if true then odd 12 else false @>
//        let parseCustomAttributes


//    type FunctionInfo =
//        {
//            Source : Choice<MethodInfo, PropertyInfo>
//            Metadata : ExprMetadata
//            Expr : Expr 
//        }
//    with
//        member qfi.Method =
//            match qfi.Source with
//            | Choice1Of2 m -> m
//            | Choice2Of2 p -> p.GetGetMethod(true)
            


//    //
//    //  Quotation and QuotationsTree types: constructs metadata out of a quotation and nested reflected definition methods
//    //
//
//    type Quotation = { Expr : Expr ; ReflectedMethod : MethodOrProperty option }
//    with
//        member __.Name = __.ReflectedMethod |> Option.map (fun m -> m.Name)
//
//    and QuotationsTree = QBranch of Quotation * QuotationsTree list
//
//    and MethodOrProperty = 
//        | PropertyGetter of PropertyInfo 
//        | MethodCall of MethodInfo
//    with
//        member self.MemberInfo = match self with PropertyGetter p -> p :> MemberInfo | MethodCall m -> m :> MemberInfo
//        member self.MethodInfo = match self with PropertyGetter p -> p.GetGetMethod(true) | MethodCall m -> m
//        member self.ReturnType = match self with PropertyGetter p -> p.PropertyType | MethodCall m -> m.ReturnType
//        member self.DeclaringType = self.MemberInfo.DeclaringType
//        member self.Name = self.MemberInfo.Name
//        member self.ModuleName = rootModuleOrNamespace self.DeclaringType
//        
//        member self.IsProperty = match self with PropertyGetter _ -> true | _ -> false
//        member self.IsMethod   = match self with MethodCall _ -> true | _ -> false
//        member self.HasAttribute<'T when 'T :> Attribute> () =
//            self.MemberInfo.GetCustomAttributes<'T> () |> Seq.isEmpty |> not
//        
//        member self.IsReflectedDefinition = 
//            if self.MemberInfo.GetCustomAttributes(typeof<ReflectedDefinitionAttribute>,false).Length <> 0 then true
//            else
//                let rec traverse (t : Type) =
//                    if t.GetCustomAttributes(typeof<ReflectedDefinitionAttribute>,false).Length <> 0 then true
//                    elif t.DeclaringType <> null then traverse t.DeclaringType
//                    else false
//
//                traverse self.DeclaringType
//
//        member self.TryGetReflectedDefinition() = Expr.TryGetReflectedDefinition self.MethodInfo
//
//
//    let (|MethodOrProperty|_|) =
//        function
//        | PropertyGet(_,p,_) -> PropertyGetter p |> Some
//        | Call(_,m,_) -> MethodCall m |> Some
//        | _ -> None
//
//    let (|ReflectedMoP|_|) (meth : MethodOrProperty) =
//        if meth.IsReflectedDefinition then Some meth else None
//
//    let rec (|LambdaMoP|_|) =
//        function
//        | MethodOrProperty m -> Some m
//        | ShapeLambda(_,expr) -> (|LambdaMoP|_|) expr
//        | _ -> None
//
//
//    [<RequireQualifiedAccess>]
//    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
//    module QuotationsTree =
//        /// <summary>build a tree of reflected definition calls based on original expr</summary>
//        /// <param name="traverseP">this predicate decides whether given reflected method should be further traversed.</param>
//        /// <param name="expr">the original quotation.</param>
//        let create (traverseP : MethodOrProperty -> bool) (expr : Expr) =
//            let traversed = new System.Collections.Generic.HashSet<MethodOrProperty> ()
//
//            let rec tryEvaluateMethod (m : MethodOrProperty) : QuotationsTree option =
//                match m.TryGetReflectedDefinition() with
//                | None -> None
//                | Some expr ->
//                    let quotation = { Expr = expr ; ReflectedMethod = Some m }
//                    let children = traverseQuotation expr
//                    Some <| QBranch (quotation, children)
//
//            and traverseQuotation (e : Expr) : QuotationsTree list =
//                let traverseF (e : Expr) =
//                    seq {
//                        match e with
//                        | MethodOrProperty m when not <| traversed.Contains m && traverseP m ->
//                            traversed.Add m |> ignore
//
//                            match tryEvaluateMethod m with
//                            | None -> ()
//                            | Some q -> yield q
//                        | _ -> ()
//                    }
//
//                Expr.foldSeq traverseF e |> Seq.toList
//
//            let quotation = { Expr = expr ; ReflectedMethod = None }
//            let children = traverseQuotation expr
//            QBranch(quotation, children)
//
//        /// iterates through a quotation tree
//        let rec iter (f : Quotation -> unit) (QBranch(quotation,children)) =
//            do f quotation
//            List.iter (iter f) children