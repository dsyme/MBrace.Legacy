namespace Nessos.MBrace.Client

    open System
    open System.IO
    open System.Collections
    open System.Reflection
    open System.Runtime.Serialization
    open System.Text.RegularExpressions

    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.DerivedPatterns

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.CloudUtils
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.AssemblyCache
    open Nessos.MBrace.Utils.String
    open Nessos.MBrace.Utils.Reflection
    open Nessos.MBrace.Utils.Quotations


    type internal CloudError = Error of string list
    with
        override e.ToString() =
            let (Error es) = e
            es |> Seq.map (fun e -> " " + e) |> String.concat "\n"

    and internal CloudWarning = Warning of string list
    with
        override w.ToString() = 
            match w with
            | Warning [] -> "Warning"
            | Warning (h :: tl) ->
                string {
                    yield "Warning: "
                    yield h
                    yield "\n"
                    for t in tl do
                        yield "         "
                        yield t
                        yield "\n"
                } |> String.build


    /// Disable warnings for specified cloud block
    type NoWarnAttribute () = inherit Attribute ()


    module internal QuotationAnalysis =

//        let shellAssembly = lazy(
//            match Shell.Settings with
//            | Some conf ->
//                AppDomain.CurrentDomain.GetAssemblies()
//                |> Array.tryFind (fun t -> t.IsDynamic && t.GetName().Name = conf.ShellAssemblyName)
//            | None -> None)
//
//        let thisAssembly = lazy(Assembly.GetExecutingAssembly())
//
//        //
//        // Quotation dependency analysis
//        //
//
//        // filter public keys used by assemblies in the {m}brace projects
//        type internal Marker = class end
//        let isMBraceAssemblyKind, isMBraceAssemblyName =
//            let mbracePublicKeyTokens =
//                let tryGetPKT (t : Type) = match t.Assembly.GetName().GetPublicKeyToken() with [||] -> None | pkt -> Some pkt
//                // microsoft key ; fsharp key ; mbrace key
//                [ typeof<int option> ; typeof<System.String> ; typeof<Marker> ]
//                |> List.choose tryGetPKT |> set
//
//            let isMBraceAssemblyName (a : AssemblyName) = mbracePublicKeyTokens.Contains <| a.GetPublicKeyToken()
//
//            let isMBraceAssemblyKind = 
//                function 
//                | StaticAssembly a -> isMBraceAssemblyName <| a.GetName()
//                | _ -> false
//
//            isMBraceAssemblyKind, isMBraceAssemblyName
//
//        let analyzeQuotationDependencies expr =
//        
//            let getTypesInQuotation (expr : Expr) =
//                let gatherF (expr : Expr) =
//                    seq {
//                        // in certain conditions, evaluating the expression type will yield an exception
//                        yield! try [expr.Type] with _ -> []
//
//                        match expr with
//                        | Value(o,_) -> if o <> null then yield o.GetType()
//                        | MethodOrProperty m -> yield m.DeclaringType
//                        | _ -> ()
//                    }
//
//                Expr.foldSeq gatherF expr |> Seq.distinct
//
//            // gathers assemblies as found in quotation tree
//            // decides whether compilation of shell interactions is required
//            let getInitialDependencies (types : Type seq) =
//                match Shell.Settings, shellAssembly.Value with
//                | Some conf, Some shellAssembly ->
//                    let compilationRequired = ref false
//                
//                    let assemblies =
//                        seq {
//                            for t in types do
//                                if t.Assembly = shellAssembly then
//                                    match conf.ModuleIndex.Value.TryFind (rootModuleOrNamespace t) with
//                                    | None -> failwith "Computation depends on types within curent interaction."
//                                    | Some fsiA ->
//                                        if not <| conf.DependencyIndex.Value.ContainsKey fsiA then
//                                            compilationRequired := true
//
//                                        yield FsiCompiled fsiA
//                                else
//                                    yield StaticAssembly t.Assembly
//                        } 
//                        |> Seq.distinct
//                        |> Seq.toArray
//
//                    compilationRequired.Value, assemblies :> _ seq
//                | _ ->
//                    false, types |> Seq.map (fun t -> StaticAssembly t.Assembly) |> Seq.distinct
//
//            // recursively traverse dependencies
//            let gatherDependencies (assemblies : AssemblyKind seq) =
//                let getFsiDependencies = 
//                    match Shell.Settings with 
//                    | Some conf -> fun x -> defaultArg (conf.DependencyIndex.Value.TryFind x) []
//                    | None -> fun _ -> []
//
//                let getStaticAssembly =
//                    fun (a : AssemblyName) ->
//                        try Assembly.Load a |> StaticAssembly
//                        with _ ->
//                            mfailwithf 
//                                "Could not locate assembly '%s'. Please consider adding an explicit reference to it." 
//                                a.Name
//    //                    match Assembly.TryFind a.Name with
//    //                    | Some asm -> StaticAssembly asm |> Some
//    //                    | None when isMBraceAssemblyName a -> None
//    //                    | None ->
//    //                        mfailwithf 
//    //                            "Could not locate assembly \"%s\". Please consider adding an explicit reference to it." 
//    //                            a.Name
//                    |> memoizeBy (fun a -> a.FullName)
//
//                let rec traverse (traversed : Set<AssemblyKind>) (remaining : Set<AssemblyKind>) =
//                    match remaining with
//                    | Set.Empty -> traversed :> _ seq
//                    | Set.Cons(dep, rest) ->
//                        if isMBraceAssemblyKind dep || traversed.Contains dep then 
//                            traverse traversed rest
//                        else
//                            let newDeps =
//                                match dep with
//                                | FsiCompiled fsiA -> getFsiDependencies fsiA :> _ seq
//                                | FsiTypeProvider a
//                                | StaticAssembly a -> 
//                                    a.GetReferencedAssemblies () |> Seq.map getStaticAssembly
//
//                            traverse (traversed.Add dep) (Set.addMany newDeps rest)
//
//                traverse Set.empty (set assemblies)
//
//
//            let types = getTypesInQuotation expr
//
//            let compilationRequired, dependencies = getInitialDependencies types
//            
//            if compilationRequired then 
//                Shell.Compile () |> ignore
//
//            gatherDependencies dependencies |> Seq.toArray

        //
        //  Quotation error reporting
        //

        // gather the external bindings found in the body of an Expr<Cloud<'T>>
        // these manifest potential closures that exist in distributed continutations
        let gatherTopLevelCloudBindings (expr : Expr) =
            let rec aux ret gathered (exprs : Expr list) =
                match exprs with
                // seq's and let bindings are traversed, ignoring their expressions
                // since they cannot be of monadic nature or caught in the environment
                | Sequential(_,e) :: rest -> aux ret gathered (e :: rest)
                | Let(v,_,cont) :: rest -> aux ret (v :: gathered) (cont :: rest)
                // return values registered as dummy variables, but done only once
                // since all return statements must be of same type
                | CloudReturn e :: rest when not ret -> aux true (new Var("return", e.Type, false) :: gathered) rest
                // gather variables from monadic bindings
                | CloudUsing(v,_,cont) :: rest -> aux ret (v :: gathered) (cont :: rest)
                | CloudBind(v,_,cont) :: rest -> aux ret (v :: gathered) (cont :: rest)
                // gather for loop index binding
                | CloudFor(_, idx, body) :: rest -> aux ret (idx :: gathered) (body :: rest)
                // remaining monadic constructs
                | CloudDelay f :: rest -> aux ret gathered (f :: rest)
                | CloudTryWith(f, h) :: rest -> aux ret gathered (f :: h :: rest)
                | CloudTryFinally(f, _) :: rest -> aux ret gathered (f :: rest)
                | CloudWhile(_,body) :: rest -> aux ret gathered (body :: rest)
                // traverse if-then-else statements
                | IfThenElse(_,a,b) :: rest -> aux ret gathered (a :: b :: rest)
                // ignore all other language constructs, cannot be of monadic nature
                | ShapeVar _ :: rest -> aux ret gathered rest
                | ShapeLambda _ :: rest -> aux ret gathered rest
                | ShapeCombination _ :: rest -> aux ret gathered rest
                // return state
                | [] -> List.rev gathered

            aux false [] [expr]

//        // creates a memoizing function that traverses for dependency that satisfies predicate
//        // None : no dependency satisfying pred
//        // Some(bool, decl) : bool is true iff decl is same as argument
//        let createDependencyLocator (pred : FsiDeclaration -> bool) =
//            let lookupDeclaration = 
//                match Shell.Settings with 
//                | Some conf -> fun id -> conf.DeclarationIndex.Value.TryFind id 
//                | None -> fun _ -> None
//
//            let seek =
//                Ymemoize (fun seek (id : DeclarationId) ->
//                    match lookupDeclaration id with
//                    | None -> None
//                    | Some declaration when pred declaration -> Some declaration
//                    | Some declaration when declaration.Type = SafeProperty || declaration.Type = DeletedProperty -> None
//                    | Some declaration -> declaration.Dependencies |> List.tryPick (function Fsi (id,_) -> seek id | _ -> None))
//
//            fun (m : MethodOrProperty) -> 
//                match shellAssembly.Value with
//                | None -> None
//                | Some shellAsmb when m.DeclaringType.Assembly <> shellAsmb -> None
//                | _ -> 
//                    let id = DeclarationId.OfMemberInfo m.MemberInfo
//                    match seek id with
//                    | None -> None
//                    | Some decl -> Some(decl.Id = id, decl)
//
//
//        // declaration contains dependency from function in this assembly
//        let declarationDependsOnClientMethod =
//            createDependencyLocator 
//                (fun decl -> 
//                    decl.Type <> SafeProperty &&
//                    decl.Dependencies |> List.exists (function External (_,a) -> a = thisAssembly.Value | _ -> false))
//
//        let declarationDependsOnUnsafeProperty =
//            createDependencyLocator (fun decl -> decl.Type = UnsafeProperty)
//
//        let declarationDependsOnErasedProperty =
//            createDependencyLocator (fun decl -> decl.Type = DeletedProperty)
//
//
//        let inCurrentInteraction =
//            match Shell.Settings, shellAssembly.Value with 
//            | Some conf, Some shellAsmb ->
//                fun (t : Type) ->
//                    t.Assembly = shellAsmb &&
//                        rootModuleOrNamespace t |> conf.ModuleIndex.Value.ContainsKey |> not
//            | _ -> fun _ -> false
                

        let checkForErrors (expr : Expr) =
            let warnings = ref Set.empty<CloudWarning>
            let errors = ref Set.empty<CloudError>

            let tokenize (x : string) = x.Split('\n') |> Array.toList
            let warnf fmt = Printf.ksprintf(fun x -> warnings := warnings.Value.Add (Warning (tokenize x))) fmt
            let errorf fmt = Printf.ksprintf(fun x -> errors := errors.Value.Add (Error (tokenize x))) fmt
        

            let analyzeQuotation (q : Quotation) =

                let qname = match q.Name with None -> "Cloud block" | Some n -> sprintf' "Cloud block '%s'" n

//                let checkType (t : Type) =
//                    if inCurrentInteraction t then 
//                        errorf "%s depends on type '%s' that was declared in this interaction; this is not permitted." qname t.Name
            
                let checkBranch (e : Expr) =
                    match e with
                    | CloudBuilderExpr body ->
                        let bindings = gatherTopLevelCloudBindings body
                        for binding in bindings do
                            if not <| Serialization.DefaultPickler.IsSerializableType binding.Type then
                                errorf "%s has binding '%s' of type '%s' that is not serializable." qname binding.Name binding.Type.Name
                    | MethodOrProperty mop ->
                        let mname = sprintf' "%s '%s'" (if mop.IsMethod then "function" else "value") mop.Name
//                        if inCurrentInteraction mop.DeclaringType then
//                            errorf "%s contains %s that was declared in this interaction; this is not permitted." qname mname
                        if mop.yieldsICloud() && not mop.IsReflectedDefinition then
                            errorf "Cloud block '%s' missing [<Cloud>] attribute." mop.Name
                        elif mop.yieldsICloud() && mop.DeclaringType <> typeof<CloudBuilder> && not mop.MethodInfo.IsStatic then
                            errorf "Cloud block '%s' is non-static; this is not supported." mop.Name
//                        else
//                            match declarationDependsOnErasedProperty mop with
//                            | Some(true, _) -> errorf "%s contains unsupported '%s'." qname mname
//                            | Some(_,d) ->
//                                errorf "%s depends on unsupported value '%s'." qname d.Name
//                            | None ->
//
//                            match declarationDependsOnClientMethod mop with
//                            | Some(_, d) -> 
//                                errorf "%s depends on {m}brace %s '%s'; this is not permitted." 
//                                    qname (if d.Type = Method then "function" else "value") d.Name
//                            // separate check if method itself is of client
//                            | None when mop.DeclaringType.Assembly = thisAssembly.Value ->
//                                errorf "%s depends on {m}brace %s '%s'; this is not permitted."
//                                    qname (if mop.IsMethod then "function" else "value") mop.Name
//                            | None ->
//
//                            if not <| mop.HasAttribute<NoWarnAttribute>() then
//                                match declarationDependsOnUnsafeProperty mop with
//                                | Some(true, _) when mop.yieldsICloud() ->
//                                    warnf "Cloud block '%s' is declared as value. Consider converting it to a function instead." mop.Name
//                                | Some(true, _) ->
//                                    warnf "%s captures '%s' in its closure. Consider converting it to a function instead." qname mname
//                                | Some(_, d) ->
//                                    warnf "%s captures '%s' in its closure. Consider converting it to a function instead." qname d.Name
//                                | _ -> ()
                        
//                            ()
//                    | Value(o,_) when o <> null -> checkType <| o.GetType()
                    | _ -> ()

//                    try checkType e.Type with _ -> ()


                Expr.iter checkBranch q.Expr

            let qtree = QuotationsTree.create (fun m -> m.yieldsICloud()) expr
            do QuotationsTree.iter analyzeQuotation qtree

            warnings.Value, errors.Value


//        // a wrapper for assembly exportation
//        type AssemblyContainer (assembly : AssemblyKind) =
//            let header = { FullName = assembly.FullName ; Hash = AssemblyCache.ComputeHash assembly.Location }
//
//            member __.Location = assembly.Location
//            member __.Header   = header
//            member __.HashPacket = { Header = header ; Image = None }
//            member __.ImagePacket = { Header = header ; Image = Some <| AssemblyImage.Create assembly.Location }

        let clientSideCompile throwOnError (expr : Expr<Cloud<'T>>) =
            let warnings, errors = checkForErrors expr

            if throwOnError && not errors.IsEmpty then
                let errors = errors |> Seq.map (fun e -> e.ToString()) |> String.concat "\n"
                mfailwithf "Supplied cloud block contains errors:\n%s" errors

            // force compilation now, if needed
            let dependencies = MBraceSettings.Vagrant.ComputeObjectDependencies(expr, permitCompilation = true)

            warnings, errors, dependencies

    



//            // warning: this compiles shell code as a side-effect
//            let dependencies = analyzeQuotationDependencies expr
//
//            if dependencies |> Array.exists (function FsiTypeProvider _ -> true | _ -> false) then
//                mfailwith "Type providers not supported."
//        
//            dependencies |> Array.map (fun a -> AssemblyContainer a), Seq.toList warnings