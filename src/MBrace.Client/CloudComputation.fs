namespace Nessos.MBrace.Client

    open System.Reflection

    open Microsoft.FSharp.Quotations

    open Nessos.Vagrant

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.MBraceException

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.AssemblyCache
    open Nessos.MBrace.Utils.Quotations

    open Nessos.MBrace.Client.QuotationAnalysis

    module internal CloudComputationUtils =
    
        let report (warnings : CloudWarning list) = warnings |> List.iter (printf "%O")

        let tryGetLambda =
            function
            | LambdaMoP m -> m.Name |> Some
            | _ -> None

    type CloudComputation<'T> (expr : Expr<ICloud<'T>>, ?name) =
        let warnings, errors, dependencies = clientSideCompile MBraceSettings.ClientSideExpressionCheck expr

        let name = List.pick id [ name ; CloudComputationUtils.tryGetLambda expr ; Some "" ]

        member __.Value = expr
        member __.Name = name
        member internal __.Warnings = warnings
        member internal __.Errors = errors
        member internal __.Dependencies = dependencies

        member internal __.Image =
            {
                ClientId = MBraceSettings.ClientId
                Name = name
                Computation = Serializer.Pickler.Pickle expr
                Type = Serializer.Pickler.Pickle expr.Type
                TypeName = Reflection.prettyPrint typeof<'T>
                Dependencies = dependencies |> List.map VagrantUtils.ComputeAssemblyId
            }


////        type ProcessImage = 
//            {
//                Name : string
//                Computation : byte [] // serialized QuotationPackage
//                Type : byte []  // serialized System.Type
//                TypeName : string
//                ClientId : Guid
//                Dependencies : AssemblyId []
//            }
//    type internal CloudComputation<'T> (expr : Expr<ICloud<'T>>, dependencies : Assembly list, warnings : CloudWarning list, ?name : string) =
//        let name = defaultArg name ""
//
//        do CloudComputationUtils.report warnings
//
//        let exprImg = Serializer.Serialize <| CloudPackage.Create expr
//        let typeImg = Serializer.Serialize typeof<'T>
//        let typeName = Reflection.prettyPrint typeof<'T>
//
//        let buildPackets (missing : AssemblyId []) =
//            let set = Set.ofSeq missing
//
//            let packets = 
//                dependencies
//                |> Array.map 
//                    (fun assembly ->
//                        if set.Contains assembly.Header then assembly.ImagePacket
//                        else assembly.HashPacket)
//
//            { 
//                Name = name
//                Computation = exprImg
//                Type = typeImg
//                TypeName = typeName
//                ClientId = MBraceSettings.ClientId
//                Assemblies = packets 
//            }
//
//        member __.Type = typeof<'T>
//        member __.Warnings = warnings
//        member __.Name = name
//
//        member internal __.GetHashBundle () = buildPackets [||]
//        member internal __.GetMissingImageBundle (missing : AssemblyId []) = buildPackets missing
//
//        static member Compile (expr : Expr<ICloud<'T>>, ?name : string) =
//            try
//                let name = List.pick id [ name ; CloudComputationUtils.tryGetLambda expr ; Some "" ]
//
//                let dependencies, warnings = clientSideCompile expr
//
//                CloudComputation(expr, dependencies, warnings, name)
//            with e -> Error.handle e
//
//        static member Compile (f : Expr<'I -> ICloud<'R>>, ?name) = try CloudFunc<'I,'R> (f, ?name = name) with e -> Error.handle e
//        static member Compile (f : Expr<'I1 -> 'I2 -> ICloud<'R>>, ?name) = try CloudFunc<'I1,'I2,'R> (f, ?name = name) with e -> Error.handle e
//        static member Compile (f : Expr<'I1 -> 'I2 -> 'I3 -> ICloud<'R>>, ?name) = try CloudFunc<'I1,'I2,'I3,'R>(f, ?name = name) with e -> Error.handle e
//
//
//    and internal CloudFunc<'I,'R>  (f : Expr<'I -> ICloud<'R>>, ?name : string) =
//        let apply (x : 'I) = Expr.Application(f, <@ x @>) |> Expr.cast<ICloud<'R>>
//
//        let name = List.pick id [ name ; CloudComputationUtils.tryGetLambda f ; Some "" ]
//
//        let dependencies, warnings = clientSideCompile (apply Unchecked.defaultof<'I>)
//    
//        member __.Invoke (x : 'I) = try CloudComputation(apply x, dependencies, warnings, name) with e -> Error.handle e
//
//        member __.ReturnType = typeof<'R>
//        member __.Name = name
//
//
//    and internal CloudFunc<'I1,'I2,'R> (f : Expr<'I1 -> 'I2 -> ICloud<'R>>, ?name : string) =
//        let apply (x : 'I1) (y : 'I2) = Expr.Applications(f, [ [ <@ x @> ] ; [ <@ y @> ] ]) |> Expr.cast<ICloud<'R>>
//    
//        let name = List.pick id [ name ; CloudComputationUtils.tryGetLambda f ; Some "" ]
//
//        let dependencies, warnings = clientSideCompile (apply Unchecked.defaultof<'I1> Unchecked.defaultof<'I2>)
//
//        member __.Invoke (x : 'I1, y : 'I2) = 
//            try CloudComputation(apply x y, dependencies, warnings, name) with e -> Error.handle e
//
//        member __.ReturnType = typeof<'R>
//        member __.Name = name
//
//    and internal CloudFunc<'I1,'I2,'I3,'R> (f : Expr<'I1 -> 'I2 -> 'I3 -> ICloud<'R>>, ?name : string) =
//        let apply (x : 'I1) (y : 'I2) (z : 'I3) = 
//            Expr.Applications(f, [ [ <@ x @> ] ; [ <@ y @> ] ; [ <@ z @> ] ] ) |> Expr.cast<ICloud<'R>>
//    
//        let name = List.pick id [ name ; CloudComputationUtils.tryGetLambda f ; Some "" ]
//
//        let dependencies, warnings = 
//            clientSideCompile (apply Unchecked.defaultof<'I1> Unchecked.defaultof<'I2> Unchecked.defaultof<'I3>)
//
//        member __.Invoke (x : 'I1, y : 'I2, z : 'I3) = 
//            try CloudComputation(apply x y z, dependencies, warnings, name)
//            with e -> Error.handle e
//
//        member __.ReturnType = typeof<'R>
//        member __.Name = name