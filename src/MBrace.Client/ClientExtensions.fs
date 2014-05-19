namespace Nessos.MBrace.Client

open Microsoft.FSharp.Quotations

open Nessos.MBrace

[<AutoOpen>]
module ClientExtensions =
    type MBrace =
        static member internal Compile (expr : Expr<ICloud<'R>>, ?name) = CloudComputation<_>(expr, ?name = name)
//        static member internal Compile (f : Expr<'I -> ICloud<'R>>, ?name) = CloudComputation<_>.Compile(f, ?name = name)
//        static member internal Compile (f : Expr<'I1 -> 'I2 -> ICloud<'R>>, ?name) = CloudComputation<_>.Compile(f, ?name = name)
//        static member internal Compile (f : Expr<'I1 -> 'I2 -> 'I3 -> ICloud<'R>>, ?name) = CloudComputation<_>.Compile(f, ?name = name)

        static member internal RunRemoteTask (runtime: MBraceRuntime) (expr : Expr<ICloud<'T>>) =
            let computation = MBrace.Compile expr
            Async.StartAsTask (runtime.RunAsync computation)

        /// Runs a computation at the given runtime.
        static member RunRemote (runtime: MBraceRuntime) (expr : Expr<ICloud<'T>>) : 'T =
            runtime.Run expr

        /// Creates a new process at the given runtime.
        static member CreateProcess (runtime : MBraceRuntime) (expr : Expr<ICloud<'T>>) : Process<'T> =
            runtime.CreateProcess expr

        /// Runs the given computation locally without the need of a runtime.
        static member RunLocalAsync (computation : ICloud<'T>) : Async<'T> =
            // force exception to be raised if no store provider has been set
            MBraceSettings.StoreProvider |> ignore

            let logger =
                {
                    new Nessos.MBrace.Core.ICloudLogger with
                        member __.LogTraceInfo _ = ()
                        member __.LogUserInfo _ = ()
                }

            Nessos.MBrace.Core.Interpreter.evaluateLocalWrapped MBraceSettings.DefaultCoreConfiguration logger false computation

        /// Runs the given computation locally without the need of a runtime.
        static member RunLocal (computation : ICloud<'T>) : 'T = 
            computation |> MBrace.RunLocalAsync |> Async.RunSynchronously 


//#if DEBUG
//module Debugging =
//
//    open Nessos.MBrace.Utils
//
//    let RequestCompilation() = Shell.Compile ()
//
//    let serialize (x : 'T) = Nessos.MBrace.Runtime.Serializer.Serialize x
//    let deserialize<'T> (data : byte []) = Nessos.MBrace.Runtime.Serializer.Deserialize<'T> data
//
//    let showDependencies() =
//        match Shell.Settings with
//        | Some conf ->
//            let (!) (d : DeclarationId) = d.Path + "+" + d.Name
//            let deps = conf.DeclarationIndex.Value |> Map.toList |> List.sortBy (fun (_,d) -> d.Id.Name)
//            deps |> List.map (fun (_,d) -> ! d.Id, d.Dependencies |> List.map (fun i -> i.Id))
//        | None -> []
//
//    let dependsOnClient (expr : Expr) =
//        match expr with
//        | Quotations.MethodOrProperty r -> QuotationAnalysis.declarationDependsOnClientMethod r
//        | _ -> failwith "not proper expr"
//
//#endif