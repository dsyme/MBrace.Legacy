namespace Nessos.MBrace.Client

    open Nessos.Vagrant

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils.PrettyPrinters
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Logging

    /// Represents a cloud computation that has been statically checked by the client.
    type CloudComputation<'T> = Nessos.MBrace.Core.CloudComputation<'T>

    [<AutoOpen>]
    module internal CloudComputation =

        type CloudComputation with
//            member cmp.GetRawImage () =
//                {
//                    ClientId = MBraceSettings.ClientId
//                    Name = cmp.Name
//                    Computation = Serialization.Serialize cmp
//                    Type = Serialization.Serialize cmp.ReturnType
//                    TypeName = Type.prettyPrint cmp.ReturnType
//                    Dependencies = cmp.Dependencies |> List.map VagrantUtils.ComputeAssemblyId
//                }

            static member Compile (block : Cloud<'T>, ?name : string) =
                let cc = MBraceSettings.CloudCompiler.Compile(block, ?name = name)
                for w in cc.Warnings do
                    MBraceSettings.Logger.LogWarning w
                cc

            static member Compile (expr : Quotations.Expr<Cloud<'T>>, ?name : string) =
                let cc = MBraceSettings.CloudCompiler.Compile(expr, ?name = name)
                for w in cc.Warnings do
                    MBraceSettings.Logger.LogWarning w
                cc