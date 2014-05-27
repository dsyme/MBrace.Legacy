namespace Nessos.MBrace.Client

    open Nessos.Vagrant

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils.PrettyPrinters
    open Nessos.MBrace.Runtime

    type CloudComputation<'T> = Nessos.MBrace.Core.CloudComputation<'T>

    [<AutoOpen>]
    module internal CloudComputation =

        type CloudComputation with
            member cmp.GetRawImage () =
                {
                    ClientId = MBraceSettings.ClientId
                    Name = cmp.Name
                    Computation = Serialization.Serialize cmp
                    Type = Serialization.Serialize cmp.ReturnType
                    TypeName = Type.prettyPrint cmp.ReturnType
                    Dependencies = cmp.Dependencies |> List.map VagrantUtils.ComputeAssemblyId
                }