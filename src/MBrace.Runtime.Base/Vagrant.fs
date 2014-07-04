namespace Nessos.MBrace.Runtime

    open System
    open System.Reflection

    open Nessos.Vagrant

    open Nessos.MBrace.Utils

    type VagrantRegistry private () =
        static let container = ref None

        static let mbraceAssemblies = lazy(
            let this = Assembly.GetExecutingAssembly()
            let dependencies = VagrantUtils.ComputeAssemblyDependencies(this)
            hset dependencies)

        static let isMbraceAssembly a = mbraceAssemblies.Value.Contains a

        static member Register(v : Vagrant) =
            lock container (fun () ->
                match container.Value with
                | None -> container := Some v
                | Some _ -> invalidOp "An instance of Vagrant has already been registered.")

        static member Instance =
            match container.Value with
            | None -> invalidOp "No instance of Vagrant has been registered."
            | Some v -> v


        static member ComputeDependencies(graph : obj) =
            VagrantRegistry.Instance.ComputeObjectDependencies(graph, permitCompilation = true)
            |> List.filter (not << isMbraceAssembly)