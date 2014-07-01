namespace Nessos.MBrace.Runtime

    open System
    open System.Reflection

    open Nessos.Vagrant

    open Nessos.MBrace.Utils

    module internal AssemblyFilter =

        let private mbraceAssemblies = lazy(
            let this = Assembly.GetExecutingAssembly()
            let assemblies = VagrantUtils.ComputeAssemblyDependencies this
            hset assemblies)

        let isMBraceAssembly (a : Assembly) = mbraceAssemblies.Value.Contains a