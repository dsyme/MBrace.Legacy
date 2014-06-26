namespace Nessos.MBrace.Runtime

    open System
    open System.Reflection
    open System.Collections.Generic

    open Nessos.Vagrant

    open Nessos.MBrace.Utils

    module internal AssemblyFilter =
        
        let getMBraceAssemblies () =
            let this = Assembly.GetExecutingAssembly()
            let assemblies = VagrantUtils.ComputeAssemblyDependencies this
            hset assemblies

        let private mbraceAssemblies = lazy(getMBraceAssemblies())
        let isMBraceAssembly (a : Assembly) = mbraceAssemblies.Value.Contains a