namespace Nessos.MBrace.Runtime

    open System
    open System.IO

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime.Logging

    type private RuntimeConfiguration =
        {
            InstanceId : Guid

            WorkingDirectory : string
            LocalCacheStoreDirectory : string
            AssemblyCacheDirectory : string

            Logger : ISystemLogger
        }

    type RuntimeSettings private () =
        