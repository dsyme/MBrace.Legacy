namespace Nessos.MBrace.Runtime.ProcessDomain

    open System

    open Nessos.UnionArgParser

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime

    module Configuration =

        [<NoAppSettings>]
        type WorkerConfig =
            | Debug of bool
            | Parent_Pid of int
            | Parent_Address of string
            | Process_Domain_Id of Guid
            | Assembly_Cache of string
            | HostName of string
            | Port of int
            | [<EncodeBase64>] Store_Activator of StoreActivationInfo
            | Cache_Store_Endpoint of string
        with
            interface IArgParserTemplate with
                member s.Usage =
                    match s with
                    | Debug _ -> "Enable debug mode."
                    | Parent_Pid _ -> "Pid of the parent OS process."
                    | Parent_Address _ -> "Parent process port."
                    | Process_Domain_Id _ -> "Process domain id."
                    | Assembly_Cache _ -> "Cloud process assembly dependencies."
                    | HostName _ -> "Hostname, must be the same as for parent daemon."
                    | Port _ -> "Port argument."
                    | Store_Activator _ -> "Store activation info."
                    | Cache_Store_Endpoint _ -> "Local caching path."

        let workerConfig = new UnionArgParser<WorkerConfig>("WARNING: not intended for manual use.")