namespace Nessos.MBrace.Runtime

    open System
    open System.Diagnostics
    open System.Net

    open Nessos.Thespian

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Core
    open Nessos.MBrace.Runtime.Store

    module Defaults =

        let ClientEndPoint = IPEndPoint(IPAddress.Any, 0)
        let RuntimeDefaultPort = 2675 //2675 is the integer value of the string: "M-Brace Runtime default port."
        let DefaultPermissions = Nessos.MBrace.Runtime.Permissions.All

        let MBracedIpcServerName = "ipcNodeInfo"
        let MBracedIpcSpawningReceiverName (childPid : int) = sprintf' "spawningReceiver-%d" childPid


    [<AutoOpen>]
    module Utils =

        module MBraceUri =

            open System
            open Nessos.Thespian
            open Nessos.Thespian.Remote.TcpProtocol

            open Nessos.MBrace.Runtime
            open Nessos.MBrace.Utils

            //An mbrace actor uri is of the following format
            //mbrace://hostnameOrAddress:port/service
            //where service ::= ε | actorName
            //where the above is equivalent to the following actor uri
            //btcp://hostnameOrAddress:port/*/namePart/serializer
            //where namePart ::= actorName iff service <> ε
            //                   | 'runtime' iff service = ε
            // ε is the empty string
            //the trailing / of an mbrace uri is optional
            let tryActorUriToMbraceUri (actorUri: string) : Uri option =
                match actorUri with
                | Uri.ActorUriOfString uri when uri.Protocol = "btcp" && uri.ActorName <> String.Empty -> //&& uri.Serializer = "ndcs" ->
                    Some <| UriBuilder("mbrace", uri.HostnameOrAddress, uri.Port, if uri.ActorName = "runtime" then "" else uri.ActorName).Uri
                | _ -> None

            let actorUriToMbraceUri (actorUri: string): Uri =
                match tryActorUriToMbraceUri actorUri with
                | Some uri -> uri
                | None -> invalidArg "actorUri" "Invalid tcp actor uri."

            let tryMbraceUriToActorUri (sname : string) (uri: Uri) : string option =
                let (|ServicePart|_|) = 
                    function 
                    | RegexMatch @"^/([^/]*)(/?)$" (serviceName::_) -> Some(if serviceName = String.Empty then "runtime" else serviceName) 
                    | _ -> None

                match uri.Scheme, uri.AbsolutePath with
                | "mbrace", ServicePart actorName -> 
                    Some <| sprintf' "btcp://%s:%d/*/%s/%s" uri.Host (if uri.Port = -1 then Defaults.RuntimeDefaultPort else uri.Port) actorName sname
                | _ -> None

            let mbraceUriToActorUri sname (uri: Uri) : string =
                match tryMbraceUriToActorUri sname uri with
                | Some actorUri -> actorUri
                | None -> invalidArg "Invalid mbrace actor uri." "uri"

            let actorRefToMBraceUri (n : ActorRef<MBraceNode>) = ActorRef.toUri n |> actorUriToMbraceUri

            /// converts a host,port pair into an mbrace uri scheme
            let hostPortToUri (host : string, port : int) =
                UriBuilder("mbrace", host, port).Uri

            /// converts endpoint to an mbrace uri scheme
            let endPointToMbraceUri (endPoint: IPEndPoint): Uri =
                let hostEntry = Dns.GetHostEntry(endPoint.Address)
                UriBuilder("mbrace", hostEntry.HostName, endPoint.Port).Uri



        [<RequireQualifiedAccess>]
        module ProcessId =

            [<Literal>]
            let MaxPid = 10000

            let PidSlots = MaxPid - 2 // all values permitted except 0 and 1

            let generateUniqueProcessId (isTakedPid : ProcessId -> bool) =
                let rng = new System.Random()
                let rec search candidate =
                    if isTakedPid candidate then
                        // try next candidate
                        search <| max ((candidate + 1) % MaxPid) 2
                    else
                        candidate

                // generate seed
                let init = rng.Next(2, MaxPid - 1)
                search init

        [<RequireQualifiedAccess>]
        module Permissions =

            let inline switch (on : bool) flag (state : Permissions) =
                if on then flag ||| state
                else
                    (~~~ flag) &&& state

        let private localHostId = lazy HostId.Local
        type NodeDeploymentInfo with
            
            static member CreateLocal(permissions, state, ref, includePerfMetrics : bool) =
                {
                    DeploymentId = processUUID
                    Reference = ref
                    HostId = localHostId.Value
                    ProcessId = selfProc.Id
                    Permissions = permissions
                    State = state
                    PerformanceInfo = 
                        if includePerfMetrics then
                            Some <| IoC.Resolve<PerformanceMonitor>().GetCounters()
                        else
                            None
                }

            member info.TryGetLocalProcess () =
                if info.HostId = localHostId.Value then
                    Some <| Process.GetProcessById info.ProcessId
                else
                    None

            /// specifies if node is part of a cluster
            member info.IsActive = 
                match info.State with
                | Idle -> false
                | _ -> true

            member info.Uri = MBraceUri.actorRefToMBraceUri info.Reference

        let (|RuntimeReply|_|) (msg : MBraceNode) =
            match msg with
            | MasterBoot(r,_) -> r :> IReplyChannel |> Some
            | Attach(r, _) -> r :> IReplyChannel |> Some
            | Detach r -> r :> IReplyChannel |> Some
            | Ping r -> r :> IReplyChannel |> Some
            | GetLogDump r -> r :> IReplyChannel |> Some
            | GetNodeDeploymentInfo (r,_) -> r :> IReplyChannel |> Some
            | GetStoreManager r -> r :> IReplyChannel |> Some
            | GetClusterDeploymentInfo (r,_) -> r :> IReplyChannel |> Some
            | GetInternals r -> r :> IReplyChannel |> Some
            | ResetNodeState r -> r :> IReplyChannel |> Some
            | ShutdownSync r -> r :> IReplyChannel |> Some
            | Shutdown _ -> None
            | SetNodePermissions _ -> None

//        let (|RuntimeProxyReply|_|) (msg : ClientRuntimeProxy) =
//            match msg with
//            | GetLastRecordedState rc -> rc :> IReplyChannel |> Some
//            | RemoteMsg (RuntimeReply rc) -> Some rc
//            | _ -> None
//
//        let tryReply msg reply = match msg with RuntimeProxyReply rc -> rc.ReplyUntyped reply | _ -> ()

        let (|ProcessManagerReply|) (msg : ProcessManager) =
            match msg with
            | GetAssemblyLoadInfo (r,_,_) -> r :> IReplyChannel
            | LoadAssemblies (r,_,_) -> r :> IReplyChannel
            | CreateDynamicProcess (r,_,_) -> r :> IReplyChannel
            | GetProcessInfo (r,_) -> r :> IReplyChannel
            | GetAllProcessInfo r -> r :> IReplyChannel
            | KillProcess (r,_) -> r :> IReplyChannel
            | ClearProcessInfo (r,_) -> r :> IReplyChannel
            | ClearAllProcessInfo r -> r :> IReplyChannel
            | RequestDependencies(r,_) -> r :> IReplyChannel

        /// recursively traverses through MessageHandlingExceptions, retrieving the innermost exception raised by the original actor
        let (|MessageHandlingExceptionRec|_|) (e : exn) =
            let rec aux depth (e : exn) =
                match e with
                | :? MessageHandlingException -> aux (depth + 1) e.InnerException
                | null -> None
                | e when depth = 0 -> None
                | e -> Some e

            aux 0 e

        let rec (|CommunicationExceptionRec|_|) (e : exn) =
            match e with
            | :? CommunicationException as e -> Some e
            | MessageHandlingExceptionRec e -> (|CommunicationExceptionRec|_|) e
            | _ -> None

//        let nodeUsesCompatibleStore (node : NodeRef) =
//            try (node <!= GetStoreId) = StoreRegistry.DefaultStoreInfo.Id
//            with _ -> false
//
//        let runtimeUsesCompatibleStore (runtime : ActorRef<ClientRuntimeProxy>) =
//            try runtime <!= (RemoteMsg << GetStoreId) = StoreRegistry.DefaultStoreInfo.Id
//            with _ -> false