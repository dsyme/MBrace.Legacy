namespace Nessos.MBrace.Runtime

    open System
    open System.Diagnostics
    open System.Net

    open Nessos.MBrace.Client
    open Nessos.MBrace.Utils
    open Nessos.Thespian
//    open Nessos.MBrace.Actors.PowerPack


    module Defaults =
        let ClientEndPoint = IPEndPoint(IPAddress.Any, 0)
        let RuntimeDefaultPort = 2675 //2675 is the integer value of the string: "M-Brace Runtime default port."
//        let RuntimeDefaultInternalPort = 3568 //3568 is the integer value of the string "M-Brace Runtime default inernal port."
        let DefaultPermissions = Nessos.MBrace.Runtime.CommonAPI.Permissions.All
        let MaxPid = 10000
        
        let MBracedIpcServerName = "ipcNodeInfo"
        let MBracedIpcSpawningReceiverName (childPid : int) = sprintf' "spawningReceiver-%d" childPid

    module Utils =

        let stringToProcessId (txt : string) = int txt //Guid txt : ProcessId
        let PidSlots = Defaults.MaxPid - 2 // all values permitted except 0 and 1

        let genNextPid =
            let rng = new System.Random()
            let generate (pred : ProcessId -> bool) =
                // generate seed
                let init = rng.Next(2, Defaults.MaxPid - 1)
                let rec search candidate =
                    if pred candidate then candidate
                    else search <| max ((candidate + 1) % Defaults.MaxPid) 2

                search init

            generate              

        let setPermissions permissions (node : NodeRef) = 
            node <-- SetNodePermissions permissions

        let permitted (mode : Permissions) (node : NodeRef) =
            try (node <!= GetNodeDeploymentInfo).Permissions.HasFlag mode with _ -> false

        let switchPermissionFlag (on : bool) flag (state : Permissions) (node : NodeRef) =
            let state' =
                match on with
                | true ->  flag ||| state  
                | false -> (~~~ flag) &&& state

            setPermissions state' node
        
        let isAlive (node : NodeRef) =
            try
                node <!= fun ch -> Ping(ch, true)
                true
            with _ -> false

        //TODO!!! Change to use a TryPostWithReply variant
        //to avoid catching exception in order to return false
        // nope: TryPostWithReply is not exception safe; returns 'None' only in case of timeout,
        // for example, DNS resolution errors are not caught
        let isActive (node : NodeRef) =
            try
                let info = node <!= GetNodeDeploymentInfo
                info.State <> Idle
            with _ -> false

        let isActiveRuntime (runtime : ActorRef<ClientRuntimeProxy>) =
            try
                let info = runtime <!= (RemoteMsg << GetNodeDeploymentInfo)
                info.State <> Idle
            with _ -> false

        let hostId = HostId.Local
        let mkNodeDeploymentInfo (permissions : Permissions) (state : NodeType) =
            {
                HostId = hostId
                DeploymentId = processUUID
                Pid = selfProc.Id
                Permissions = permissions
                State = state
            }

        let getNodeInfo (node : NodeRef) =
            let info = node <!= GetNodeDeploymentInfo
            let proc =
                if info.HostId = hostId then
                    try Some <| Process.GetProcessById info.Pid
                    with _ -> None
                else None

            info, proc

        let hostPortToUri (host : string, port : int) =
            UriBuilder("mbrace", host, port).Uri

        //do not use this to get the uri of an actorRef
        let endPointToMbraceUri (endPoint: IPEndPoint): Uri =
            let hostEntry = Dns.GetHostEntry(endPoint.Address)
            UriBuilder("mbrace", hostEntry.HostName, endPoint.Port).Uri


        let (|RuntimeReply|_|) (msg : Runtime) =
            match msg with
            | MasterBoot(r, _) -> r :> IReplyChannel |> Some 
            | GetProcessManager r -> r :> IReplyChannel |> Some
            | GetMasterAndAlts r -> r :> IReplyChannel |> Some
            | GetAllNodes r -> r :> IReplyChannel |> Some
            | Attach(r, _) -> r :> IReplyChannel |> Some
            | Detach r -> r :> IReplyChannel |> Some
            | Ping(r, _) -> r :> IReplyChannel |> Some
            | GetNodeDeploymentInfo r -> r :> IReplyChannel |> Some
            | GetDeploymentId r -> r :> IReplyChannel |> Some
            | _ -> None

        let (|RuntimeProxyReply|_|) (msg : ClientRuntimeProxy) =
            match msg with
            | GetLastRecordedState rc -> rc :> IReplyChannel |> Some
            | RemoteMsg (RuntimeReply rc) -> Some rc
            | _ -> None

        let tryReply msg reply = match msg with RuntimeProxyReply rc -> rc.ReplyUntyped reply | _ -> ()

        let (|ProcessManagerReply|_|) (msg : ProcessManager) =
            match msg with
            | CreateDynamicProcess (r,_,_) -> r :> IReplyChannel |> Some
            | GetProcessInfo (r,_) -> r :> IReplyChannel |> Some
            | GetAllProcessInfo r -> r :> IReplyChannel |> Some
            | KillProcess (r,_) -> r :> IReplyChannel |> Some
            | ClearProcessInfo (r,_) -> r :> IReplyChannel |> Some
            | ClearAllProcessInfo r -> r :> IReplyChannel |> Some
            | RequestDependencies(r,_) -> r :> IReplyChannel |> Some


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

        let actorRefToMBraceUri (n : NodeRef) = ActorRef.toUri n |> actorUriToMbraceUri