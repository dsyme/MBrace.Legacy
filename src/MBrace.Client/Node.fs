namespace Nessos.MBrace.Client

open System
open System.Collections.Generic
open System.Diagnostics
open System.Net
open System.Net.Sockets
open System.Threading
open System.Text
open System.IO

open Nessos.Thespian
open Nessos.Thespian.AsyncExtensions
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.Remote.PipeProtocol
open Nessos.Thespian.PowerPack.RetryExtensions

open Nessos.MBrace.Runtime.CommonAPI
open Nessos.MBrace.Runtime.Utils
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Daemon.Configuration
open Nessos.MBrace.Utils
open Nessos.MBrace.Store
open Nessos.MBrace.Utils.String
//open Nessos.MBrace.Utils.Remoting


module Logs =
    type LogEntry = Nessos.MBrace.Utils.LogEntry
    type LogLevel = Nessos.MBrace.Utils.LogLevel

    let show (log : seq<LogEntry>) =
        log
        |> Seq.map (fun e -> e.Print(showDate = true))
        |> String.concat "\n"
        |> printfn "%s"



type MBraceNode private (nodeRef: ActorRef<Runtime>, uri: Uri) as self = 
    // NOTE : uri field denotes mbrace uri

    static do MBraceSettings.Initialize ()

    let handleError (e : exn) : 'T =
        match e with
        | MessageHandlingException(_,_,_,e) ->
            mfailwithfInner e "Node %A has replied with exception." self
        | CommunicationException _ ->
            mfailwithf "Cannot communicate with %A." self
        | :? TimeoutException -> 
            mkMBraceExn None (sprintf' "Timeout when connecting to %A." self) |> Error.handle
        | e -> Error.handle e

    let nodeInfo = CacheAtom.Create((fun () -> Utils.getNodeInfo nodeRef), interval = 1000, keepLastResultOnError = true)

    internal new (nref: ActorRef<Runtime>) =
        let uri = ActorRef.toUri nref |> MBraceUri.actorUriToMbraceUri
        MBraceNode(nref, uri)

    new (uri: Uri) =
        let nref = uri |> MBraceUri.mbraceUriToActorUri Serialization.SerializerRegistry.DefaultName |> ActorRef.fromUri
        MBraceNode(nref, uri)

    new (hostname : string, port : int) = MBraceNode(hostPortToUri(hostname, port))

    new (uri : string) = MBraceNode(new Uri(uri))

    member __.Process : Process option = snd nodeInfo.Value
    member internal __.Ref = nodeRef
    member __.Uri = uri

    member __.Permissions
        with get () : Permissions = (fst nodeInfo.Value).Permissions
        and  set (newPermissions: Permissions) =
            try
                if newPermissions < Permissions.None || newPermissions > Permissions.All then
                    mfailwithf "Invalid permissions combination."

                setPermissions newPermissions <| nodeRef
            with e -> handleError e

    member n.PermittedSlave
        with get() = try n.Permissions.HasFlag Permissions.Slave with e -> handleError e
        and  set (x: bool) = try switchPermissionFlag x Permissions.Slave n.Permissions nodeRef with e -> handleError e
    member n.PermittedMaster
        with get () = try n.Permissions.HasFlag Permissions.Slave with e -> handleError e
        and  set (x: bool) = try switchPermissionFlag x Permissions.Master n.Permissions nodeRef with e -> handleError e

    member n.State = (fst nodeInfo.Value).State
    member n.DeploymentId = (fst nodeInfo.Value).DeploymentId
    member n.IsActive = n.State <> Idle

    member __.Ping(?silent: bool, ?timeout: int) =
        try
            let silent = defaultArg silent false
            let timeout = defaultArg timeout 3000

            let timer = new Stopwatch()

            timer.Start()
            nodeRef <!== ( (fun chan -> Ping(chan,silent)) , timeout )
            timer.Stop()

            int timer.ElapsedMilliseconds
        with e -> handleError e

    member __.GetPerformanceCounters () =
        try nodeRef <!= GetNodePerformanceCounters
        with e -> handleError e

    member n.GetLogs (?clear) =
        try
            let clear = defaultArg clear false
            nodeRef <!= fun ch -> GetLogDump(ch, clear)
        with e -> handleError e

    member n.ShowLogs (?clear) =
        n.GetLogs (?clear = clear) |> Logs.show

    member n.IsLocal = n.Process.IsSome

    member n.Kill() =
        match n.Process with
        | None -> invalidOp "Node %A is not local and cannot be killed." self
        | Some proc -> proc.Kill()

    override node.ToString() = node.Uri.ToString()

    member x.CompareTo (y : obj) =
        match y with
        | :? MBraceNode as y -> compare x.Ref y.Ref
        | _ -> raise <| ArgumentException("Invalid input.", "other")

    override x.GetHashCode() = x.Ref.GetHashCode()
    override x.Equals(y) = try x.CompareTo y = 0 with _ -> false
    interface IComparable with
        member x.CompareTo y = x.CompareTo y

    static member internal SpawnAsync (arguments : MBracedConfig list, ?background) =
        async {
            let background = defaultArg background false
            let mbracedExe = MBraceSettings.MBracedExecutablePath

            if not <| File.Exists MBraceSettings.MBracedExecutablePath then
                mfailwithf "Error: invalid mbraced.exe location '%s'." mbracedExe

            try
                // start a receiver
                let receiverId = Guid.NewGuid().ToString()
                use nodeReceiver =
                    Receiver.create<MBracedSpawningServer>()
                    |> Receiver.rename receiverId
                    |> Receiver.publish [ PipeProtocol() ]
                    |> Receiver.start

                let arguments = List.filter (function Parent_Receiver_Id _ -> false | _ -> true) arguments
                let flatArgs = 
                    (Parent_Receiver_Id (selfProc.Id, receiverId) :: arguments)
                    |> mbracedParser.PrintCommandLineFlat

                let command, args =
                    if runsOnMono then
                        "mono", sprintf' "%s %s" mbracedExe flatArgs
                    else
                        mbracedExe, flatArgs
        
                let proc = new Process()

                proc.EnableRaisingEvents <- true
                proc.StartInfo.FileName <- command
                proc.StartInfo.Arguments <- args

                if background then
                    proc.StartInfo.UseShellExecute <- false
                    proc.StartInfo.CreateNoWindow <- true
                else
                    proc.StartInfo.UseShellExecute <- true

                let _ = proc.Start ()

                let! result = 
                    nodeReceiver 
                    |> Receiver.toObservable
                    |> Observable.merge (proc.Exited |> Observable.map (fun _ -> StartupError (proc.ExitCode, None)))
                    |> Async.AwaitObservable

                proc.EnableRaisingEvents <- false

                return
                    match result with
                    | StartupError (id, msg) ->
                        match msg with
                        | None -> mfailwithf "Node %d exited with error %d." proc.Id id
                        | Some msg -> mfailwithf "Node %d exited with message '%s'." proc.Id msg
                    | StartupSuccessful(uri,_) -> 
                        let nref = uri |> MBraceUri.mbraceUriToActorUri Serialization.SerializerRegistry.DefaultName |> ActorRef.fromUri
                        MBraceNode(nref, uri)
            with
            | MBraceExn e -> return! Async.Raise e
            | e -> return mfailwithInner e "Error spawning local {m}brace daemon."
        }

    static member Spawn(arguments : string [], ?background) =
        async {
            let arguments = 
                try mbracedParser.ParseCommandLine(arguments, ignoreMissing = true).GetAllResults()
                with e -> mfailwithf "Argument Parse error: %s" e.Message

            return! MBraceNode.SpawnAsync(arguments, ?background = background)
        } |> Error.handleAsync2

    static member SpawnAsync(?hostname : string, ?primaryPort : int, ?workerPorts: int list, ?logFiles : string list, ?logLevel : LogLevel,
                                ?permissions : Permissions, ?serializerName : string, ?compressSerialization : bool, ?debug : bool,
                                ?workingDirectory : string, ?useTemporaryWorkDir : bool, ?background : bool, ?storeProvider : StoreProvider) = 
        async {
            let debug = defaultArg debug false
            let useTemporaryWorkDir = defaultArg useTemporaryWorkDir false
            let workerPorts = defaultArg workerPorts []
            let logFiles = defaultArg logFiles []
//            let compressSerialization = defaultArg compressSerialization MBraceSettings.SerializerCompression
//            let serializerName = defaultArg serializerName MBraceSettings.SerializerName
            let storeProvider = List.tryPick id [ storeProvider ; MBraceSettings.TryGetStoreProvider () ]

            // build arguments
            let args =
                [
//                    yield Compress_Serialization compressSerialization
//                    yield Default_Serializer serializerName

                    match hostname with Some h -> yield Hostname h | None -> ()
                    match primaryPort with Some pp -> yield Primary_Port pp | None -> ()

                    for p in workerPorts -> Worker_Port p
                    for l in logFiles -> Log_File l

                    if debug then yield Debug
                    if useTemporaryWorkDir then yield Use_Temp_WorkDir

                    match permissions with Some p -> yield Permissions (int p) | _ -> ()
                    match workingDirectory with Some w -> yield Working_Directory w | _ -> ()
                    match logLevel with Some l -> yield Log_Level l.Value | _ -> ()
                    match storeProvider with
                    | None -> ()
                    | Some sp ->
                        yield Store_Provider sp.Name
                        yield Store_EndPoint sp.EndPoint
                ]
        
            return! MBraceNode.SpawnAsync(args, ?background = background)
        }

    static member Spawn(?hostname : string, ?primaryPort : int, ?workerPorts: int list, ?logFiles : string list, ?logLevel : LogLevel,
                                ?permissions : Permissions, ?serializerName : string, ?compressSerialization : bool, ?debug : bool,
                                ?workingDirectory : string, ?useTemporaryWorkDir : bool, ?background : bool, ?storeProvider : StoreProvider) =
        MBraceNode.SpawnAsync(?hostname = hostname, ?primaryPort = primaryPort, ?workerPorts = workerPorts, ?logFiles = logFiles,
                                    ?logLevel = logLevel, ?permissions = permissions, ?serializerName = serializerName, 
                                    ?compressSerialization = compressSerialization, ?debug = debug, ?workingDirectory = workingDirectory,
                                    ?useTemporaryWorkDir = useTemporaryWorkDir, ?background = background, ?storeProvider = storeProvider)
        |> Error.handleAsync2

    static member SpawnMultiple(nodeCount : int, ?workerPortsPerNode : int,  ?hostname : string, ?logFiles : string list, ?logLevel : LogLevel,
                                    ?permissions : Permissions, ?serializerName : string, ?compressSerialization : bool, ?debug : bool, 
                                    ?background : bool, ?storeProvider : StoreProvider) =
        
        let spawnSingle primary pool =
                MBraceNode.SpawnAsync(?hostname = hostname, primaryPort = primary, workerPorts = pool, ?logFiles = logFiles,
                                        ?logLevel = logLevel, ?permissions = permissions, ?serializerName = serializerName,
                                            ?compressSerialization = compressSerialization, ?debug = debug, ?background = background,
                                                ?storeProvider = storeProvider, useTemporaryWorkDir = true)
        async {
            let workerPortsPerNode = defaultArg workerPortsPerNode 7

            if nodeCount <= 0 || nodeCount > 50 then return mfailwith "given invalid node count."
            else
                // TODO : THIS IS WRONG; CHILD SHOULD CHOOSE ITS OWN PORTS
                let n = workerPortsPerNode + 1
                let ports = getAvailableTcpPorts <| nodeCount * n |> Array.ofList

                let! nodes =
                    [0..nodeCount-1] 
                    |> Seq.map (fun i -> ports.[i * n .. (i+1) * n - 1] |> Array.toList)
                    |> Seq.map (function h :: t -> spawnSingle h t | _ -> failwith "impossible")
                    |> Async.Parallel

                return Array.toList nodes
        } |> Error.handleAsync2


    static member PrettyPrint(nodes : MBraceNode list, ?displayPerfCounters, ?header, ?useBorders) =
        NodeInfo.PrettyPrint([nodes], ?displayPerfCounters = displayPerfCounters, ?header = header, ?useBorders = useBorders)


//
// Node & Runtime information display types
//

and internal NodeInfo (nodes : seq<MBraceNode>) =

    let roleTable =
        nodes
        |> Seq.groupBy (fun node -> node.State)
        |> Seq.map (fun (role, info) -> role, List.ofSeq info)
        |> Map.ofSeq

    let getByRole key = defaultArg (roleTable.TryFind key) []

    let master = match getByRole Master with [] -> None | h :: _ -> Some h

    // node pretty printer
    static let prettyPrint =
        let template : Field<MBraceNode> list =
            [
                Field.create "Host" Left (fun n -> n.Uri.Host)
                Field.create "Port" Right (fun n -> n.Uri.Port)
                Field.create "Role" Left (fun n -> n.State)
                Field.create "Location" Left (fun n -> match n.Process with Some p -> sprintf' "Local (Pid %d)" p.Id | _ -> "Remote")
                Field.create "Connection String" Left (fun n -> n.Uri)
            ]

        String.prettyPrintTable template

    static let prettyPrintPerf =
        let template : Field<MBraceNode * NodePerformanceInfo> list =
            [
                Field.create "Host" Left (fun (n,_) -> n.Uri.Host)
                Field.create "Port" Right (fun (n,_) -> n.Uri.Port)
                Field.create "Role" Left (fun (n,_) -> n.State)
                Field.create "%Cpu" Right (fun (_,p) -> p.TotalCpuUsage)
                Field.create "%Cpu(avg)" Right (fun (_,p) -> p.TotalCpuUsageAverage)
                Field.create "Memory(MB)" Right (fun (_,p) -> p.TotalMemory)
                Field.create "%Memory" Right (fun (_,p) -> p.TotalMemoryUsage)
                Field.create "Network(ul/dl: kbps)" Right (fun (_,p) -> p.TotalNetworkUsage)
            ]

        String.prettyPrintTable template

    static member Create (nrefs : NodeRef seq) = NodeInfo (nrefs |> Seq.map (fun n -> MBraceNode n))

    static member internal PrettyPrint(nodes : MBraceNode list list, ?displayPerfCounters, ?header, ?useBorders) =
        let useBorders = defaultArg useBorders false
        let displayPerfCounter = defaultArg displayPerfCounters false

        let parMap (f : 'T -> 'S) (inputs : 'T list list) = 
            inputs 
            |> Seq.map (Seq.map (Async.lift f) >> Async.Parallel >> Async.map Array.toList) 
            |> Async.Parallel
            |> Async.RunSynchronously
            |> Array.toList

        if displayPerfCounter then
            // force lookup of nodes in parallel
            let info = nodes |> parMap (fun n -> n, n.GetPerformanceCounters())
            prettyPrintPerf header useBorders info
        else
            prettyPrint header useBorders nodes

    member __.Master = master
    member __.Slaves = getByRole Slave
    member __.Alts = getByRole Alt
    member __.Idle = getByRole Idle

    member conf.Nodes =
        [
            yield! conf.Master |> Option.toList
            yield! conf.Alts
            yield! conf.Slaves
            yield! conf.Idle
        ]

    member conf.Display(?displayPerfCounters, ?useBorders) =
        let title =
            if master.IsSome then "{m}brace runtime information (active)"
            else "{m}brace runtime information (inactive)"

        let nodes =
            [
                yield master |> Option.toList
                yield getByRole Alt 
                yield getByRole Slave
                yield getByRole Idle
            ]

        NodeInfo.PrettyPrint(nodes, ?displayPerfCounters = displayPerfCounters, header = title, ?useBorders = useBorders)



type Node = MBraceNode