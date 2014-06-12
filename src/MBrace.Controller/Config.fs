namespace Nessos.MBrace.Runtime.Daemon.Controller

    open System
    open System.Diagnostics
    open System.Threading
    open System.IO
    open System.Xml
    open System.Xml.Linq

    open Nessos.UnionArgParser

    open Nessos.MBrace.Client
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Utils.String

    type MBraceSession = 
        { 
            Id        : System.Guid
            StartedOn : System.DateTime
            Nodes     : (Uri * Guid) list // uri * deployment id
        }
    with
        member s.ToXml =
            let xn = XName.Get
            let writeNode (uri : Uri, gId : Guid) =
                XElement(xn "Node", 
                            XAttribute(xn "Uri", uri), 
                            XAttribute(xn "DeploymentId", gId))
            XElement(xn "Session",
                XElement(xn "Id", s.Id),
                XElement(xn "StartedOn", s.StartedOn),
                s.Nodes |> List.map writeNode
            ).ToString()

        static member OfXml (text : string) =
            let xn = XName.Get
            let root = XDocument.Parse(text).Element(xn "Session")
            let getNode (x : XElement) = 
                let uri = Uri(x.Attribute(xn "Uri").Value)
                let gId = x.Attribute(xn "DeploymentId").Value |> Guid.Parse
                uri, gId
            {
                Id        = root.Element(xn "Id").Value |> System.Guid.Parse
                StartedOn = root.Element(xn "StartedOn").Value |> System.DateTime.Parse
                Nodes     = root.Elements(xn "Node") |> Seq.map getNode |> Seq.toList
            }

    type Configuration =
        | [<NoCommandLine>] Session_File of string
        | [<NoCommandLine>][<Mandatory>] MBraced_Executable of string
        | [<First>][<NoAppSettings>][<CustomCommandLine("start")>] Start
        | [<First>][<NoAppSettings>][<CustomCommandLine("stop")>] Stop
        | [<First>][<NoAppSettings>][<CustomCommandLine("status")>] Status
        | [<NoAppSettings>][<AltCommandLine("-n")>] Nodes of int
        | [<NoAppSettings>][<AltCommandLine("-w")>] Spawn_Window
        | [<NoAppSettings>][<AltCommandLine("-b")>] Boot
    with
        interface IArgParserTemplate with
            member s.Usage =
                match s with
                | Session_File _ -> "location of the session file."
                | MBraced_Executable _ -> "location of the mbraced executable."
                | Start -> "starts a new mbraced session."
                | Stop -> "stops the current mbraced session."
                | Status -> "displays information on the current mbraced session."
                | Nodes _ -> "specify number of mbraced processes to spawn."
                | Spawn_Window -> "spawns mbraced processes as popup windows."
                | Boot -> "boots processes as a standalone local runtime."



    module internal Implementations =

        let selfExe = System.Reflection.Assembly.GetExecutingAssembly().Location

        let resolveRelativePath (path : string) =
            if Path.IsPathRooted path then path
            else Path.GetFullPath <| Path.Combine(Path.GetDirectoryName selfExe, path)

        let defaultSessionFile = Path.Combine(Path.GetTempPath(), "mbrace-session.xml")

        let parseMBracedExecutable (path : string) =
            let path = resolveRelativePath path
            if File.Exists path then path
            else failwith "specified mbraced.exe location does not exist."

        let parseNodes startMode (nodes : int) =
            if not startMode then failwith "can only specify number of nodes when running in START mode."
            if nodes <= 1 then failwith "input only valid for nodes > 1."
            else nodes

        let exiter = new ConsoleProcessExiter(true) :> IExiter

        let argParser = UnionArgParser<Configuration>("USAGE: mbracectl [start|stop|status] ... options")

        let tryParseSessionFile (path : string) =
            try
                if File.Exists path then
                    let text = File.ReadAllText(path)
                    Some(MBraceSession.OfXml text)
                else None
            with e -> exiter.Exit("error: invalid or corrupt session file.", 2)

        let saveSession (path : string) (session : MBraceSession) =
            try
                let text = session.ToXml
                retry (RetryPolicy.Retry(2, 0.5<sec>)) (fun () -> File.WriteAllText(path, text))
            with e -> 
                try File.Delete path with _ -> ()
                exiter.Exit("error: could not update session file.", 2)

        let deleteSessionFile (path : string) =
            if File.Exists path then
                try retry (RetryPolicy.Retry(2, 0.5<sec>)) (fun () -> File.Delete path)
                with e ->
                    exiter.Exit("error: could not update session file.", 2)

        let tryParseSession (sessionFile : string) =
            let tryGetNode (uri : Uri, gId : Guid) =
                try 
                    let n = new MBraceNode(uri)
                    if n.DeploymentId <> gId || n.Process.IsNone then None
                    else Some n
                with _ -> None
            
            match tryParseSessionFile sessionFile with
            | None -> None
            | Some session ->
                match List.parChoose tryGetNode session.Nodes with
                | [] when session.Nodes.Length > 0 ->
                    // found a dead session file, delete
                    deleteSessionFile sessionFile ; None
                | nodes -> Some (session, nodes)

        let prettyPrint (session : MBraceSession) (nodes : MBraceNode list) =
            try
                let title = sprintf "{m}brace session started on %O." session.StartedOn
                    
                MBraceNode.PrettyPrint(nodes, title = title, useBorders = false) |> printfn "%s"
            with e -> exiter.Exit(sprintf "mbracectl: unexpected error: %s" e.Message, 5)

        let spawnNodes spawnWindows (multiNode : (int * bool) option) =
            try
                let nodes =
                    match multiNode with
                    | None -> [ MBraceNode.Spawn(background = not spawnWindows) ]
                    // (nodes * boot) option
                    | Some(n,false) -> MBraceNode.SpawnMultiple(n, background = not spawnWindows)
                    | Some(n,true) -> 
                        let r = MBraceRuntime.InitLocal(n, background = not spawnWindows)
                        r.Nodes

                let session =
                    { 
                        Id = Guid.NewGuid() ; StartedOn = DateTime.Now ; 
                        Nodes = nodes |> List.map (fun n -> n.Uri, n.DeploymentId)
                    }

                session, nodes
            with e -> exiter.Exit(sprintf "error initializing session: %s." e.Message, 2)

        let lockSession (sessionFile : string) =
            match ThreadSafe.tryClaimGlobalMutex <| "mbracectl:" + sessionFile with
            | None -> exiter.Exit("mbracectl: session appears to be locked by another process.", 2)
            | Some mtx -> exiter.ExitEvent.Add(fun _ -> mtx.Close())