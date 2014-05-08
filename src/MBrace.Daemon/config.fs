namespace Nessos.MBrace.Runtime.Daemon

    module internal Configuration =

        open System
        open System.Collections.Generic
        open System.Diagnostics
        open System.IO
        open System.Net
        open System.Reflection
        open System.Threading

        open Nessos.FsPickler
        
        open Nessos.Thespian
        open Nessos.Thespian.AsyncExtensions
        open Nessos.Thespian.Remote.TcpProtocol
        open Nessos.Thespian.Remote.PipeProtocol
        open Nessos.Thespian.Serialization

        open Nessos.MBrace.Core
        open Nessos.MBrace.Utils
        open Nessos.MBrace.Utils.Retry
        open Nessos.MBrace.Utils.AssemblyCache
        open Nessos.MBrace.Utils.String
        open Nessos.MBrace.Runtime
        open Nessos.MBrace.Runtime.Store
        open Nessos.MBrace.Runtime.Daemon.Configuration
        open Nessos.MBrace.Client

        // if anyone can suggest a less hacky way, be my guest..
        // a process spawned from command line is UserInteractive but has null window handle
        // a process spawned in an autonomous window is UserInteractive and has a non-trivial window handle
        let isWindowed = Environment.UserInteractive && selfProc.MainWindowHandle <> 0n

        let thisAssembly = System.Reflection.Assembly.GetExecutingAssembly()
        let selfExe = thisAssembly.Location
        let selfInfo = thisAssembly.GetName()

        let exiter = new ConsoleProcessExiter(true) :> IExiter

        let redirectAsync (source : StreamReader) (target : TextWriter) =
            async {
                while not source.EndOfStream do
                    let! line = Async.AwaitTask <| source.ReadLineAsync()
                    do! target.WriteLineAsync(line) |> Async.AwaitIAsyncResult |> Async.Ignore
            }

        let flushConsole () = Console.Error.Flush() ; Console.Out.Flush()

        let registerLogger logFiles logLevel =
            // gather constituent loggers
            let loggers =
                // TODO : replace in-memory with cloudrefs
                IoC.Register<Logger.InMemoryLogger>(fun () -> new Logger.InMemoryLogger ())
                let imemLogger = IoC.Resolve<Logger.InMemoryLogger> () :> ILogger

                let createFileLogger (file: string) =
                    let initMsg = sprintf' ">>> M-BRACE DAEMON STARTED AT %s\n." <| DateTime.Now.ToString("yyyy-MM-dd H:mm:ss")
                    //Debug.Listeners.Add(new TextWriterTraceListener(file)) |> ignore
                    new Logger.FileLogger(file, initMsg = initMsg) :> ILogger

                let fileLoggers =
                    logFiles
                    |> Seq.distinct
                    |> Seq.choose 
                        (fun file ->
                            try Some <| createFileLogger file
                            with e -> exiter.Exit(sprintf "ERROR: cannot initialize logger: %s" e.Message, id = 5)) 
                    |> Seq.toList

                //Debug.AutoFlush <- true

                //Logger.createConsoleLogger () :: imemLogger :: fileLoggers
                Logger.createConsoleLogger () :: fileLoggers

            IoC.Register<ILogger>( 
                fun () ->
                    loggers 
                    |> Logger.propagate 
                    |> Logger.maxLogLevel logLevel
                    |> Logger.wrapAsync
            )

            //Debug.Listeners.Add(new Nessos.Thespian.Cluster.Log.LoggerTraceListener()) |> ignore

            IoC.Resolve<ILogger>()


        let parseMBraceProc (id : int) =
            let proc = Process.GetProcessById id
            if proc.ProcessName = selfProc.ProcessName then proc
            else failwith "process id is not of mbraced."

        let lockWorkingDir (path : string) =
            match ThreadSafe.tryClaimGlobalMutex <| "mbraced:" + path with
            | None -> exiter.Exit(sprintf "ERROR: working directory '%s' is in use." path, 10)
            | Some mtx -> exiter.ExitEvent.Add(fun _ -> mtx.Close())


        let getTempWD () = Path.Combine(Path.GetTempPath(), sprintf "mbraced-%d" selfProc.Id)

        // "None" denotes using a disposable tmp folder
        let registerWorkingDirectory cleanup (wd : string) =
            if cleanup then
                try
                    retry (RetryPolicy.Retry(2, 0.5<sec>)) (fun () ->
                        if Directory.Exists wd then Directory.Delete(wd, true)
                        Directory.CreateDirectory wd |> ignore)
                with e ->
                    exiter.Exit(sprintf "ERROR: cannot initialize working directory '%s'." wd, 1)
            else
                if not <| Directory.Exists wd then
                    try retry (RetryPolicy.Retry(2, 0.5<sec>)) (fun () -> Directory.CreateDirectory wd |> ignore)
                    with e ->    
                        exiter.Exit(sprintf "ERROR: cannot initialize working directory '%s'." wd, 1)

                do lockWorkingDir wd

            Directory.SetCurrentDirectory wd

            // populate subdirectories
            let create subdir registrar =
                let path = Path.Combine(wd, subdir)
                try
                    if not <| Directory.Exists path then
                        Directory.CreateDirectory path |> ignore

                    do registrar path
                with e ->
                    exiter.Exit(sprintf "ERROR: cannot initialize working directory '%s'." path)

            do create "AssemblyCache" AssemblyCache.SetCacheDir
            do create "LocalCaches" <| 
                    fun cachePath ->
                        raise <| new NotImplementedException("daemon configuration")
//                        IoC.RegisterValue(LocalCacheStore(cachePath), "cacheStore")            
//                        IoC.RegisterValue(cachePath, "cacheStoreEndpoint")

        let registerProcessDomainExecutable (path : string) =
            let path =
                if File.Exists path then path
                else
                    // do some guesswork
                    let binFolder = Assembly.GetExecutingAssembly().Location |> Path.GetDirectoryName
                    let file = Path.Combine(binFolder, path)

                    if File.Exists file then file
                    else
                        exiter.Exit("ERROR: cannot locate mbrace.worker.exe.", 11)
                
            IoC.RegisterValue(path, "MBraceProcessExe")

        let registerFancyConsoleEvent debug (address : Address) =
            Nessos.MBrace.Runtime.Definitions.MBraceNode.SubscribeToStateChange(
                fun state ->
                    if Environment.UserInteractive then
                        let consoleBgColor,role =
                            match state with
                            | NodeType.Slave 
                            | NodeType.Alt -> ConsoleColor.Blue, "Slave"
                            | NodeType.Master -> ConsoleColor.DarkMagenta, "Master"
                            | NodeType.Idle -> ConsoleColor.Black, "Idle"

                        let title =
                            string {
                                yield "mbraced"
                             
                                if debug then yield "[debug]"
                            
                                yield sprintf' "(%s Node)" role
                                yield sprintf' " - %s:%d" address.HostnameOrAddress address.Port
                    
                            } |> String.build

                        Console.Title <- title
                        if isWindowed then
                            Console.BackgroundColor <- consoleBgColor            
            )

        let registerSerializers () =
            Nessos.MBrace.Runtime.Serializer.Register(new FsPickler())


        let registerStore (storeProvider : string) (storeEndpoint : string) (workingDirectory : string) =
            try
                let storeInfo = StoreRegistry.Activate(StoreProvider.Parse (storeProvider, storeEndpoint), makeDefault = true)
                let localCacheDir = Path.Combine(workingDirectory, "LocalCache")
                let coreConfig = CoreConfiguration.Create(IoC.Resolve<ILogger>(), Serializer.Pickler, storeInfo, localCacheDir)
                // sooner...
                IoC.Register<CoreConfiguration>(fun () -> coreConfig)
                IoC.RegisterValue<IStore>(storeInfo.Store)
                //IoC.Register<ICloudRefStore>(fun () -> coreConfig.CloudRefStore) 
                //IoC.Register<IMutableCloudRefStore>(fun () -> coreConfig.MutableCloudRefStore) 
                //IoC.Register<ICloudSeqProvider>(fun () -> coreConfig.CloudSeqStore) 
                //IoC.Register<ICloudFileStore>(fun () -> coreConfig.CloudFileStore) 

                IoC.RegisterValue(storeEndpoint, "storeEndpoint")
                IoC.RegisterValue(storeProvider, "storeProvider")
            with e ->
                exiter.Exit(sprintf "Error connecting to store: %s" e.Message, 2)

        // parser implementations        

        type WorkerPorts =
            | Singular of int
            | Range of int * int

        let parsePort (p : int) =
            if p < -1 || p > int UInt16.MaxValue then failwithf "invalid port %d." p else p

        let parseWorkerPortRange (lower, upper) =
            if lower < 0 || upper < lower || upper > int UInt16.MaxValue then
                failwithf "invalid port range %d..%d." lower upper
            else Range(lower, upper)

        let parseSingularWorkerPort = parsePort >> Singular

        let evalPorts (ports : WorkerPorts list) =
            ports |> List.collect(function Singular p -> [p] | Range (l,u) -> [l..u])

        let printWorkerPorts (ports : WorkerPorts list) =
            string {
                yield "Worker port pool: "

                let printPort (p : WorkerPorts) =
                    string {
                        match p with
                        | Singular p -> yield sprintf' "%d" p
                        | Range (l,u) -> yield sprintf' "[%d..%d]" l u
                    }

                match ports with
                | [] -> ()
                | h :: rest ->
                    yield! printPort h

                    for p in rest do
                        yield ", "
                        yield! printPort p
            } |> String.build

        let parsePermissions (n : int) =
            let p = enum<Permissions> n
            if p < Permissions.None || p > Permissions.All then
                failwithf "invalid permissions %d" n
            else p

        let isLocalIpAddress =
            let localIPs =
                hset [
                        yield IPAddress.IPv6Loopback
                        yield IPAddress.Loopback
                        yield! Dns.GetHostAddresses(Dns.GetHostName())
                ]

            localIPs.Contains


        let parseHostname (hostname : string) =
            if hostname = null then Dns.GetHostName()
            else
                match hostname.Trim() with
                | "" -> Dns.GetHostName()
                | h -> h

        let parseListenIps (ips: string list) =
            match ips with
            | [] -> [IPAddress.Any]
            | _ -> 
                let parsed = ips |> List.map (fun ipstr -> 
                    let success, ip = IPAddress.TryParse ipstr
                    if success then Choice1Of2 ip else Choice2Of2 ipstr)

                let choose1Of2 = function Choice1Of2 fst -> Some fst | _ -> None
                let choose2Of2 = function Choice2Of2 snd -> Some snd | _ -> None
                let is1Of2 = function Choice1Of2 _ -> true | _ -> false
                let is2Of2 = function Choice2Of2 _ -> true | _ -> false

                if parsed |> List.forall is1Of2 then
                    let candidates = 
                        parsed |> List.choose choose1Of2
                               |> List.map (fun ip -> if isLocalIpAddress ip then Choice1Of2 ip else Choice2Of2 ip)

                    if candidates |> List.forall is1Of2 then 
                        candidates |> List.choose choose1Of2
                    else 
                        let nonLocal = candidates |> List.choose choose2Of2
                        exiter.Exit(sprintf "unable to listen to non-local ips: %A" nonLocal)
                else
                    let parseFailed = parsed |> List.choose choose2Of2
                    exiter.Exit(sprintf "unable to parse ips: %A" parseFailed)


        let resolveAddress (logger : ILogger) (hostname : string) (port : int) =
            // check if valid local hostname
            try
                if Dns.GetHostAddresses hostname |> Array.forall isLocalIpAddress then
                    Address(hostname, port)
                else
                    exiter.Exit(sprintf "invalid hostname '%s'." hostname, id = 1)
            with _ -> exiter.Exit(sprintf "invalid hostname '%s'." hostname, id = 1)


        let mainLoop () : 'T =
            let rec loop () = async {
                do! Async.Sleep 2000
                return! loop ()
            }

            Async.RunSynchronously(loop())

        let initWindowProc (args : MBracedConfig list) =
            try
                let selfExe = System.Reflection.Assembly.GetExecutingAssembly().Location
                let args = args |> List.filter (function Detach | Spawn_Window -> false | _ -> true)

                let psi = new ProcessStartInfo(selfExe, mbracedParser.PrintCommandLineFlat args)
                psi.UseShellExecute <- true
                psi.CreateNoWindow <- false

                let _ = Process.Start psi in ()
            with e -> exiter.Exit("error spawning child process.", id = 5)

        let initBackgroundProc (args : MBracedConfig list) =
            async {
                try
                    // set up an ipc receiver actor
                    let receiverId = Guid.NewGuid().ToString()
                    use receiver =
                        Receiver.create<MBracedSpawningServer> ()
                        |> Receiver.rename receiverId
                        |> Receiver.publish [| PipeProtocol () |]
                        |> Receiver.start

                    let selfExe = System.Reflection.Assembly.GetExecutingAssembly().Location
                    let args = Parent_Receiver_Id (selfProc.Id, receiverId) :: args |> List.filter (function Detach | Spawn_Window -> false | _ -> true)

                    let proc = new System.Diagnostics.Process()

                    proc.StartInfo.FileName <- selfExe
                    proc.StartInfo.Arguments <- mbracedParser.PrintCommandLineFlat args
                    proc.StartInfo.UseShellExecute <- false
                    proc.StartInfo.CreateNoWindow <- true
                    proc.StartInfo.RedirectStandardOutput <- true
                    proc.StartInfo.RedirectStandardError <- true

                    proc.EnableRaisingEvents <- true

                    let _ = proc.Start ()

                    // start IO redirection
                    use cts = new CancellationTokenSource()
                    use _ = exiter.ExitEvent.Subscribe(fun _ -> cts.Cancel())
                    do Async.Start(redirectAsync proc.StandardError Console.Error, cts.Token)
                    do Async.Start(redirectAsync proc.StandardOutput Console.Out, cts.Token)

                    let! result = 
                        receiver 
                        |> Receiver.toObservable 
                        |> Observable.merge (proc.Exited |> Observable.map (fun _ -> StartupError (proc.ExitCode, None)))
                        |> Async.AwaitObservable

                    match result with
                    | StartupError (id,_) -> 
                        do! Async.Sleep 10
                        exiter.Exit(id = id)
                    | StartupSuccessful (_,r) ->
                        r.Reply <| Value (Some "Detaching from console.")
                        do! Async.Sleep 50
                        exiter.Exit(id = 0)

                with e -> exiter.Exit("error spawning child process.", id = 5)
            } |> Async.RunSynchronously