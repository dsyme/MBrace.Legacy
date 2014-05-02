namespace Nessos.MBrace.Runtime.Daemon

    module internal MainModule =
        
        open System
        open System.IO
        open System.Diagnostics

        open Nessos.UnionArgParser

        open Nessos.Thespian
        open Nessos.Thespian.Remote.TcpProtocol
        open Nessos.Thespian.Remote.PipeProtocol

        open Nessos.MBrace.Utils
        open Nessos.MBrace.Store
        open Nessos.MBrace.Store.Registry
        open Nessos.MBrace.Caching
        open Nessos.MBrace.Client

        open Nessos.MBrace.Runtime
        open Nessos.MBrace.Runtime.Daemon.Configuration

        [<EntryPoint>]
        let main _ =

            //
            //  Configuration/argument parsing
            //

            if runsOnMono then exiter.Exit("mono not supported... yet!", 1)

            do Assembly.RegisterAssemblyResolutionHandler()
            
            // register serialization
            do registerSerializers ()
            
            let results = mbracedParser.Parse(errorHandler = plugExiter exiter)

            if results.Contains <@ Version @> then
                printfn "{m}brace daemon version %O" selfInfo.Version
                exiter.Exit ()

            let hostname = results.GetResult(<@ Hostname @>, defaultValue = "") |> parseHostname
            let ips = results.GetResults(<@ Listen_Ips @>) |> parseListenIps
            let primaryPort = results.PostProcessResult (<@ Primary_Port @>, parsePort)
            let workingDirectory = results.TryGetResult <@ Working_Directory @>
            let logLevel = results.PostProcessResult(<@ Log_Level @>, LogLevel.Parse)
            let logFiles = results.GetResults <@ Log_File @>
            let defaultPermissions = results.PostProcessResult(<@ Permissions @>, parsePermissions)
            let debugMode = results.Contains <@ Debug @>
            let detach = results.Contains <@ Detach @>
            let spawnWindow = results.Contains <@ Spawn_Window @>
            let useTempWD = results.Contains <@ Use_Temp_WorkDir @>

            let storeEndpoint = results.GetResult (<@ Store_EndPoint @>, null)
            let storeProvider = results.GetResult (<@ Store_Provider @>, null)
            let mbraceProc = results.GetResult <@ MBrace_ProcessDomain_Executable @>

            let workerPorts =
                // drop *all* app.config settings if either appears as command line argument
                let source =
                    if results.Contains(<@ Worker_Port_Range @>, source = CommandLine) ||
                        results.Contains(<@ Worker_Port @>, source = CommandLine) then
                        Some CommandLine
                    else None

                (results.PostProcessResults(<@ Worker_Port_Range @>, parseWorkerPortRange, ?source = source))
                    @ (results.PostProcessResults(<@ Worker_Port @>, parseSingularWorkerPort, ?source = source))

            // temporarily bind exit of child process to the parent actor
            let parentActor = results.TryPostProcessResult (<@ Parent_Receiver_Id @>,
                                    fun (pid,receiverId) ->
                                        let a = ActorRef.ofProcessId<MBracedSpawningServer> pid receiverId
                                        let d = exiter.ExitEvent.Subscribe (fun err -> flushConsole () ; try a.Post (StartupError err) with _ -> ())
                                        a,d)

            if spawnWindow && detach then
                results.Raise "invalid combination of arguments '--detach' and '--spawn-window'."

            if detach && Environment.UserInteractive  then
                if logFiles.Length = 0 then
                    results.Raise "'--detach' requires at least one logfile."
                else
                    // will make the main thread loop until cancellation is triggered remotely
                    initBackgroundProc <| results.GetAllResults()

            let workingDirectory =
                match useTempWD, workingDirectory with
                | true, _ | _, Some "temp" -> getTempWD ()
                | false, Some path -> Path.GetFullPath path
                | false, None -> results.Raise("missing '--working-directory' parameter.", errorCode = 2)

            // received detach request
            if spawnWindow then            
                initWindowProc <| results.GetAllResults ()
                exiter.Exit(id = 0)


            //
            //  Initialization and registrations
            //


            // TODO : remove?
            IoC.RegisterValue(debugMode, "debugMode")

            // set working directory
            do registerWorkingDirectory useTempWD workingDirectory

            // Register logging
            let logger = registerLogger logFiles logLevel

            // register mbrace.process executable
            registerProcessDomainExecutable mbraceProc
            
            // TODO : move to app.config ? --naah
            IoC.RegisterValue(true, "IsolateProcesses")

            // Register Exiter
            IoC.RegisterValue<IExiter>(exiter)

            // Register Store
            registerStore storeProvider storeEndpoint

            //IoC.RegisterValue(clProcPorts, "mbrace.process.portPool")
            //TODO!!!! THIS IS WRONG
            IoC.RegisterValue(evalPorts workerPorts, "mbrace.process.portPool")

            // resolve primary address
            let address = resolveAddress logger hostname primaryPort

            //
            // begin boot
            //

            logger.Logf Info "{m}brace daemon version %O" selfInfo.Version
            logger.Logf Info "Process Id = %d" selfProc.Id
            logger.Logf Info "Default Serializer = %s" Serialization.SerializerRegistry.DefaultName
            logger.Logf Info "Working Directory = %s" workingDirectory
            logger.Logf Info "StoreProvider = %s" storeProvider

            if debugMode then logger.LogInfo "Running in DEBUG mode."
            if isWindowed then registerFancyConsoleEvent debugMode address

            try
                Definitions.Service.boot hostname ips primaryPort
            with e ->
                logger.LogWithException e "{m}brace fault." Error
                exiter.Exit(id = 1)

            let uri = Uri(sprintf "mbrace://%s:%d" address.HostnameOrAddress address.Port)

            // unsubscribe exiter and exit parent
            do parentActor 
                |> Option.iter (fun (a,d) ->
                        try 
                            d.Dispose ();
                            let msg = a <!= fun ch -> StartupSuccessful(uri, ch)
                            match msg with
                            | None -> ()
                            | Some m -> logger.LogInfo m
                        with e ->
                            logger.LogError e "Error communicating with parent process."
                            
                        flushConsole () )

            mainLoop ()
