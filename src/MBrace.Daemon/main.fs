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
        open Nessos.MBrace.Runtime.Logging

        open Nessos.MBrace.Runtime
        open Nessos.MBrace.Runtime.Daemon.Configuration

        [<EntryPoint>]
        let main _ =

            //
            //  Configuration/argument parsing
            //

            if runsOnMono then exiter.Exit("mono not supported... yet!", 1)

            // init performance monitor
            let perfMonTask = Async.StartAsTask(registerPerfMonitorAsync())
            
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
            let defaultPermissions = results.PostProcessResult(<@ Permissions @>, (fun p -> enum<Permissions> p))
            let debugMode = results.Contains <@ Debug @>
            let detach = results.Contains <@ Detach @>
            let spawnWindow = results.Contains <@ Spawn_Window @>
            let useTempWD = results.Contains <@ Use_Temp_WorkDir @>
            let mbraceWorkerExe = results.TryPostProcessResult(<@ MBrace_ProcessDomain_Executable @>, parseWorkerExe)

            let workerPorts =
                // drop *all* app.config settings if either appears as command line argument
                let source =
                    if results.Contains(<@ Worker_Port_Range @>, source = ParseSource.CommandLine) ||
                        results.Contains(<@ Worker_Port @>, source = ParseSource.CommandLine) then
                        Some ParseSource.CommandLine
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
                | true, _ | _, Some "temp" -> None
                | false, Some path -> Some <| Path.GetFullPath path
                | false, None -> results.Raise("missing '--working-directory' parameter.", errorCode = 2)

            // received detach request
            if spawnWindow then            
                initWindowProc <| results.GetAllResults ()
                exiter.Exit(id = 0)


            //
            //  Initialization and registrations
            //

            // increase min threads in thread pool to eliminate scheduling delays
            System.Threading.ThreadPool.SetMinThreads(100,100) |> ignore

            IoC.RegisterValue<Permissions>(defaultPermissions)

            // TODO : remove?
            IoC.RegisterValue(debugMode, "debugMode")

            // initialize base runtime configuration
            SystemConfiguration.InitializeConfiguration(?workingDirectory = workingDirectory, ?mbraceWorkerPath = mbraceWorkerExe, useVagrantPickler = false)
            SystemConfiguration.MBraceWorkerExecutablePath |> ignore // force evaluation to ensure that is found

            // init and register logger
            let logger = initLogger SystemConfiguration.WorkingDirectory logFiles logLevel
            SystemConfiguration.Logger <- logger

            // Register Exiter
            IoC.RegisterValue<IExiter>(exiter)

            // Dependency injection : remove
            IoC.RegisterValue(evalPorts workerPorts, "mbrace.process.portPool")

            // resolve primary address
            let address = resolveAddress hostname primaryPort
            let hostname = address.HostnameOrAddress

            //
            // begin boot
            //

            logger.Logf Info "{m}brace daemon version %O" selfInfo.Version
            logger.Logf Info "Process Id = %d" selfProc.Id
            logger.Logf Info "Default Serializer = %s" Serialization.SerializerRegistry.DefaultName
            logger.Logf Info "Working Directory = %s" SystemConfiguration.WorkingDirectory

            let perfMon = perfMonTask.Result
            IoC.RegisterValue perfMon

            logger.Logf Info "Performance Monitor active"

            if debugMode then logger.LogInfo "Running in DEBUG mode"
            if isWindowed then registerFancyConsoleEvent address

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
                            logger.Log "Error communicating with parent process." LogLevel.Warning
                            
                        flushConsole () )

            mainLoop ()
