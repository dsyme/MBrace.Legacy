namespace Nessos.MBrace.Runtime

    open System
    open System.IO

    open Nessos.FsPickler
    open Nessos.Vagrant

    open Nessos.Thespian.ConcurrencyTools
    open Nessos.Thespian.Remote

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime.Logging

    [<AutoOpen>]
    module private InitializerUtils =
        
        type Configuration =
            {
                MBraceDaemonExecutablePath : string option
                MBraceWorkerExecutablePath : string option

                WorkingDirectory : string
                AssemblyCacheDirectory : string
                LocalCacheStoreDirectory : string

                Logger : ISystemLogger
            }

        let registerLogger (logger : ISystemLogger) =
            ThespianLogger.Register(logger)
            IoC.RegisterValue<ISystemLogger>(logger, overwrite = true)

        let initRuntimeState cleanup useVagrantPickler 
                                (mbracedPath : string option) 
                                (workingDirectory : string option) 
                                (logger : ISystemLogger option) =

            let cleanup = defaultArg cleanup false
            let useVagrantPickler = defaultArg useVagrantPickler false

            let mbracedPath =
                match mbracedPath with
                | None ->
                    let thisAssembly = System.Reflection.Assembly.GetExecutingAssembly()
                    let candidate = Path.Combine(Path.GetDirectoryName thisAssembly.Location, "mbraced.exe")
                    if File.Exists candidate then Some candidate
                    else None
                | Some path when File.Exists path -> Some <| Path.GetFullPath path
                | Some path -> mfailwithf "Invalid configuration: '%s' does not exist." path

            let mbraceWorkerPath =
                match mbracedPath with
                | None -> None
                | Some d ->
                    let candidate = Path.Combine(Path.GetDirectoryName d, "mbrace.worker.exe")
                    if File.Exists candidate then Some candidate
                    else
                        None
            
            let workingDirectory =
                match workingDirectory with
                | Some d -> d
                | None ->
                    let uniq = DateTime.Now.Ticks |> BitConverter.GetBytes |> String.Convert.BytesToBase32
                    let tmpName = sprintf "mbrace-%d-%s" selfProc.Id uniq
                    Path.Combine(Path.GetTempPath(), tmpName)
                |> Path.GetFullPath

            let assemblyCacheDir = Path.Combine(workingDirectory, "AssemblyCache")
            let localCacheDir = Path.Combine(workingDirectory, "StoreCache")

            let populate () =
                if cleanup && Directory.Exists workingDirectory then Directory.Delete(workingDirectory, true)
                Directory.CreateDirectory workingDirectory |> ignore
                Directory.CreateDirectory assemblyCacheDir |> ignore
                Directory.CreateDirectory localCacheDir |> ignore

            do retry (RetryPolicy.Retry(3, 0.5<sec>)) populate

            // init & register Vagrant daemon
            let vagrant = new Vagrant(cacheDirectory = assemblyCacheDir)
            do VagrantRegistry.Register vagrant

            // register serializer
            Serialization.Register(
                if useVagrantPickler then vagrant.Pickler
                else
                    FsPickler.CreateBinary())

            // activate local cache
            let localCache = StoreDefinition.FileSystem(localCacheDir)
            do StoreRegistry.ActivateLocalCacheStore(localCache)

            // register logger
            let logger = match logger with Some l -> l | None -> new NullLogger() :> _
            do registerLogger logger

            // initialize Thespian connection pool
            do ConnectionPool.TcpConnectionPool.Init()

            {
                MBraceDaemonExecutablePath = mbracedPath
                MBraceWorkerExecutablePath = mbraceWorkerPath

                WorkingDirectory = workingDirectory
                AssemblyCacheDirectory = assemblyCacheDir
                LocalCacheStoreDirectory = localCacheDir

                Logger = logger
            }

    type SystemConfiguration private () =
        
        static let container = Atom.atom<Configuration option> None

        static let getConfig () =
            match container.Value with
            | None -> invalidOp "Runtime configuration has not been initialized."
            | Some c -> c

        static member InitializeConfiguration(?logger : ISystemLogger, ?mbracedPath : string, ?workingDirectory : string, 
                                                    ?cleanupWorkingDirectory : bool, ?useVagrantPickler) =

            lock container (fun () ->
                if container.Value.IsSome then
                    invalidOp "Runtime configuration has already been initialized."

                let config = initRuntimeState cleanupWorkingDirectory useVagrantPickler mbracedPath workingDirectory logger
                container.Set <| Some config)

        static member WorkingDirectory = getConfig().WorkingDirectory
        static member AssemblyCacheDirectory = getConfig().AssemblyCacheDirectory
        static member LocalCacheStoreDirectory = getConfig().LocalCacheStoreDirectory

        static member Logger
            with get () = getConfig().Logger
            and set l =
                lock container (fun () ->
                    match container.Value with
                    | None -> invalidOp "Runtime configuration has not been initialized."
                    | Some conf -> 
                        do registerLogger l
                        let conf' = { conf with Logger = l }
                        container.Set <| Some conf')

        static member MBraceDaemonExecutablePath
            with get () = 
                match getConfig().MBraceDaemonExecutablePath with
                | None -> mfailwith "No mbrace daemon executable specified." 
                | Some p -> p

            and set p = 
                let p = Path.GetFullPath p
                if File.Exists p then
                    getConfig() |> ignore // force exception if not initialized
                    container.Swap(fun c -> Some { c.Value with MBraceDaemonExecutablePath = Some p })
                else
                    mfailwithf "Invalid path '%s'." p

        static member MBraceWorkerExecutablePath
            with get () = 
                match getConfig().MBraceWorkerExecutablePath with
                | None -> mfailwith "No mbrace worker executable specified." 
                | Some p -> p

            and set p = 
                let p = Path.GetFullPath p
                if File.Exists p then
                    getConfig() |> ignore // force exception if not initialized
                    container.Swap(fun c -> Some { c.Value with MBraceWorkerExecutablePath = Some p })
                else
                    mfailwithf "Invalid path '%s'." p