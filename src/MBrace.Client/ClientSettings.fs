namespace Nessos.MBrace.Client

    open System
    open System.Reflection
    open System.IO

    open Nessos.Thespian.ConcurrencyTools

    open Nessos.FsPickler
    open Nessos.UnionArgParser
    open Nessos.Vagrant
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Remote

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Logging
    open Nessos.MBrace.Runtime.Store

    module private ConfigUtils =

        type Configuration =
            {
                ClientId : Guid

                MBracedPath : string option

                WorkingDirectory : string
                LocalCacheDirectory : string

                Logger : ISystemLogger

                StoreInfo : StoreInfo
            }

        and AppConfigParameter =
            | MBraced_Path of string
            | Working_Directory of string
            | Store_Provider of string
            | Store_Endpoint of string
        with
            interface IArgParserTemplate with
                member config.Usage =
                    match config with
                    | MBraced_Path _ -> "path to a local mbraced executable."
                    | Working_Directory _ -> "the working directory of this client instance. Leave blank for a temp folder."
                    | Store_Provider _ -> "The type of storage to be used by mbrace."
                    | Store_Endpoint _ -> "Url/Connection string for the given storage provider."

        let registerLogger (logger : ISystemLogger) =
            // register logger
            ThespianLogger.Register(logger)

        let initConfiguration () =
            
            let parser = new UnionArgParser<AppConfigParameter>()
            let thisAssembly = System.Reflection.Assembly.GetExecutingAssembly()
            let parseResults = parser.ParseAppSettings(thisAssembly)
            
            // parse mbraced executable
            let mbracedExe =
                match parseResults.TryGetResult <@ MBraced_Path @> with
                | None ->
                    let candidate = Path.Combine(Path.GetDirectoryName thisAssembly.Location, "mbraced.exe")
                    if File.Exists candidate then Some candidate
                    else None
                | Some path when not <| File.Exists path ->
                    mfailwithf "Invalid configuration: '%s' does not exist." path
                | _ as p -> p

            // working directory
            let workingDirectory =
                match parseResults.TryGetResult <@ Working_Directory @> with
                | None -> Path.Combine(Path.GetTempPath(), sprintf "mbrace-client-%d" selfProc.Id)
                | Some path -> path

            // parse store provider
            let storeProvider =
                match parseResults.TryGetResult <@ Store_Provider @> with
                | None -> StoreProvider.LocalFS
                | Some sp ->
                    let endpoint = defaultArg (parseResults.TryGetResult <@ Store_Endpoint @>) ""
                    StoreProvider.Parse(sp, endpoint)

            // Populate working directory
            let assemblyCacheDir = Path.Combine(workingDirectory, "AssemblyCache")
            let localCacheDir = Path.Combine(workingDirectory, "StoreCache")

            let populate () =
                if Directory.Exists workingDirectory then Directory.Delete(workingDirectory, true)
                Directory.CreateDirectory workingDirectory |> ignore
                Directory.CreateDirectory assemblyCacheDir |> ignore
                Directory.CreateDirectory localCacheDir |> ignore

            do retry (RetryPolicy.Retry(3, 0.5<sec>)) populate

            // activate vagrant
            let vagrant = new Vagrant(cacheDirectory = assemblyCacheDir)
            VagrantRegistry.Register vagrant

            // register serializer
            Serialization.Register vagrant.Pickler

            let logger = Logger.createConsoleLogger()
            do registerLogger logger

            // activate local cache
            do StoreRegistry.ActivateLocalCache(StoreProvider.FileSystem(localCacheDir))

            // activate store provider
            let storeInfo = StoreRegistry.Activate(storeProvider, makeDefault = true)

            // initialize connection pool
            do ConnectionPool.TcpConnectionPool.Init()

            {
                ClientId = vagrant.UUId

                MBracedPath = mbracedExe
                WorkingDirectory = workingDirectory

                LocalCacheDirectory = localCacheDir

                Logger = logger

                StoreInfo = storeInfo
            }


    open ConfigUtils

    /// The object representing the {m}brace client settings.
    type MBraceSettings private () =

        static let config = lazy (Atom.atom <| initConfiguration ())
        
        /// Gets the client's unique identifier.
        static member ClientId = config.Value.Value.ClientId

        /// The (relative/absolute) path to the mbraced.exe.
        static member MBracedExecutablePath 
            with get () = 
                match config.Value.Value.MBracedPath with 
                | None -> mfailwith "No mbrace daemon executable defined." 
                | Some p -> p
            and set p = 
                let p = Path.GetFullPath p
                if File.Exists p then
                    config.Value.Swap(fun c -> { c with MBracedPath = Some p })
                else
                    mfailwithf "Invalid path '%s'." p

        static member internal DefaultStoreInfo = config.Value.Value.StoreInfo

        /// Gets or sets the logger used by the client.
        static member Logger
            with get () = config.Value.Value.Logger
            and set l = 
                lock config.Value (fun () ->
                    registerLogger l
                    config.Value.Swap(fun c -> { c with Logger = l })
                )

        /// Gets or sets the StoreProvider used by the client.
        static member StoreProvider
            with get () = config.Value.Value.StoreInfo.Provider

            and set p =
                // store activation has side-effects ; use lock instead of swap
                lock config.Value (fun () ->
                    let storeInfo = StoreRegistry.Activate(p, makeDefault = true)
                    config.Value.Swap(fun c -> { c with StoreInfo = storeInfo })
                )

        /// Gets the path used by the client as a working directory.
        static member WorkingDirectory = config.Value.Value.WorkingDirectory

        static member internal StoreInfo = config.Value.Value.StoreInfo