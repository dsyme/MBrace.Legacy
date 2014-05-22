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

                EnableClientSideStaticChecking : bool

                Logger : ISystemLogger
                Vagrant : VagrantServer

                StoreInfo : StoreInfo
                PrimitiveConfiguration : PrimitiveConfiguration
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

        let activateDefaultStore (localCacheDir : string) (provider : StoreProvider) = 
            let storeInfo = StoreRegistry.Activate(provider, makeDefault = true)
            let coreConfig = PrimitiveConfiguration.activate(storeInfo, localCacheDir)
                
            // soonish
            IoC.RegisterValue (coreConfig,      overwrite = true)
            IoC.RegisterValue (storeInfo,       overwrite = true)
            IoC.RegisterValue (storeInfo.Store, overwrite = true)

            storeInfo, coreConfig

        let registerLogger (logger : ISystemLogger) =
            // register logger
            let logger = Logger.createConsoleLogger()
            IoC.RegisterValue<ISystemLogger>(logger, overwrite = true)
            ThespianLogger.Register(logger)
            logger

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
            let vagrantDir = Path.Combine(workingDirectory, "Vagrant")
            let assemblyCacheDir = Path.Combine(workingDirectory, "AssemblyCache")
            let localCacheDir = Path.Combine(workingDirectory, "LocalCache")

            let populate () =
                if Directory.Exists workingDirectory then Directory.Delete(workingDirectory, true)
                Directory.CreateDirectory workingDirectory |> ignore
                Directory.CreateDirectory vagrantDir |> ignore
                Directory.CreateDirectory assemblyCacheDir |> ignore
                Directory.CreateDirectory localCacheDir |> ignore

            do retry (RetryPolicy.Retry(2, 0.1<sec>)) populate

            // activate vagrant
            let vagrant = new VagrantServer(outpath = vagrantDir)

            // register serializer
            Serialization.Register vagrant.Pickler

            let logger = registerLogger <| Logger.createConsoleLogger()

            // activate store provider
            let storeInfo, coreConfig = activateDefaultStore localCacheDir storeProvider

            // initialize connection pool
            do ConnectionPool.TcpConnectionPool.Init()

            {
                ClientId = vagrant.UUId

                EnableClientSideStaticChecking = true

                MBracedPath = mbracedExe
                WorkingDirectory = workingDirectory

                LocalCacheDirectory = localCacheDir

                Logger = logger
                Vagrant = vagrant

                StoreInfo = storeInfo
                PrimitiveConfiguration = coreConfig
            }


    open ConfigUtils

    type MBraceSettings private () =

        static let config = Atom.atom <| initConfiguration ()
        
        static member ClientId = config.Value.ClientId

        static member MBracedExecutablePath 
            with get () = 
                match config.Value.MBracedPath with 
                | None -> mfailwith "No mbrace daemon executable defined." 
                | Some p -> p
            and set p = 
                if File.Exists p then
                    config.Swap(fun c -> { c with MBracedPath = Some p })
                else
                    mfailwithf "Invalid path '%s'." p

        static member ClientSideExpressionCheck
            with get () = config.Value.EnableClientSideStaticChecking
            and set p = config.Swap(fun c -> { c with EnableClientSideStaticChecking = p })

        static member internal DefaultPrimitiveConfiguration = config.Value.PrimitiveConfiguration

        static member Logger
            with get () = config.Value.Logger
            and set   l = config.Swap(fun c -> { c with Logger = l })

        static member StoreProvider
            with get () = config.Value.StoreInfo.Provider

            and set p =
                // store activation has side-effects ; use lock instead of swap
                lock config (fun () ->
                    let storeInfo, coreConfig = activateDefaultStore config.Value.LocalCacheDirectory p
                    config.Swap(fun c -> { c with StoreInfo = storeInfo ; PrimitiveConfiguration = coreConfig })
                )

        static member WorkingDirectory = config.Value.WorkingDirectory

        static member internal Vagrant = config.Value.Vagrant
        static member internal StoreInfo = config.Value.StoreInfo