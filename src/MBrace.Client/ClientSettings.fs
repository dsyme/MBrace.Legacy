namespace Nessos.MBrace.Client

    open System
    open System.Reflection
    open System.IO

    open Nessos.FsPickler
    open Nessos.UnionArgParser
    open Nessos.Vagrant
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Remote

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Logging

    module private ConfigUtils =

        type AppConfigParameter =
            | MBraced_Path of string
            | Working_Directory of string
        with
            interface IArgParserTemplate with
                member config.Usage =
                    match config with
                    | MBraced_Path _ -> "path to a local mbraced executable."
                    | Working_Directory _ -> "the working directory of this client instance. Leave blank for a temp folder."

        let initClientConfiguration () =
            
            let parser = new UnionArgParser<AppConfigParameter>()
            let thisAssembly = System.Reflection.Assembly.GetExecutingAssembly()
            let parseResults = parser.ParseAppSettings(thisAssembly)
            
            // parse mbraced executable
            let mbracedExe = parseResults.TryGetResult <@ MBraced_Path @>

            // parse working directory
            let workingDirectory = parseResults.TryGetResult <@ Working_Directory @>

            let logger = Logger.createConsoleLogger()

            // init runtime configuration
            SystemConfiguration.InitializeConfiguration(logger, ?mbraceDaemonPath = mbracedExe, 
                                ?workingDirectory = workingDirectory, useVagrantPickler = true, cleanupWorkingDirectory = false)

            // set the default store provider; use the local temp folder
            let tmp = FileSystemStore.LocalTemp
            let storeInfo = StoreRegistry.Register(tmp, makeDefault = true)

            ()


    open ConfigUtils

    /// The object representing the {m}brace client settings.
    type MBraceSettings private () =

        static let mutable timeout = 30000
        static let mutable defaultContainer = None

        static let init = runOnce initClientConfiguration
        
        /// Gets the client's unique identifier.
        static member ClientId = init () ; VagrantRegistry.Instance.UUId

        /// The (relative/absolute) path to the mbraced.exe.
        static member MBracedExecutablePath 
            with get () = init () ; SystemConfiguration.MBraceDaemonExecutablePath
            and set p = init () ; SystemConfiguration.MBraceDaemonExecutablePath <- p

        /// Gets or sets the default logger used by the client process.
        static member Logger
            with get () = init () ; SystemConfiguration.Logger
            and set l = init () ; SystemConfiguration.Logger <- l

        /// Gets or sets the default store instance used by the client process.
        static member DefaultStore
            with get () = init (); StoreRegistry.DefaultStoreInfo.Store
            and set store = init (); let info = StoreRegistry.Register(store, makeDefault = true) in ()

        /// Gets the path used by the client as a working directory.
        static member WorkingDirectory = init () ; SystemConfiguration.WorkingDirectory
        /// Gets the assembly cache directory used by Vagrant.
        static member AssemblyCacheDirectory = init () ; SystemConfiguration.AssemblyCacheDirectory
        /// Gets the local cache directory used for Store primitives.
        static member LocalCacheStoreDirectory = init () ; SystemConfiguration.LocalCacheStoreDirectory

        /// Gets or sets the default timeout (in milliseconds) used for communicating with the runtime
        static member DefaultTimeout
            with get () = timeout
            and set t =
                if t <= 0 then invalidArg "timeout" "must be non-negative."
                timeout <- t

        /// Gets or sets the default folder that will be used by any client store operations.
        static member DefaultContainer 
            with get () = 
                init ()
                match defaultContainer with 
                | None -> 
                    defaultContainer <- Some(sprintf "client%s" <| MBraceSettings.ClientId.ToString("N"))
                    defaultContainer.Value
                | Some c -> c
            and set (container : string) = init () ; defaultContainer <- Some container