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

        let initClientConfiguration () =
            
            let parser = new UnionArgParser<AppConfigParameter>()
            let thisAssembly = System.Reflection.Assembly.GetExecutingAssembly()
            let parseResults = parser.ParseAppSettings(thisAssembly)
            
            // parse mbraced executable
            let mbracedExe = parseResults.TryGetResult <@ MBraced_Path @>

            // parse working directory
            let workingDirectory = parseResults.TryGetResult <@ Working_Directory @> 

            // parse store provider
            let storeProvider =
                match parseResults.TryGetResult <@ Store_Provider @> with
                | None -> StoreDefinition.LocalFS
                | Some sp ->
                    let endpoint = defaultArg (parseResults.TryGetResult <@ Store_Endpoint @>) ""
                    StoreDefinition.Parse(sp, endpoint)

            let logger = Logger.createConsoleLogger()

            // init runtime configuration
            SystemConfiguration.InitializeConfiguration(logger, ?mbraceDaemonPath = mbracedExe, 
                                ?workingDirectory = workingDirectory, useVagrantPickler = true, cleanupWorkingDirectory = false)

            // activate store provider
            let storeInfo = StoreRegistry.Activate(storeProvider, makeDefault = true)

            ()


    open ConfigUtils

    /// The object representing the {m}brace client settings.
    type MBraceSettings private () =

        static let init = runOnce initClientConfiguration
        
        /// Gets the client's unique identifier.
        static member ClientId = init () ; VagrantRegistry.Instance.UUId

        /// The (relative/absolute) path to the mbraced.exe.
        static member MBracedExecutablePath 
            with get () = init () ; SystemConfiguration.MBraceDaemonExecutablePath
            and set p = init () ; SystemConfiguration.MBraceDaemonExecutablePath <- p

        /// Gets or sets the logger used by the client.
        static member Logger
            with get () = init () ; SystemConfiguration.Logger
            and set l = init () ; SystemConfiguration.Logger <- l

        /// Gets or sets the default StoreProvider used by the client.
        static member StoreProvider
            with get () = init () ; StoreRegistry.DefaultStoreInfo.Definition
            and set p = init () ; StoreRegistry.Activate(p, makeDefault = true) |> ignore

        /// Gets the path used by the client as a working directory.
        static member WorkingDirectory = init () ; SystemConfiguration.WorkingDirectory
        static member AssemblyCacheDirectory = init () ; SystemConfiguration.AssemblyCacheDirectory
        static member LocalCacheStoreDirectory = init () ; SystemConfiguration.LocalCacheStoreDirectory