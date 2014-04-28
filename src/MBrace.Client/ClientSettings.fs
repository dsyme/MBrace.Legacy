namespace Nessos.MBrace.Client

    open System
    open System.IO

    open Nessos.FsPickler
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Remote

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Utils.AssemblyCache
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Store
    open Nessos.MBrace.Store.Registry
    open Nessos.MBrace.Caching

    module internal ConfigUtils =
        // TODO: individual registrations should enforce some sort of locking

        let selfExe = System.Reflection.Assembly.GetExecutingAssembly().Location

        let resolveMBracedExe (settings : ShellSettings option) =
            match settings with
            | Some settings -> settings.MBracedLocalExe
            | None ->
                let candidate = Path.Combine(Path.GetDirectoryName selfExe, "mbraced.exe")
                if File.Exists candidate then Some candidate
                else None

        let registerSerializer (settings : ShellSettings option) =

            let pickler =
                match settings with
                | Some settings -> settings.ShellPickler
                | None -> new FsPickler()

            IoC.RegisterValue<FsPickler>(pickler)
            do Nessos.MBrace.Runtime.Serializer.Register pickler
            pickler

        let registerLogger (logger : ILogger option) =
            let logger = 
                match logger with
                | None -> Logger.createNullLogger ()
                | Some logger -> logger

            IoC.RegisterValue<ILogger>(logger, behaviour = Override)

        let resolveWorkingDirs (settings : ShellSettings option) =
            let assemblyCachePath, localCachePath =
                match settings with
                | Some settings -> settings.AssemblyCachePath, settings.LocalCachePath
                | None ->
                    // create a temp directory
                    let dir = Path.Combine(Path.GetTempPath(), sprintf "mbrace-cache-%d" selfProc.Id)
                    let assemblyCachePath = Path.Combine(dir, "AssemblyCache")
                    let localCachePath = Path.Combine(dir, "LocalCache")
                    let populate () =
                        if Directory.Exists dir then Directory.Delete(dir, true)
                        Directory.CreateDirectory dir |> ignore
                        Directory.CreateDirectory assemblyCachePath |> ignore
                        Directory.CreateDirectory localCachePath |> ignore

                    retry (RetryPolicy.Retry(2, 0.5<sec>)) populate

                    assemblyCachePath, localCachePath

            IoC.RegisterValue<LocalCacheStore>(LocalCacheStore(localCachePath), "cacheStore")
            AssemblyCache.SetCacheDir assemblyCachePath

            assemblyCachePath, localCachePath

        let parseStoreProvider (settings : ShellSettings) =
            StoreProvider.Parse(settings.StoreProvider, settings.StoreEndpoint)

        let activateDefaultStore (provider : StoreProvider) = 
            let storeInfo = StoreRegistry.Activate(provider, makeDefault = true)
            IoC.RegisterValue (storeInfo, behaviour = Override)
            IoC.RegisterValue (storeInfo.Store, behaviour = Override)
            IoC.RegisterValue<ICloudRefStore>(new Nessos.MBrace.Core.CloudRefStore(storeInfo.Store) :> ICloudRefStore, behaviour = Override)
            IoC.RegisterValue<IMutableCloudRefStore>(new Nessos.MBrace.Core.MutableCloudRefStore(storeInfo.Store) :> IMutableCloudRefStore, behaviour = Override)
            IoC.RegisterValue<ICloudSeqStore>(
                new Nessos.MBrace.Core.CloudSeqStore(storeInfo.Store) :> ICloudSeqStore, behaviour = Override)
            IoC.RegisterValue<ICloudFileStore>(
                new Nessos.MBrace.Core.CloudFileStore(storeInfo.Store) :> ICloudFileStore, behaviour = Override)
            IoC.RegisterValue<Nessos.MBrace.Core.StoreLogger>(
                Nessos.MBrace.Core.StoreLogger(store = storeInfo.Store, batchCount = 42, batchTimespan = 500), behaviour = Override)


    open ConfigUtils


    type MBraceSettings private () =

        static let clientId = Guid.NewGuid()
        static let mbracedPath = ref None
        static let defaultStoreProvider = ref None
        static let clientSideExprCheck = ref true
        static let localCachePath = ref None
        static let assemblyCachePath = ref None

        static let init =
            fun () ->
                try
                    do Assembly.RegisterAssemblyResolutionHandler()
                    do ConnectionPool.TcpConnectionPool.Init()
                    do registerLogger None

                    let acp, lcp = resolveWorkingDirs Shell.Settings
                    localCachePath := Some lcp
                    assemblyCachePath := Some acp
                    mbracedPath := resolveMBracedExe Shell.Settings

                    let _ = registerSerializer Shell.Settings

                    match Shell.Settings with
                    | None -> ()
                    | Some settings ->    
                        defaultStoreProvider := Some <| parseStoreProvider settings
                        activateDefaultStore defaultStoreProvider.Value.Value

                with e ->
                    match Shell.Settings with
                    | Some settings ->
                        settings.ShellActor.Post <| MBraceConfigError (sprintf "{m}brace fatal error: %s" e.Message, 42)
                    | None -> 
                        // will result in type initialization exception, probably not the best way
                        reraise ()
            |> runOnce

        static do init ()
        
        static member ClientId = clientId

        static member MBracedExecutablePath 
            with get () = 
                match mbracedPath.Value with 
                | None -> mfailwith "No mbrace daemon executable defined." 
                | Some p -> p
            and set p = mbracedPath := Some p

        static member ClientSideExpressionCheck
            with get () = clientSideExprCheck.Value
            and set p = clientSideExprCheck := p

        static member StoreProvider
            with get () = 
                match defaultStoreProvider.Value with 
                | None -> mfailwith "No default store provider has been specified."
                | Some p -> p
            and set p = 
                defaultStoreProvider := Some p
                activateDefaultStore p

        static member TryGetStoreProvider () = defaultStoreProvider.Value

        static member LocalCachePath
            with get () = localCachePath.Value.Value
            and set p =
                if Directory.Exists p then
                    IoC.RegisterValue<LocalCacheStore>(LocalCacheStore(p), "cacheStore")
                else mfailwith "Invalid cache directory specified."

        static member AssemblyCachePath
            with get () = assemblyCachePath.Value.Value
            and set p =
                if Directory.Exists p then
                    AssemblyCache.SetCacheDir p
                else mfailwith "Invalid cache directory specified."

        static member internal Initialize () = init()

namespace Nessos.MBrace.Runtime
    open Nessos.MBrace.Client
    
    type MBraceSettingsExtensions =
        static member Init () = MBraceSettings.Initialize()