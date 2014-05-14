namespace Nessos.MBrace.Runtime.Service

    open System
    open System.Configuration
    open System.Configuration.Install
    open System.ComponentModel
    open System.ServiceProcess

    open Nessos.MBrace.Utils

    [<RunInstaller(true)>]
    type MBraceServiceInstaller () as installer =
        inherit Installer ()

        let processInstaller = new ServiceProcessInstaller();
        let serviceInstaller = new ServiceInstaller();

        do
            processInstaller.Account <- ServiceAccount.LocalSystem
            processInstaller.Password <- null
            processInstaller.Username <- null
        
            serviceInstaller.Description <- "MBrace Runtime Service. Initializes a MBrace daemon with the given arguments and the mbraced configuration file."
            serviceInstaller.DisplayName <- "MBrace Runtime"
            serviceInstaller.ServiceName <- "MBrace"
            serviceInstaller.StartType <- ServiceStartMode.Automatic

            [| processInstaller :> Installer; serviceInstaller :> Installer|]
            |> installer.Installers.AddRange



    module internal WindowsService =
    
        [<EntryPoint>]
        let main _ =
            
            let results = config.ParseAppSettings(errorHandler = plugExiter exiter)
            let mbracedExe = results.PostProcessResult(<@ MBraced_Path @>, parseMBracedPath)
            let mbracedExe = parseMBracedPath "mbraced.exe"

            let svc = new MBraceService(mbracedExe)

            ServiceBase.Run [| svc :> ServiceBase |]

            exiter.Exit(id = 0)