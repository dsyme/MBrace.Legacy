namespace Nessos.MBrace.Runtime.Service

    open System
    open System.Diagnostics
    open System.ServiceProcess
    open System.Text
    open System.Reflection

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.String
    open Nessos.MBrace.Client

    type MBraceService (mbracedPath : string)  =
        inherit ServiceBase(ServiceName = "MBrace")

        let eventLog = new EventLog()
        let serviceState = ref None : (Guid * Diagnostics.Process * MBraceNode) option ref
        let logf fmt = Printf.ksprintf eventLog.WriteEntry fmt


        let tryGetCurrentDaemon () =
            try
                match serviceState.Value with
                | Some(id, proc, node) when not proc.HasExited && id = node.DeploymentId -> Some node
                | _ -> None
            with _ -> None

        do
            MBraceSettings.MBracedExecutablePath <- mbracedPath
            System.AppDomain.CurrentDomain.ProcessExit.Add(fun _ -> tryGetCurrentDaemon () |> Option.iter (fun n -> n.Kill ()))
            let eventSource = "MBraceService"
            if not (EventLog.SourceExists(eventSource)) then EventLog.CreateEventSource(eventSource, "Application")
            eventLog.Source <- eventSource
            eventLog.Log <- "Application"


        override svc.OnStart args =
            base.OnStart(args)

            match tryGetCurrentDaemon () with
            | Some n -> 
                logf "Request to Start but service is already running. Exiting."
                invalidOp "Service is already running."  
            | None -> ()

            logf "Spawning MBraceNode\nPath: %s\nArguments: %s" MBraceSettings.MBracedExecutablePath (String.concat " " args)

            try
                let node = MBraceNode.Spawn(args, background = true)

                if node.Process.IsNone then failwith "Failed to spawn node process."

                let pid = node.Process.Value.Id
                logf "Spawned Node %A\nProcess Id: %d\nDeployment Id: %O\nUri: %O" node pid node.DeploymentId node.Uri

                serviceState := Some(node.DeploymentId, node.Process.Value, node)

                node.Process.Value.Exited.Add(fun _ -> 
                        logf "mbraced process %d has exited." pid
                        svc.Stop())
            with e ->
                logf "Spawn failed with : %A\nExiting." e
                reraise ()

        override svc.OnStop () =
            base.OnStop()
            
            logf "Stopping service"

            match tryGetCurrentDaemon () with
            | Some n ->
                logf "Stopping mbraced"
                n.Kill ()
                logf "Stopped mbraced"
            | _ -> ()

            serviceState := None

            logf "Stopped service"
