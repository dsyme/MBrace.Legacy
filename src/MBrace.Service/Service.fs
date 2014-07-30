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
        let serviceState = ref None : (Guid * Diagnostics.Process * Node) option ref

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
            | Some n -> invalidOp "Service is already running."
            | None -> ()

            let msg = sprintf' "Starting mbraced\nPath: %s\nArguments: %s" MBraceSettings.MBracedExecutablePath (String.concat " " args)
            eventLog.WriteEntry(msg)

            let node = Node.Spawn(args, background = true)

            serviceState := Some(node.DeploymentId, node.Process.Value, node)
            let pid = node.Process.Value.Id

            let msg = sprintf' "Started mbraced\nProcess Id: %d\nDeployment Id: %O\nUri: %O" pid node.DeploymentId node.Uri
            eventLog.WriteEntry(msg)

            node.Process.Value.Exited.Add(fun _ -> 
                    eventLog.WriteEntry(sprintf' "mbraced process %d has exited." pid)
                    svc.Stop() )

        override svc.OnStop () =
            base.OnStop()
            
            eventLog.WriteEntry("Stopping service")

            match tryGetCurrentDaemon () with
            | Some n ->
                eventLog.WriteEntry("Stopping mbraced")
                n.Kill ()
                eventLog.WriteEntry("Stopped mbraced")
            | _ -> ()

            serviceState := None

            eventLog.WriteEntry("Stopped service")
