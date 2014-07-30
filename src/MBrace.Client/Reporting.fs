module internal Nessos.MBrace.Client.Reporting

    open Nessos.Thespian

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.PrettyPrinters
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Logging
    open Nessos.MBrace.Runtime.Compiler

    module Logs =

        let show (logs : seq<SystemLogEntry>) =
            logs
            |> Seq.map (fun e -> e.Print(showDate = true))
            |> String.concat "\n"
            |> printfn "%s"


    type CloudComputation =

        static member Compile (block : Cloud<'T>, ?name : string) =
            // force client initialization
            MBraceSettings.ClientId |> ignore

            let cc = CloudCompiler.Compile(block, ?name = name)
            for w in cc.Warnings do
                MBraceSettings.Logger.LogWarning w
            cc

        static member Compile (expr : Quotations.Expr<Cloud<'T>>, ?name : string) =
            // force client initialization
            MBraceSettings.ClientId |> ignore

            let cc = CloudCompiler.Compile(expr, ?name = name)
            for w in cc.Warnings do
                MBraceSettings.Logger.LogWarning w
            cc


    type NodeReporter private () =

        static let nodeReportTemplate : Field<NodeDeploymentInfo> list =
            [
                Field.create "Host" Left (fun n -> n.Uri.Host)
                Field.create "Port" Right (fun n -> n.Uri.Port)
                Field.create "Role" Left (fun n -> n.State)
                Field.create "Location" Left (fun n -> match n.TryGetLocalProcess() with Some p -> sprintf' "Local (Pid %d)" p.Id | _ -> "Remote")
                Field.create "Connection String" Left (fun n -> n.Uri)
            ]

        static let nodePerfReportTemplate : Field<NodeDeploymentInfo> list =
            let printer (value : PerformanceCounter) =
                match value with
                | None -> "N/A"
                | Some value -> sprintf "%.2f" value
            [
                Field.create "Host" Left (fun n -> n.Uri.Host)
                Field.create "Port" Right (fun n -> n.Uri.Port)
                Field.create "Role" Left (fun n -> n.State)
                Field.create "%Cpu" Right (fun n -> printer n.PerformanceInfo.Value.CpuUsage)
                Field.create "%Cpu(avg)" Right (fun n -> printer n.PerformanceInfo.Value.CpuUsageAverage)
                Field.create "Total Memory(MB)" Right (fun n -> printer n.PerformanceInfo.Value.TotalMemory)
                Field.create "%Memory" Right (fun n -> printer n.PerformanceInfo.Value.MemoryUsage)
                Field.create "Network(ul/dl: kbps)" Right (fun n -> sprintf "%s / %s" <| printer n.PerformanceInfo.Value.NetworkUsageUp <| printer n.PerformanceInfo.Value.NetworkUsageUp )
            ]

        static member Report(nodes : seq<NodeDeploymentInfo>, showPerf, ?title, ?showBorder) =
            let showBorder = defaultArg showBorder false

            let nodes = 
                nodes 
                |> Seq.sortBy (fun n -> match n.State with Master -> 0 | AltMaster -> 1 | Slave -> 2 | Idle -> 4)
                |> Seq.toList

            if showPerf then
                Record.prettyPrint3 nodePerfReportTemplate title showBorder nodes
            else
                Record.prettyPrint3 nodeReportTemplate title showBorder nodes


    type ProcessReporter private () =

        static let template : Field<ProcessInfo> list =
            [
                Field.create "Name" Left (fun p -> p.Name)
                Field.create "Process Id" Right (fun p -> p.ProcessId)
                Field.create "Status" Left (fun p -> p.State)
                Field.create "#Workers" Right (fun p -> p.Workers)
                Field.create "#Tasks" Right (fun p -> p.Tasks)
                Field.create "Start Time" Left (fun p -> p.InitTime)
                Field.create "Execution Time" Left (fun p -> p.ExecutionTime)
                Field.create "Result Type" Left (fun p -> p.TypeName)
            ]

        static member Report(processes : ProcessInfo list, ?title, ?showBorder) =
            let showBorder = defaultArg showBorder false

            Record.prettyPrint3 template title showBorder processes