module internal Nessos.MBrace.Client.Reporting

    open Nessos.Thespian

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.PrettyPrinters
    open Nessos.MBrace.Core
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Logging

    module Logs =

        let show (logs : seq<SystemLogEntry>) =
            logs
            |> Seq.map (fun e -> e.Print(showDate = true))
            |> String.concat "\n"
            |> printfn "%s"


    type MBraceNodeReporter private () =

        static let nodeReportTemplate : Field<NodeDeploymentInfo> list =
            [
                Field.create "Host" Left (fun n -> n.Uri.Host)
                Field.create "Port" Right (fun n -> n.Uri.Port)
                Field.create "Role" Left (fun n -> n.State)
                Field.create "Location" Left (fun n -> match n.TryGetLocalProcess() with Some p -> sprintf' "Local (Pid %d)" p.Id | _ -> "Remote")
                Field.create "Connection String" Left (fun n -> n.Uri)
            ]

        static let nodePerfReportTemplate : Field<NodeDeploymentInfo> list =
            [
                Field.create "Host" Left (fun n -> n.Uri.Host)
                Field.create "Port" Right (fun n -> n.Uri.Port)
                Field.create "Role" Left (fun n -> n.State)
                Field.create "%Cpu" Right (fun n -> n.PerformanceInfo.Value.TotalCpuUsage)
                Field.create "%Cpu(avg)" Right (fun n -> n.PerformanceInfo.Value.TotalCpuUsageAverage)
                Field.create "Memory(MB)" Right (fun n -> n.PerformanceInfo.Value.TotalMemory)
                Field.create "%Memory" Right (fun n -> n.PerformanceInfo.Value.TotalMemoryUsage)
                Field.create "Network(ul/dl: kbps)" Right (fun n -> n.PerformanceInfo.Value.TotalNetworkUsage)
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


    type MBraceProcessReporter private () =

        static let template : Field<ProcessInfo> list =
            [
                Field.create "Name" Left (fun p -> p.Name)
                Field.create "Process Id" Right (fun p -> p.ProcessId)
                Field.create "Status" Left (fun p -> p.State)
                Field.create "#Workers" Right (fun p -> p.Workers)
                Field.create "#Tasks" Right (fun p -> p.Workers)
                Field.create "Start Time" Left (fun p -> p.InitTime)
                Field.create "Execution Time" Left (fun p -> p.ExecutionTime)
                Field.create "Result Type" Left (fun p -> p.TypeName)
            ]

        static member Report(processes : ProcessInfo list, ?title, ?showBorder) =
            let showBorder = defaultArg showBorder false

            Record.prettyPrint3 template title showBorder processes

//        Record.prettyPrint3 template None

//    //
//    // Node & Runtime information display types
//    //
//
//    and internal NodeInfo (nodes : seq<MBraceNode>) =
//

//
//        let master = match getByRole Master with [] -> None | h :: _ -> Some h
//

//
//            Record.prettyPrint template
//
//        static member Create (nrefs : NodeRef seq) = NodeInfo (nrefs |> Seq.map (fun n -> MBraceNode n))
//
//        static member internal PrettyPrint(nodes : MBraceNode list list, ?displayPerfCounters, ?header, ?useBorders) =
//            let useBorders = defaultArg useBorders false
//            let displayPerfCounter = defaultArg displayPerfCounters false
//
//            let parMap (f : 'T -> 'S) (inputs : 'T list list) = 
//                inputs 
//                |> List.toArray
//                |> Array.Parallel.map (Array.ofList >> Array.Parallel.map f >> Array.toList)
//                |> List.ofArray
//
//            if displayPerfCounter then
//                // force lookup of nodes in parallel
//                let info = nodes |> parMap (fun n -> n, n.GetPerformanceCounters())
//                prettyPrintPerf header useBorders info
//            else
//                prettyPrint header useBorders nodes
//
//        member __.Master = master
//        member __.Slaves = getByRole Slave
//        member __.Alts = getByRole Alt
//        member __.Idle = getByRole Idle
//
//        member conf.Nodes =
//            [
//                yield! conf.Master |> Option.toList
//                yield! conf.Alts
//                yield! conf.Slaves
//                yield! conf.Idle
//            ]
//
//        member conf.Display(?displayPerfCounters, ?useBorders) =
//            let title =
//                if master.IsSome then "{m}brace runtime information (active)"
//                else "{m}brace runtime information (inactive)"
//
//            let nodes =
//                [
//                    yield master |> Option.toList
//                    yield getByRole Alt 
//                    yield getByRole Slave
//                    yield getByRole Idle
//                ]
//
//            NodeInfo.PrettyPrint(nodes, ?displayPerfCounters = displayPerfCounters, header = title, ?useBorders = useBorders)
//
//
//
//    type Node = MBraceNode