namespace Nessos.MBrace.Runtime

    open System
    open System.Diagnostics
    open System.Net
    open System.Collections.Generic

    open Nessos.Thespian
    open Nessos.Thespian.Agents
    open Nessos.Thespian.ImemDb

    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Utils

    type private PerfCounter = System.Diagnostics.PerformanceCounter

    type PerformanceCounter = single option

    /// Some node metrics, such as CPU, memory usage, etc
    type NodePerformanceInfo =
        {
            CpuUsage            : PerformanceCounter
            CpuUsageAverage     : PerformanceCounter
            TotalMemory         : PerformanceCounter
            MemoryUsage         : PerformanceCounter
            NetworkUsageUp      : PerformanceCounter
            NetworkUsageDown    : PerformanceCounter
        } 
        with 
            /// Represents failure in retrieving performance counters.
            static member NotAvailable = 
                {
                    CpuUsage            = None
                    CpuUsageAverage     = None 
                    TotalMemory         = None
                    MemoryUsage         = None
                    NetworkUsageUp      = None
                    NetworkUsageDown    = None
                }

    type private Counter = TotalCpu | TotalMemoryUsage 
    type private PerformanceCounterType = PerformanceCounter of (unit -> PerformanceCounter)
    type private Message = Info of AsyncReplyChannel<NodePerformanceInfo> | Stop of AsyncReplyChannel<unit>

    /// Collects statistics on CPU, memory, network, etc.
    type PerformanceMonitor (?updateInterval : int, ?maxSamplesCount : int) =

        let onMono = Utils.runsOnMono

        // Get a new counter value after 0.5 sec and keep the last 20 values
        let updateInterval = defaultArg updateInterval 500
        let maxSamplesCount = defaultArg maxSamplesCount 20
    
        // Performance counters 
        let cpuUsage =
            if PerformanceCounterCategory.Exists("Processor") then 
                let pc = new PerfCounter("Processor", "% Processor Time", "_Total",true)
                Some <| PerformanceCounter (fun () -> Some (pc.NextValue()))
            else None
    
        let totalMemory = 
            if onMono then
                if PerformanceCounterCategory.Exists("Mono Memory") then 
                    let pc = new PerfCounter("Mono Memory", "Total Physical Memory")
                    Some <| PerformanceCounter (fun () -> Some (pc.NextValue()))
                else None
            else
                let ci = Microsoft.VisualBasic.Devices.ComputerInfo() // DAFUQ? maybe use wmi?
                Some <| PerformanceCounter( let mb = ci.TotalPhysicalMemory / (uint64 (1 <<< 20))
                                            fun () -> Some (single mb) )
    
        let memoryUsage = 
            if PerformanceCounterCategory.Exists("Memory") 
            then match totalMemory with
                    | None -> None
                    | Some(PerformanceCounter(getNext)) ->
                    let pc = new PerfCounter("Memory", "Available Mbytes",true)
                    match getNext() with
                    | None -> None
                    | Some totalMemory -> Some <| PerformanceCounter (fun () -> Some (100.f - 100.f * pc.NextValue() / totalMemory))
            else None
    
        let networkSentUsage =
            if PerformanceCounterCategory.Exists("Network Interface") then 
                let inst = (new PerformanceCounterCategory("Network Interface")).GetInstanceNames()
                let pc = 
                    inst |> Array.map (fun nic ->
                                new PerfCounter("Network Interface", "Bytes Sent/sec", nic))
    
                Some <| PerformanceCounter(fun () -> 
                        Some(pc |> Array.fold (fun sAcc s ->     
                                                    sAcc + 8.f * s.NextValue () / 1024.f)
                                                    0.f))
            else None
    
        let networkReceivedUsage =
            if PerformanceCounterCategory.Exists("Network Interface") then 
                let inst = (new PerformanceCounterCategory("Network Interface")).GetInstanceNames()
                let pc = 
                    inst |> Array.map (fun nic ->
                                new PerfCounter("Network Interface", "Bytes Received/sec",nic))
    
                Some <| PerformanceCounter(fun () -> 
                        Some(pc |> Array.fold (fun rAcc r ->     
                                                    rAcc + 8.f * r.NextValue () / 1024.f )
                                                    0.f))
            else None
    
        let getPerfValue : PerformanceCounterType option -> PerformanceCounter = function
            | None -> None
            | Some(PerformanceCounter(getNext)) -> getNext()
    
        let getAverage (values : PerformanceCounter seq) =
            if Seq.exists ((=) None) values then None
            else values |> Seq.map (function (Some v) -> v | v -> failwithf "invalid state '%A'" v)
                        |> Seq.average
                        |> Some
    
    
        let perfCounterActor = new MailboxProcessor<Message>(fun inbox ->
    
            let queues = dict [ TotalCpu, Queue<PerformanceCounter>()
                                TotalMemoryUsage, Queue<PerformanceCounter>()
                                ]
    
            let newValue cnt = 
                match cnt with
                | TotalCpu -> cpuUsage
                | TotalMemoryUsage -> memoryUsage
                |> getPerfValue
    
            let updateQueues () =
                [TotalCpu; TotalMemoryUsage]
                |> List.iter (fun cnt ->
                    let q = queues.[cnt]
                    let newVal = newValue cnt
    
                    if q.Count < maxSamplesCount then q.Enqueue newVal
                    else q.Dequeue() |> ignore; q.Enqueue newVal)
    
            let newNodePerformanceInfo () : NodePerformanceInfo =
                {
                    CpuUsage = queues.[TotalCpu].Peek()
                    CpuUsageAverage = queues.[TotalCpu] |> getAverage
                    TotalMemory = totalMemory |> getPerfValue
                    MemoryUsage = queues.[TotalMemoryUsage].Peek()
                    NetworkUsageUp = networkSentUsage |> getPerfValue
                    NetworkUsageDown = networkReceivedUsage |> getPerfValue
                }
    
            let rec agentLoop () : Async<unit> = async {
                updateQueues ()
    
                while inbox.CurrentQueueLength <> 0 do
                    let! msg = inbox.Receive()
                    match msg with
                    | Stop ch -> ch.Reply (); return ()
                    | Info ch -> newNodePerformanceInfo () |> ch.Reply
    
                do! Async.Sleep updateInterval
    
                return! agentLoop ()
            }
    
            agentLoop ())

        member this.GetCounters () : NodePerformanceInfo =
            try
                perfCounterActor.PostAndReply(fun ch -> Info ch)
            with _ -> NodePerformanceInfo.NotAvailable // TODO : revise

        member this.Start () =
            perfCounterActor.Start()
            this.GetCounters() |> ignore // first value always 0

        member this.Stop () =
            perfCounterActor.PostAndReply(fun ch -> Stop ch)

        interface System.IDisposable with
            member this.Dispose () = this.Stop()