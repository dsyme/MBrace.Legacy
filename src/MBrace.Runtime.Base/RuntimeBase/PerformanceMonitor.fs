module Nessos.MBrace.Runtime.PerformanceMonitor

    open System
    open System.Diagnostics
    open System.Net
    open System.Collections.Generic

    open Nessos.Thespian
    open Nessos.Thespian.Agents
    open Nessos.Thespian.ImemDb

    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Utils

    //
    //  TODO : perf actor should not be static instance
    //

    type PerformanceCounterValue =
        | Single of single
        | Pair   of single * single
        | NotAvailable
    with 
        override this.ToString () = 
            match this with 
            | Single v -> sprintf "%.2f" v 
            | Pair (a,b) -> sprintf "%.2f / %.2f" a b
            | NotAvailable -> "N/A"

    /// Some node metrics, such as CPU, memory usage, etc
    type NodePerformanceInfo =
        {
            TotalCpuUsage : PerformanceCounterValue
            TotalCpuUsageAverage : PerformanceCounterValue
            TotalMemory : PerformanceCounterValue
            TotalMemoryUsage : PerformanceCounterValue
            TotalNetworkUsage : PerformanceCounterValue
        }

    type private Counter = TotalCpu | TotalMemoryUsage 
    type private PerformanceCounterType = PerformanceCounter of (unit -> PerformanceCounterValue)

    let private onMono = Utils.runsOnMono

    // Performance counters 
    let private totalCpu =
        if PerformanceCounterCategory.Exists("Processor") then 
            let pc = new PerformanceCounter("Processor", "% Processor Time", "_Total",true)
            Some <| PerformanceCounter (fun () -> Single (pc.NextValue()))
        else None

    let private totalMemory = 
        if onMono then
            if PerformanceCounterCategory.Exists("Mono Memory") then 
                let pc = new PerformanceCounter("Mono Memory", "Total Physical Memory")
                Some <| PerformanceCounter (fun () -> Single (pc.NextValue()))
            else None
        else
            let ci = Microsoft.VisualBasic.Devices.ComputerInfo() // DAFUQ? maybe use wmi?
            Some <| PerformanceCounter( let mb = ci.TotalPhysicalMemory / (uint64 (1 <<< 20))
                                        fun () -> Single (single mb) )

    let private totalMemoryUsage = 
        if PerformanceCounterCategory.Exists("Memory") 
        then match totalMemory with
                | None -> None
                | Some(PerformanceCounter(getNext)) ->
                let pc = new PerformanceCounter("Memory", "Available Mbytes",true)
                match getNext() with
                | NotAvailable 
                | Pair _ -> None
                | Single (totalMemory) -> Some <| PerformanceCounter (fun () -> Single (100.f - 100.f * pc.NextValue() / totalMemory))
        else None

    let private totalNetworkUsage =
        if PerformanceCounterCategory.Exists("Network Interface") then 
            let inst = (new PerformanceCounterCategory("Network Interface")).GetInstanceNames()
            let pc = 
                inst |> Array.map (fun nic ->
                            new PerformanceCounter("Network Interface", "Bytes Sent/sec", nic),
                            new PerformanceCounter("Network Interface", "Bytes Received/sec",nic))

            Some <| PerformanceCounter(fun () -> 
                    Pair(pc |> Array.fold (fun (sAcc, rAcc) (s,r) ->     
                                                sAcc + 8.f * s.NextValue () / 1024.f, rAcc + 8.f * r.NextValue () / 1024.f )
                                                (0.f, 0.f)))
        else None

    let private getPerfValue : PerformanceCounterType option -> PerformanceCounterValue = function
        | None -> NotAvailable
        | Some(PerformanceCounter(getNext)) -> getNext()

    let private getAverage (values : PerformanceCounterValue seq) =
        if Seq.exists ((=) NotAvailable) values then NotAvailable
        else values |> Seq.map (function (Single v) -> v | v -> failwithf "invalid state '%A'" v)
                    |> Seq.average
                    |> Single

    // Get a new counter value after 0.5 sec and keep the last 20 values
    let private updateInterval = 500
    let private maxSamplesCount = 20

    let private perfCounterActor = Actor.bind (fun (inbox: Actor<IReplyChannel<NodePerformanceInfo>>) -> 

        let queues = dict [ TotalCpu, Queue<PerformanceCounterValue>()
                            TotalMemoryUsage, Queue<PerformanceCounterValue>()
                            ]

        let newValue cnt = 
            match cnt with
            | TotalCpu -> totalCpu
            | TotalMemoryUsage -> totalMemoryUsage
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
                TotalCpuUsage = queues.[TotalCpu].Peek()
                TotalCpuUsageAverage = queues.[TotalCpu] |> getAverage
                TotalMemory = totalMemory |> getPerfValue
                TotalMemoryUsage = queues.[TotalMemoryUsage].Peek()
                TotalNetworkUsage = totalNetworkUsage |> getPerfValue
            }

        let rec agentLoop () = async {
            updateQueues ()

            while inbox.CurrentQueueLength <> 0 do
                let! msg = inbox.Receive()
                newNodePerformanceInfo () |> Value |> msg.Reply

            do! Async.Sleep updateInterval

            return! agentLoop ()
        }

        agentLoop ())

    let private notAvailable = 
        {
            TotalCpuUsage = NotAvailable
            TotalCpuUsageAverage = NotAvailable 
            TotalMemory = NotAvailable
            TotalMemoryUsage = NotAvailable
            TotalNetworkUsage = NotAvailable
        }

    let getCounters () = 
        try perfCounterActor.Ref <!= id
        with _ -> notAvailable

    let init () =
        // first value is always 0.
        async {
                perfCounterActor.Start()
                getCounters() |> ignore
                do! Async.Sleep updateInterval
                getCounters() |> ignore
        } |> Async.Start

        perfCounterActor