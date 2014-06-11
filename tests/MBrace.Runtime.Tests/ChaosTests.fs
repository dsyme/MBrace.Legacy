#nowarn "0044" // Cloud blocks should be wrapped in quotation literals for better debug support.

namespace Nessos.MBrace.Runtime.Tests

    open Nessos.MBrace
    open Nessos.MBrace.Client

    open NUnit.Framework

    open System
    open System.Diagnostics
    open System.Threading
    open System.IO
    open Microsoft.FSharp.Quotations
    open FsUnit
    open FsCheck

    [<TestFixture; Category("ChaosMonkeyTests")>]
    module ``Chaos Monkey Tests`` =
        type SpawnType = Same | Rand of int * int
        type SpawnOnKill = Never | Always of SpawnType | FlipACoin of SpawnType

        type ChaosAction =
            | Attach 
            | Detatch
            | KillViolently of SpawnOnKill

        type ChaosMonkeyConfiguration = {
            MinSleepTime    : int
            MaxSleepTime    : int
            MinKillCount    : int
            MaxKillCount    : int
            MinNodesCount   : int
            MaxNodesCount   : int
        }

        let rec chaosMonkey (conf: ChaosMonkeyConfiguration) 
                            (actions : ChaosAction list) 
                            (runtime: MBraceRuntime): Async<unit> = async {
            match actions with
            | [] -> 
                printfn "Chaos Monkey done"
            | action :: rest ->
                let random = new Random(System.DateTime.Now.Millisecond)
                let nodeCount = runtime.Nodes.Length
                printfn "NodeCount %d" nodeCount
                printfn "Current action %A" action

                match action with
                | KillViolently spawnOnKill ->
                    let processes = Process.GetProcessesByName("mbraced")
                    let killCount = random.Next(conf.MinKillCount, conf.MaxKillCount + 1)
                    let killCount =
                        if nodeCount - killCount < conf.MinNodesCount 
                        then nodeCount - conf.MinNodesCount
                        else killCount

                    printfn "Killing %d processes" killCount
                    let k = ref killCount
                    while !k <> 0 do
                        let i = random.Next(0, processes.Length)
                        if not <| processes.[i].HasExited then
                            processes.[i].Kill()
                            printfn "Killed %A" processes.[i].Id
                            k := !k - 1
            
                    let spawnCount =
                        match spawnOnKill with
                        | Never -> 0
                        | Always Same -> killCount
                        | Always(Rand(min, max)) -> random.Next(min, max)
                        | FlipACoin Same -> if random.Next(0, 2) = 0 then killCount else 0
                        | FlipACoin(Rand(min, max)) -> if random.Next(0, 2) = 0 then random.Next(min, max) else 0

                    if spawnCount > 0 then
                        let spawnCount = 
                            let nodeCount' = runtime.Nodes.Length
                            if nodeCount' + spawnCount > conf.MaxNodesCount 
                            then conf.MaxNodesCount - nodeCount'
                            else spawnCount

                        printfn "Spawning %d nodes..." spawnCount
                        let nodes = Node.SpawnMultiple spawnCount
                        printfn "Attaching nodes..."
                        try
                            do! runtime.AttachAsync nodes
                        with e -> printfn "%A" e
                | Attach ->
                    if nodeCount + 1 > conf.MaxNodesCount then 
                        printfn "Ignoring Attach"
                    else
                        let node = Node.SpawnMultiple 1
                        try
                            do! runtime.AttachAsync node
                        with e -> printfn "%A" e
                | Detatch ->
                    if nodeCount - 1 < conf.MinNodesCount then
                        printfn "Ignoring Detatch"
                    else
                        let node = runtime.Nodes.[random.Next(0, runtime.Nodes.Length-1)]
                        printfn "Detaching %A" node
                        try
                            do! runtime.DetachAsync node
                        with e -> printfn "%A" e

                let waitTime = random.Next(conf.MinSleepTime, conf.MaxKillCount + 1)                
                printfn "Waiting for %d seconds for next action..." waitTime
                do! Async.Sleep (waitTime*1000)

                return! chaosMonkey conf rest runtime
        }

        let defaultConf = 
            {
                MinSleepTime    = 2
                MaxSleepTime    = 10
                MinKillCount    = 1
                MaxKillCount    = 10
                MinNodesCount   = 3
                MaxNodesCount   = 10
            }
            
        let checkF (cexpr : Cloud<'T>) (result : 'T) =
            MBraceSettings.MBracedExecutablePath <- Path.Combine(Directory.GetCurrentDirectory(), "mbraced.exe")
            printfn "Using conf %A" defaultConf
            let quick = { FsCheck.Config.Quick with MaxTest = 10 }
            FsCheck.Check.One(quick, (fun (actions : ChaosAction list) ->
                printfn "%s" <| String.init 120 (fun _ -> "_")
                if List.isEmpty actions then
                    printfn "Ignoring empty action stack"
                else
                    printfn "Booting runtime"
                    let rt = MBrace.InitLocal(4, debug = true)
                    let chaos = chaosMonkey defaultConf actions rt
                    let cts = new CancellationTokenSource()
                    try 
                        printfn "Creating process"
                        let ps = rt.CreateProcess(cexpr)
                        printfn "Using action stack %A" actions
                        printfn "STARTING CHAOS MONKEY"
                        Async.Start(chaos, cts.Token)
                        ps.AwaitResult() |> should equal result
                        printfn "Process completed"
                    finally
                        cts.Cancel()
                        rt.Kill()
                        Process.GetProcessesByName("mbraced") 
                        |> Seq.iter (fun ps -> ps.Kill()) ))

        [<Test>]
        let ``Binary rec``() =
            let rec bin (depth : int)  = cloud {
                if depth = 0 then 
                    return 1
                else 
                    let! (l,r) = bin (depth-1) <||> bin (depth-1) 
                    return l + r
            }
         
            let i = 9
            checkF (bin i) (pown 2 i)
