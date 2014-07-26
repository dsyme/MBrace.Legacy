namespace Nessos.MBrace.Runtime.Tests

    #nowarn "0444" // Disables compiler warnings emitted by MBrace API

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
            InitialNodeCount : int
            MinSleepTime     : int
            MaxSleepTime     : int
            MinKillCount     : int
            MaxKillCount     : int
            MinNodesCount    : int
            MaxNodesCount    : int
        }

        let logfn fmt = Printf.kprintf (printfn "%s : %s" <| DateTime.Now.ToShortTimeString()) fmt

        let rec chaosMonkey (conf: ChaosMonkeyConfiguration) 
                            (actions : ChaosAction list) 
                            (runtime: MBraceRuntime): Async<unit> = async {
            match actions with
            | [] -> 
                logfn "CHAOS MONKEY COMPLETED" 
            | action :: rest ->
                let random = new Random(System.DateTime.Now.Millisecond)
                logfn "Current action %A" action
                let nodeCount = runtime.Nodes.Length
                logfn "NodeCount %d" nodeCount

                try
                    match action with
                    | KillViolently spawnOnKill ->
                        let processes = runtime.Nodes |> List.map (fun n -> n.Process.Value) //Process.GetProcessesByName("mbraced")
                        let killCount = random.Next(conf.MinKillCount, conf.MaxKillCount + 1)
                        let killCount =
                            if nodeCount - killCount < conf.MinNodesCount 
                            then nodeCount - conf.MinNodesCount
                            else killCount

                        logfn "Killing %d processes" killCount
                        let k = ref killCount
                        while !k <> 0 do
                            let i = random.Next(0, processes.Length)
                            if not <| processes.[i].HasExited then
                                processes.[i].Kill()
                                logfn "Killed %A" processes.[i].Id
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

                            logfn "Spawning %d nodes..." spawnCount
                            let nodes = Node.SpawnMultiple spawnCount
                            logfn "Attaching nodes..."
                            do! runtime.AttachAsync nodes
                    | Attach ->
                        if nodeCount + 1 > conf.MaxNodesCount then 
                            logfn "Ignoring Attach"
                        else
                            let node = Node.SpawnMultiple 1
                            logfn "Attaching %A" <| Seq.exactlyOne  node
                            do! runtime.AttachAsync node
                    | Detatch ->
                        if nodeCount - 1 < conf.MinNodesCount then
                            logfn "Ignoring Detatch"
                        else
                            let node = runtime.Nodes.[random.Next(0, runtime.Nodes.Length-1)]
                            logfn "Detaching %A" node
                            do! runtime.DetachAsync node
                            node.Kill()
                with e -> logfn "Action failed with %A" e

                let waitTime = random.Next(conf.MinSleepTime, conf.MaxSleepTime + 1)                
                logfn "Waiting for %d seconds for next action..." waitTime
                do! Async.Sleep (waitTime*1000)

                return! chaosMonkey conf rest runtime
        }

        let defaultConf = 
            {
                InitialNodeCount = 5
                MinSleepTime     = 2
                MaxSleepTime     = 6
                MinKillCount     = 1
                MaxKillCount     = 10
                MinNodesCount    = 4
                MaxNodesCount    = 10
            }
            
        let checkF (cexpr : Cloud<'T>) (compare : 'T -> bool) =
            MBraceSettings.MBracedExecutablePath <- Path.Combine(Directory.GetCurrentDirectory(), "mbraced.exe")
            printfn "Using conf %A" defaultConf
            let quick = { Config.QuickThrowOnFailure with MaxTest = 10 }

            Check.One(quick, (fun (actions : ChaosAction list) ->
                if not <| List.isEmpty actions then
                    use cts = new CancellationTokenSource()
                    let mutable runtime = Unchecked.defaultof<_>
                    try 
                        printfn "\n%s" <| String.init 90 (fun _ -> "_")
                        logfn "Booting runtime..."
                        runtime <- MBrace.InitLocal(defaultConf.InitialNodeCount, debug = true)
                        logfn "done"
                        let chaos = chaosMonkey defaultConf actions runtime
                        logfn "Creating process"
                        let ps = runtime.CreateProcess(cexpr)
                        logfn "Using action stack %A" actions
                        logfn "STARTING CHAOS MONKEY"
                        Async.Start(chaos, cts.Token)
                        try
                            let r = ps.AwaitResult() 
                            logfn "Process completed : %A" r
                            compare r
                        with ex ->
                            logfn "AwaitResult failed with %A" ex
                            Assert.Fail(sprintf "%A" ex)
                            false
                    finally
                        cts.Cancel()
                        logfn "Cleaning processes"
                        Process.GetProcessesByName("mbraced") |> Array.iter (fun ps -> ps.Kill())
                else
                    true))

        [<Test>]
        let ``Binary rec``() =
            let rec bin (depth : int)  = cloud {
                if depth = 0 then 
                    return 1
                else 
                    let! (l,r) = bin (depth-1) <||> bin (depth-1) 
                    return l + r
            }
         
            let i = 11
            checkF (bin i) ((=) (pown 2 i))
