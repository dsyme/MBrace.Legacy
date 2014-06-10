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

    [<TestFixture; Category("ChaosMonkeyTests")>]
    module ``Chaos Monkey Tests`` =
        type SpawnType = Same | Rand of int * int
        type SpawnOnKill = Never | Always of SpawnType | FlipACoin of SpawnType

        type ChaosAction =
            | Attach 
            | Detatch
            | KillViolently of SpawnOnKill
            | Nothing

        type ChaosMonkeyConfiguration = {
            MinKillWaitTime : int
            MaxKillWaitTime : int
            MinKillCount    : int
            MaxKillCount    : int
            Actions         : ChaosAction list
        }

        let rec chaosMonkey (conf: ChaosMonkeyConfiguration) (runtime: MBraceRuntime): Async<unit> = async {
            match conf.Actions with
            | [] -> 
                return ()
            | action :: rest ->
                let random = new Random(System.DateTime.Now.Millisecond)
                let waitTime = random.Next(conf.MinKillWaitTime, conf.MaxKillWaitTime + 1)
                printfn "Waiting for %d seconds for next action..." waitTime
                do! Async.Sleep (waitTime*1000)
            
                match action with
                | KillViolently spawnOnKill ->
                    let processes = Process.GetProcessesByName("mbraced")
                    let killCount = random.Next(conf.MinKillCount, conf.MaxKillCount + 1)
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
                        printfn "Spawning %d nodes..." spawnCount
                        let nodes = Node.SpawnMultiple spawnCount
                        printfn "Attaching nodes..."
                        try
                            do! runtime.AttachAsync nodes
                        with e -> printfn "%A" e
                | Attach ->
                    let node = Node.SpawnMultiple 1
                    try
                        do! runtime.AttachAsync node
                    with e -> printfn "%A" e
                | Detatch ->
                    let node = runtime.Nodes.[random.Next(0, runtime.Nodes.Length-1)]
                    try
                        do! runtime.DetachAsync node
                    with e -> printfn "%A" e
                | Nothing ->
                    ()
                return! chaosMonkey {conf with Actions = rest } runtime
        }

        [<Cloud>]
        let rec bin (depth : int)  = cloud {
            if depth = 0 then 
                return 1
            else 
                let! (l,r) = bin (depth-1) <||> bin (depth-1) 
                return l + r
        }

        [<Test>]
        let ``Binary rec``() =
            let conf = {
                MinKillWaitTime = 10
                MaxKillWaitTime = 35
                MinKillCount = 1
                MaxKillCount = 1
                Actions = [Nothing] // Create with fscheck
                }

            do MBraceSettings.MBracedExecutablePath <- Path.Combine(Directory.GetCurrentDirectory(), "mbraced.exe")
            let rt = MBrace.InitLocal(3, debug = true)


            let chaos = chaosMonkey conf rt
            let cts = new CancellationTokenSource()
            Async.Start(chaos, cts.Token)

            rt.Run <@ bin 10 @> |> should equal 1024
            cts.Cancel()
