#load "bootstrap.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

[<Cloud>]
let beBusy howLong : Cloud<unit> = 
    cloud {
        do Async.RunSynchronously (Async.Sleep howLong)
    }
    |> Cloud.ToLocal

[<Cloud>]
let busyTask bussiness result = 
    cloud {
        do! beBusy bussiness
        return result
    }

module FaultTests =
    [<Cloud>]
    let coarseGrained distribFactor baseTime baseResult =
        cloud {
            let tasks = [| for i in 1..distribFactor -> busyTask (baseTime * i) (baseResult * i) |]
            
            let! results = Cloud.Parallel tasks

            return results
        }

module Utils =
    [<Cloud>]
    let twice cloudComp =
        cloud {
            let! r1 = cloudComp
            let! r2 = cloudComp
            return r1, r2
        }

//Init runtime
let runtime = MBrace.InitLocal 10

[<Cloud>]
let rec bin (depth : int)  = cloud {
    if depth = 0 then 
        return 1
    else 
        let! (l,r) = bin (depth-1) <||> bin (depth-1) 
        return l + r
}

let ps = runtime.CreateProcess <@ bin 8 @>
let ps = runtime.CreateProcess <@ bin 10 @>
let ps = runtime.CreateProcess <@ bin 4 @>


ps.AwaitResult()


for i in 1 .. 100 do
    let ps = runtime.CreateProcess <@ bin 10 @>
    ps.AwaitResult() |> ignore



let node = Node.Spawn()

runtime.Attach [ node ]

runtime.Detach node

//Test 1
//let res = runtime.Run <@ FaultTests.coarseGrained 4 10000 42 @>
let p = runtime.CreateProcess <@ FaultTests.coarseGrained 8 10000 42 @>


let p = runtime.CreateProcess <@ FaultTests.coarseGrained 6 10000 42 |> Utils.twice @>



open System
open System.Diagnostics
open System.Threading

type SpawnType = Same | Rand of int * int
type SpawnOnKill = Never | Always of SpawnType | FlipACoin of SpawnType

type ChaosMonkeyConfiguration = {
    MinKillWaitTime: int
    MaxKillWaitTime: int
    MinKillCount: int
    MaxKillCount: int
    SpawnNodesOnKill: SpawnOnKill
}

let chaosMonkey (conf: ChaosMonkeyConfiguration) (runtime: MBraceRuntime): Async<unit> = async {
    printfn "Chaos monkey: %A" conf

    let rec killLoop () = async {
        let random = new Random()
        let waitTime = random.Next(conf.MinKillWaitTime, conf.MaxKillWaitTime + 1)
        printfn "Waiting for %d seconds for next kills..." waitTime
        do! Async.Sleep (waitTime*1000)
        
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
            match conf.SpawnNodesOnKill with
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
        
        return! killLoop()
    }

    return! killLoop ()
}
    
let conf = {
    MinKillWaitTime = 10
    MaxKillWaitTime = 35
    MinKillCount = 1
    MaxKillCount = 1
    SpawnNodesOnKill = Always Same
}

let chaos = chaosMonkey conf runtime

let cts = new CancellationTokenSource()
Async.Start(chaos, cts.Token)

cts.Cancel()

for i in 1 .. 100 do
    printfn "Sending bin 10"
    let ps = runtime.CreateProcess <@ bin 10 @>
    ps.AwaitResult() |> ignore
    
    

let r = MBrace.InitLocal 3
for i = 1 to 100 do
    printfn "%A" <| r.Run <@ cloud { return 42 } @>
