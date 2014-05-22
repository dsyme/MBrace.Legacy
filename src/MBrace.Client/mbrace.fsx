
#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client


MBrace.RunLocal <| CloudSeq.TryGet("foo")



[<Cloud>]
let test = cloud {
    let! count = Cloud.GetWorkerCount()

    let worker (i : int) = cloud {
        for j in [ 1 .. 100 ] do
            do! Cloud.Logf "worker %d message %d" i j
    }

    return! 
        [|1..count|] 
        |> Array.map worker 
        |> Cloud.Parallel
        |> Cloud.Ignore
}

MBrace.RunLocal(test, showLogs = true)

let runtime = MBrace.InitLocal 3

let proc = runtime.CreateProcess <@ test @>

proc.ShowLogs()

proc.ProcessId

runtime.Ping()

let logs = runtime.GetSystemLogs()
runtime.ShowSystemLogs()

runtime.Nodes

let x = ref 0
for i = 1 to 10 do
    x := runtime.Run <@ cloud { return !x + 1 } @>

x.Value // 10