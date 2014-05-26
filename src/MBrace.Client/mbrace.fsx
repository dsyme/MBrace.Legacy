
#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client


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


#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client


let rt = MBrace.InitLocal 4

[<Cloud>]
let rec f i : Cloud<unit> = cloud {
        if i > 100 then return ()
        else
            do! Cloud.OfAsync <| Async.Sleep 100
            do! Cloud.Logf "i = %d" i
            return! f (i+1)
}

let proc = rt.CreateProcess <@ f 0 @>
proc.StreamLogs()

proc.ShowLogs() 


//----------------------------------------------------------------------
// Attach bug

let rt = MBrace.InitLocal 3
printfn "%d" rt.Nodes.Length
rt.AttachLocal(1)
printfn "%d" rt.Nodes.Length
let ps = <@ cloud { return "foo" } @> |> rt.CreateProcess
ps.AwaitResult()