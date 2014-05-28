
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

let runtime = MBrace.InitLocal 5
runtime.Run <@ cloud { return! Cloud.GetWorkerCount() } @>

let hello = cloud { return 42 }
[<Cloud>]
let bar = cloud { return! hello }

runtime.Run <@ bar @>

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


//----------------------------------------------------------------------
// 
#load "preamble.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

let rt = MBrace.InitLocal 4

while true do
    rt.Run <@ cloud {
                let x = ref 0
                let n = 200
                do! [|1..n|]
                    |> Array.map (fun i -> cloud { do x := i })
                    |> Cloud.Parallel
                    |> Cloud.Ignore
                return x.Value
            } @>
    |> printfn "%d"

while true do
    let x = rt.Run 
                <@ cloud {
                        let! x = MutableCloudRef.New(-1)
                        let! n = Cloud.GetWorkerCount()
                        let  n = 50 * n
                        let! v = 
                            [|1..n|] 
                            |> Array.map (fun i -> cloud { 
                                let! result = MutableCloudRef.Set(x, 1)
                                return x.GetHashCode(), result })
                            |> Cloud.Parallel
                            |> local
                        let f = Seq.filter snd v |> Seq.length
                        let d = Seq.map fst v |> Seq.distinct |> Seq.length
                        let x = Seq.length v
                        return f, d, x
                } @>
    printfn "%A" x


while true do
    let xs = rt.Run
                <@ cloud {
                    let! x = MutableCloudRef.New(-1)
                    return! [|1..10|]
                            |> Array.map (fun _ -> cloud { return x.GetHashCode() })
                            |> Cloud.Parallel
                } @>
              |> Seq.distinct |> Seq.length
    printfn "%d" xs


rt.Reboot()