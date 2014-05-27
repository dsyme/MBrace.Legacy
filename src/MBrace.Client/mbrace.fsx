
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


#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client


//----------------------------------------------------------------------
// 

[<Cloud>]
type Cloud with
    [<Cloud>]
    static member Catch(computation : Cloud<'T>) : Cloud<Choice<'T, exn>> =
        cloud {
            try
                let! result = computation
                return Choice1Of2 result
            with ex ->
                return Choice2Of2 ex
        }

let rt = MBrace.InitLocal 4

[<Cloud>]
let foo =
    cloud {
        let a = cloud { return 42 }
        let b = cloud { return failwith "Foo" }
        let c = cloud { return 43 }
        return! Cloud.Parallel [| a;b;c |]
    }

rt.Run <@ foo |> Cloud.Catch @> 



[<Cloud>] 
module MFoo =
    let MBar = cloud { return 42 }


rt.Run <@ MFoo.MBar @>

MBrace.RunLocal(MFoo.MBar)


[<ReflectedDefinition>] 
type Foo  =
    static member Bar = 1 + 1

let mi = typeof<Foo>.GetMembers()
for mi in mi do
    printfn "%A" <| try Quotations.Expr.TryGetReflectedDefinition(mi :?> System.Reflection.MethodBase) with _ -> None

