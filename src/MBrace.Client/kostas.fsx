
#load "preamble.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

//let aqn  = "Nessos.MBrace.Azure.AzureStoreFactory, MBrace.Azure, Version=0.5.0.0, Culture=neutral, PublicKeyToken=null"
//let conn = System.IO.File.ReadAllText("/mbrace/azure.txt")
//let azureProvider = StoreDefinition.Parse(aqn, conn)
//MBraceSettings.StoreProvider <- azureProvider
//
//let nodes = [1..3] 
//            |> List.map (fun n -> sprintf "mbrace://10.0.1.%d:2675" (3+n)) 
//            |> List.map (fun n -> MBraceNode(n))
//
//nodes |> List.map (fun n -> n.Ping())
//nodes |> List.iter (fun n -> n.ShowSystemLogs())

let rt = MBrace.InitLocal 3

let rt = MBrace.Connect("Artemis",49204)

rt.Run <@ cloud { 
            let! cf = CloudFile.Create(fun stream -> async { stream.WriteByte(10uy) })
            return! CloudFile.Read(cf, fun stream -> async { stream.WriteByte(20uy) })
        } @>

let ps = rt.GetProcess(254)
ps.AwaitBoxedResult()

open Nessos.MBrace.Lib.Concurrency

type RuntimeInfo = { Uri : System.Uri }
type ProcessInfo = { Id : ProcessId; RuntimeInfo : RuntimeInfo }

let load () = 
    System.Reflection.Assembly.Load("MBrace.Client, Version=0.5.0.0, Culture=neutral, PublicKeyToken=null").GetTypes() |> ignore

let exec (rti : RuntimeInfo) (action : Quotations.Expr<Cloud<'T>>)   = 
    async {
        //load ()
        let! rt = MBrace.ConnectAsync(rti.Uri)
        let ps = rt.CreateProcess(action)
        return { Id = ps.ProcessId; RuntimeInfo = rti }
    }

[<Cloud>]
let spawn (rti : RuntimeInfo) (action : Quotations.Expr<Cloud<'T>>) : Cloud<ProcessInfo> =
    Cloud.OfAsync <| exec rti action
//    cloud.Return <| { Id = -1
//                      RuntimeInfo = { Uri = null } }

[<Cloud>]
let rec loop factor (chan : Channel<int>) =
    cloud {
        let! value = Channel.read chan
        do! Channel.write chan (value * factor)
        return! loop factor chan
    }

[<Cloud>]
let main (rti : RuntimeInfo) = 
    cloud {
        //load ()
        let! c1 = Channel.newEmpty<int>
        let! c2 = Channel.newEmpty<int>
        do! spawn rti <@ loop 2 c1 @> |> Cloud.Ignore
        do! spawn rti <@ loop 3 c2 @> |> Cloud.Ignore

        let rec writer cnt (ch : Channel<int>) : Cloud<unit> = 
            cloud { 
                do! Cloud.Sleep 1000
                do! Channel.write ch cnt
                return! writer (cnt + 1) ch
            }

        do! spawn rti <@ writer 0 c1 @> |> Cloud.Ignore
        do! spawn rti <@ writer 0 c2 @> |> Cloud.Ignore

        return c1, c2
    }

let rt = MBrace.InitLocal 3

let rti = { Uri = rt.Master.Uri }
let c1, c2 = rt.Run <@ main rti @>



rt.Run  <@ cloud.Return <| (System.Reflection.Assembly.Load("MBrace.Client, Version=0.5.0.0, Culture=neutral, PublicKeyToken=null").GetTypes() |> Array.map (fun t -> t.Name)) @>

let u = rt.Master.Uri

let f (u : System.Uri) = 
    let rt = MBrace.Connect(u)
    let cc = MBrace.Compile(cloud { return 42 })
    rt.Run cc

rt.Run <@ cloud { return f u } @>

MBrace.RunLocal <@ Channel.read c1 @>
MBrace.RunLocal <@ Channel.read c2 @>
MBrace.RunLocal <@ Channel.read c2 @>


rt.Reboot()