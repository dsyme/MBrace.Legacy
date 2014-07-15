
#load "preamble.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

let aqn  = "Nessos.MBrace.Azure.AzureStoreFactory, MBrace.Azure, Version=0.5.0.0, Culture=neutral, PublicKeyToken=null"
let conn = System.IO.File.ReadAllText("/mbrace/azure.txt")
let azureProvider = StoreDefinition.Parse(aqn, conn)
MBraceSettings.StoreProvider <- azureProvider

let nodes = [1..3] 
            |> List.map (fun n -> sprintf "mbrace://10.0.1.%d:2675" (3+n)) 
            |> List.map (fun n -> MBraceNode(n))

nodes |> List.map (fun n -> n.Ping())
nodes |> List.iter (fun n -> n.ShowSystemLogs())


let rt = MBrace.Boot nodes //
let rt = MBrace.InitLocal 3

rt.Reboot()

rt.Run <@ cloud { return 42 } @>

let ps = rt.CreateProcess <@ Cloud.Log "Hi" @>

ps.AwaitResult()

ps.ShowLogs()

