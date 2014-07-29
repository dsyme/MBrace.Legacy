#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client

#r "MBrace.Azure.dll"

open Nessos.MBrace.Azure

let conn = System.IO.File.ReadAllText("/mbrace/azure.txt")
let azureProvider = StoreDefinition.Create<AzureStoreFactory>(conn)

let runtime = MBrace.InitLocal(3, masterPort = 2675)
//let runtime = MBrace.InitLocal(3, masterPort = 2675, storeProvider = azureProvider)
//let nodes = Node.SpawnMultiple(3, masterPort = 2675)
let n = MBraceNode.Spawn()
n.SetStoreConfiguration azureProvider


runtime.Shutdown()

runtime.Boot()

runtime.Nodes

runtime.Master

runtime.ShowSystemLogs()

runtime.Reboot()

runtime.Ping()

runtime.ShowInfo(true)

runtime.Run <@ cloud { return 42 } @>

runtime.Run (cloud { return 42 })

let p = runtime.CreateProcess <@ Cloud.Log "hi" @>

p.ShowLogs()


let x = ref 0
for i in 1 .. 2 do
    x := runtime.Run <@ cloud { return !x + 1 } @>

x.Value

let comp = Array.init 1000 (fun _ -> Cloud.Sleep 500 ) |> Cloud.Parallel

runtime.Run comp

runtime.ShowInfo()

runtime.ShowProcessInfo()

let p = runtime.GetProcess<unit []> 6521

p

