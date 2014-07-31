#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client

#r "../../bin/MBrace.Azure.dll"
open Nessos.MBrace.Store
open Nessos.MBrace.Azure

let azureConn = System.IO.File.ReadAllText("/mbrace/azure.txt")
let azureStore = AzureStore.Create azureConn

MBraceSettings.SetDefaultStore azureStore
MBraceSettings.DefaultStore

let runtime = MBrace.InitLocal(3, masterPort = 2673)
//let runtime = MBrace.InitLocal(3, masterPort = 2673, store = azureStore)
//let nodes = Node.SpawnMultiple(3, masterPort = 2673)

let n = Node.Spawn()
n.SetStoreConfiguration azureStore
n.SetStoreConfiguration FileSystemStore.LocalTemp
runtime.Attach n


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