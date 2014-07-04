#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client

#r "MBrace.Azure.dll"

open Nessos.MBrace.Azure

let conn = System.IO.File.ReadAllText("/mbrace/azure.txt")
let azureProvider = StoreProvider.Define<AzureStoreFactory>(conn)

//let runtime = MBrace.InitLocal(3)
let runtime = MBrace.InitLocal(3, storeProvider = azureProvider)

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


let x = ref 42

runtime.Run <@ cloud { return x.Value } @>