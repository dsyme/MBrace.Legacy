#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client

let aqn  = "Nessos.MBrace.Azure.AzureStoreFactory, MBrace.Azure, Version=0.5.0.0, Culture=neutral, PublicKeyToken=null"
let conn = "DefaultEndpointsProtocol=https;AccountName=mbraceclusterstorage;AccountKey=cq2knJyPSCP9uNcyDPbFAgHyiPpJVMcR/59yN2RW9uNmrHJyT4ZwdLYxCXuUo6w5xJ7iMjKy0+WxQQ+f2nSseQ=="
let azureProvider = StoreProvider.Parse(aqn, conn)
//MBraceSettings.StoreProvider <- azureProvider

let n = Node("mbrace://grothendieck:2675")
n.Ping()

n.SetStoreConfiguration azureProvider

let runtime = MBrace.InitLocal 3

runtime.Shutdown()

runtime.Boot()

runtime.Nodes

runtime.Master

runtime.ShowSystemLogs()

runtime.Reboot()

runtime.Ping()

runtime.ShowInfo(true)

runtime.Run <@ cloud { return 42 } @>

let p = runtime.CreateProcess <@ Cloud.Log "hi" @>

p.ShowLogs()