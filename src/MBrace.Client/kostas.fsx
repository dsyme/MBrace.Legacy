
#load "preamble.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

let aqn  = "Nessos.MBrace.Azure.AzureStoreFactory, MBrace.Azure, Version=0.5.0.0, Culture=neutral, PublicKeyToken=null"
let conn =  System.IO.File.ReadAllText("/mbrace/azure.txt")
let azureProvider = StoreProvider.Parse(aqn, conn)
MBraceSettings.StoreProvider <- azureProvider


let rt = MBrace.InitLocal 3

rt.Reboot()

rt.Run <@ cloud { return 42 } @>

let ps = rt.CreateProcess <@ Cloud.Log "Hi" @>

ps.AwaitResult()

ps.ShowLogs()
