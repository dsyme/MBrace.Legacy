#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client

let runtime = MBrace.InitLocal 4

runtime.Shutdown()

runtime.Boot()

runtime.Nodes

runtime.Master

runtime.Reboot()

runtime.Ping()

let n = Node.Spawn()
n.ShowInfo(true)

runtime.Run <@ cloud { return 42 } @>

let nodes = runtime.Nodes

runtime.Shutdown()
let runtime' = MBrace.Boot nodes

let client = runtime'.GetStoreClient()
client.StoreProvider

let n0 = MBraceNode.Spawn()

runtime.Attach [n0]