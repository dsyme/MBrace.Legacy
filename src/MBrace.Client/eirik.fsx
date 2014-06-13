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