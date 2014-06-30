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

runtime.ShowInfo(true)

runtime.Run <@ cloud { return 42 } @>