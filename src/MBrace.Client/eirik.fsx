#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client

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