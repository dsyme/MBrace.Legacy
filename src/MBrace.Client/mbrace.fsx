
#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client

let runtime = MBrace.InitLocal 3

let n = MBraceNode.SpawnMultiple 3

runtime.Ping()

runtime.GetLogs()

runtime.Nodes

let x = ref 0
for i = 1 to 10 do
    x := runtime.Run <@ cloud { return !x + 1 } @>

x.Value // 10