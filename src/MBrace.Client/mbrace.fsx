#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Client.dll"

open Nessos.MBrace
open Nessos.MBrace.Client

let runtime = MBrace.InitLocal 3

runtime.Ping()

runtime.Nodes

let x = ref 0
for i = 1 to 10 do
    x := runtime.Run <@ cloud { return !x + 1 } @>

x.Value // 10