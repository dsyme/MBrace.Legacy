
#load "preamble.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

let rt = MBrace.InitLocal 3




rt.Nodes.Length
let ns = Node.SpawnMultiple 1
rt.Attach ns
rt.Nodes.Length
