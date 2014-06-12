
#load "preamble.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

let rt = MBrace.InitLocal 3


let [node] = Node.SpawnMultiple 1


rt.ShowInfo(true)
node.GetPerformanceCounters()
node.ShowPerformanceCounters()
