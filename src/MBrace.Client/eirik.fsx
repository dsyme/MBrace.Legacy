
#load "preamble.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client


let nodes = Node.SpawnMultiple 3
//let runtime = MBrace.Boot nodes

let runtime = MBrace.InitLocal 3

let n = nodes.Head

let p = n.Permissions

n.Permissions <- Permissions.None
n.Permissions
n.Permissions <- p