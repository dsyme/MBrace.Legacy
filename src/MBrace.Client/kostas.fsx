
#load "preamble.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client


let rt = MBrace.InitLocal 3




rt.Nodes.Length

let ns = Node.SpawnMultiple 2
ns |> Seq.map (fun n -> n.DeploymentId)


rt.Attach ns
rt.Nodes.Length


#r "System.Management"
open System
open System.Management
