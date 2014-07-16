
#load "../packages/MBrace.Runtime.0.5.0/preamble.fsx" 

open Nessos.MBrace
open Nessos.MBrace.Client

let runtime = MBrace.InitLocal 3

let ps = runtime.CreateProcess <@ cloud { return "Hello world" } @>

ps.AwaitResult()
