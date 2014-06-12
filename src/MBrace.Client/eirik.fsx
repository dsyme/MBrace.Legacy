#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client

let runtime = MBrace.InitLocal 4


runtime.Ping()

runtime.ShowInfo(true)

runtime.CreateProcess (cloud { return 42 })