namespace Nessos.MBrace.Runtime
    
    open System
    open System.IO
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Utils

    type ILogStore =
        abstract LogEntry           : ProcessId * LogEntry  -> unit
        abstract LogEntryAndFlush   : ProcessId * LogEntry  -> unit
        abstract Flush              : unit -> unit
        abstract DumpLogs           : ProcessId -> Async<LogEntry []> 
        abstract DumpLogs           : unit -> Async<LogEntry []> 
        abstract DeleteLogs         : ProcessId -> Async<unit> 
        abstract DeleteLogs         : unit -> Async<unit> 