namespace Nessos.MBrace.Runtime.Interpreter

    open System
    open System.Reflection
    open System.Collections.Generic

    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns
    
    open Nessos.MBrace
    open Nessos.MBrace.CloudExpr
    open Nessos.MBrace.Runtime.Compiler

    /// Cloud process log abstraction

    type ICloudLogger = 
        abstract LogUserInfo  : message:string * taskId:TaskId -> unit
        abstract LogTraceInfo : message:string * taskId:TaskId * info:TraceInfo -> unit

    and TraceInfo = 
        { 
            Line        : int option
            File        : string option
            Function    : string option
            
            Environment : IDictionary<string, string>
        }

    type TaskConfiguration =
        {
            ProcessId : ProcessId
            TaskId : TaskId
            
            Logger : ICloudLogger
            Functions : FunctionInfo list
        }