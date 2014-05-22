namespace Nessos.MBrace.Core

    open System
    open System.Reflection
    open System.Collections.Generic
    
    open Nessos.MBrace

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

    /// Defines an object cloning abstraction
    and IObjectCloner =
        abstract Clone : 'T -> 'T

    and FunctionInfo = 
        { 
            MethodInfo : MethodInfo
            File : string
            StartRow : int ; StartCol : int
            EndRow   : int ; EndCol : int
            Expr : Quotations.Expr 
        }

    and TaskConfiguration =
        {
            ProcessId : ProcessId
            TaskId : TaskId
            
            Logger : ICloudLogger
            Functions : FunctionInfo list
        }