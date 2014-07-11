namespace Nessos.MBrace.Runtime.Interpreter

    open System
    open System.Reflection
    open System.Collections.Generic

    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns
    
    open Nessos.MBrace
    open Nessos.MBrace.CloudExpr

    /// Parsed version of Expr.CustomAttributes

    type ExprMetadata =
        {
            File : string
            StartRow : int ; StartCol : int
            EndRow   : int ; EndCol : int
        }
    with
        static member TryParse(expr : Expr) =
            match expr.CustomAttributes with
            | [ NewTuple [_; NewTuple [Value (file, _); Value (srow, _); Value (scol, _); Value (erow, _); Value(ecol, _)]] ] -> 
                Some { 
                    File = file :?> string

                    StartRow = srow :?> int ;   StartCol = scol :?> int
                    EndRow   = erow :?> int ;   EndCol = ecol :?> int    
                }
            | _ -> None

    and FunctionInfo = 
        {
            Source : Choice<MethodInfo, PropertyInfo>
            Metadata : ExprMetadata
            Expr : Quotations.Expr

            IsCloudExpression : bool
        }
    with
        member fi.FunctionName =
            match fi.Source with
            | Choice1Of2 m -> m.Name
            | Choice2Of2 p -> p.Name

        member fi.MethodInfo =
            match fi.Source with
            | Choice1Of2 m -> m
            | Choice2Of2 p -> p.GetGetMethod(true)

        member fi.IsProperty =
            match fi.Source with
            | Choice1Of2 _ -> false
            | Choice2Of2 _ -> true



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