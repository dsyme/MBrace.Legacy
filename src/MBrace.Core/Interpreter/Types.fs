namespace Nessos.MBrace.Core

    open System
    open System.Reflection
    open System.Collections.Generic
    
    open Nessos.MBrace

    type ICloudLogger = 
        abstract LogUserInfo  : message:string -> unit
        abstract LogTraceInfo : message:string * info:TraceInfo -> unit

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

    and CoreConfiguration =
        {
            CloudRefProvider        : ICloudRefProvider
            MutableCloudRefProvider : IMutableCloudRefProvider
            CloudSeqProvider        : ICloudSeqProvider
            CloudFileProvider       : ICloudFileProvider

            Cloner : IObjectCloner
        }

    and FunctionInfo = 
        { 
            MethodInfo : System.Reflection.MethodInfo; 
            File : string; 
            StartRow : int; StartCol : int; 
            EndRow : int; EndCol : int; 
            Expr : Quotations.Expr 
        }

    and Dump = Dump of CloudExpr list


//    [<AutoOpen>]
//    module internal Data =
//
//        open System
//
//        open Microsoft.FSharp.Quotations
//
//        open Nessos.MBrace

//        type VariableName = String
//        and Variable = VariableName * Type
//        and FunctionName = String
//        and ThunkId = String
//        and ExprId = String
//        and ElementType = Type
//        and ReturnType = Type
//
//        and Row = int
//        and Col = int
//        and File = string
//
//        and Function = FunctionInfo 
        