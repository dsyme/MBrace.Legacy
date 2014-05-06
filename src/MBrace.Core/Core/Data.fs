namespace Nessos.MBrace.Core

    [<AutoOpen>]
    module internal Data =

        open System

        open Microsoft.FSharp.Quotations

        open Nessos.MBrace

        type VariableName = String
        and Variable = VariableName * Type
        and FunctionName = String
        and ThunkId = String
        and ExprId = String
        and ElementType = Type
        and ReturnType = Type

        and Row = int
        and Col = int
        and File = string
        and FunctionInfo = 
            { 
                MethodInfo : System.Reflection.MethodInfo; 
                File : string; 
                StartRow : int; StartCol : int; 
                EndRow : int; EndCol : int; 
                Expr : Quotations.Expr 
            }

        and Function = FunctionInfo 
        and Dump = Dump of CloudExpr list
