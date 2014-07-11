namespace Nessos.MBrace.Runtime.Compiler

    open System
    open System.Reflection

    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.Patterns


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