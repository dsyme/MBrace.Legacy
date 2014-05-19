namespace Nessos.MBrace.Runtime.Tests

    open Nessos.MBrace
    open Nessos.MBrace.Client
    
    type ``Local Cloud Tests`` () =
        inherit ``Cloud Tests`` ()

        override __.Name = "Local Cloud Tests"
        override __.IsLocalTesting = true

        override __.ExecuteExpression(expr : Quotations.Expr<Cloud<'T>>) : 'T =
            let cexpr = Swensen.Unquote.Operators.eval expr
            MBrace.RunLocal cexpr