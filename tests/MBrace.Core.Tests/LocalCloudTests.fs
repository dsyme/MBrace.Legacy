namespace Nessos.MBrace.Core.Tests

    open Nessos.MBrace
    open Nessos.MBrace.Client

    open NUnit.Framework
    
    [<LocalTestsCategory>]
    type ``Local Cloud Tests`` () =
        inherit ``Core Tests`` ()

        override __.Name = "Local Cloud Tests"
        override __.IsLocalTesting = true

        override __.ExecuteExpression(expr : Quotations.Expr<Cloud<'T>>) : 'T =
            MBrace.RunLocal expr