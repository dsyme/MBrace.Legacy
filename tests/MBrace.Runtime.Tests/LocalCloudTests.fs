namespace Nessos.MBrace.Runtime.Tests

    open Nessos.MBrace
    open Nessos.MBrace.Client

    open System.IO

    open NUnit.Framework
    
    [<Category("LocalTests")>]
    type ``Local Cloud Tests`` () =
        inherit ``Cloud Tests`` ()

        override __.Name = "Local Cloud Tests"
        override __.IsLocalTesting = true

        override __.ExecuteExpression(expr : Quotations.Expr<Cloud<'T>>) : 'T =
            let cexpr = Swensen.Unquote.Operators.eval expr
            MBrace.RunLocal cexpr


    [<TestFixtureAttribute;Category("AppVeyor")>]
    type ``AppVeyor Tests`` () =
        [<TestAttribute>]
        member test.Foo () = 
            MBraceSettings.MBracedExecutablePath <- Path.Combine(Directory.GetCurrentDirectory(), "mbraced.exe")
            let node = MBraceNode.SpawnMultiple(1)
            node.Head.Ping() |> ignore