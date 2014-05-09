namespace Nessos.MBrace.Runtime.Tests

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Core
    open Nessos.MBrace.Client

    open System
    open System.IO
    open System.Collections.Generic
    open Microsoft.FSharp.Quotations
    open NUnit.Framework
    open System.Net
    open System.Diagnostics

    open Nessos.Thespian
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Remote

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Store
    open Nessos.MBrace.Client
    open Nessos.MBrace.Client.ClientExtensions

    [<TestFixture>]
    type ``RunLocal`` =
        inherit TestBed

        val runtimeInfo: MBraceRuntime option ref

        new() = {
            inherit TestBed()

            runtimeInfo = ref None
        }

        override test.Name = "RunLocal"
        override test.Runtime = Unchecked.defaultof<_>
        override test.InitRuntime() = ()
        override test.FiniRuntime() = ()
        override test.SerializationTest () = ()

        override test.ExecuteExpression<'T>(expr: Expr<ICloud<'T>>): 'T =
            let cexpr = (expr |> Linq.RuntimeHelpers.LeafExpressionConverter.EvaluateQuotation) :?> ICloud<'T>
            MBrace.RunLocal cexpr

