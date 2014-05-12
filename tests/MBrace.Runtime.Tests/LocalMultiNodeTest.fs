namespace Nessos.MBrace.Runtime.Tests

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Core
    open Nessos.MBrace.Client

    open System
    open System.IO
    open System.Collections.Generic
    open Microsoft.FSharp.Quotations
    open FsUnit
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
    open Nessos.MBrace.Runtime.Tests.TestFunctions

    [<TestFixture>]
    type ``Local MultiNode tests``() as test =
        inherit ``RunLocal tests``()

        [<DefaultValue>] val mutable internal runtimeInfo : MBraceRuntime option
        
        member this.Name with get () = "MultiNode"

        member test.ExecuteExpression<'T>(expr: Expr<ICloud<'T>>): 'T =
            let runtime = test.runtimeInfo.Value
            MBrace.RunRemote runtime expr
        
        member test.Runtime = test.runtimeInfo.Value

        [<TestFixtureSetUp>]
        member test.InitRuntime() =
            IoC.Register<ILogger>(Logger.createConsoleLogger, behaviour = Override)

            match test.runtimeInfo with
            | Some runtime -> runtime.Kill(); ConnectionPool.TcpConnectionPool.Fini()
            | _ -> ConnectionPool.TcpConnectionPool.Init()

            let runtime = MBraceRuntime.InitLocal(3, debug = true)

            test.runtimeInfo <- Some runtime
        
        [<TestFixtureTearDown>]
        member test.FiniRuntime() =
            let runtime = test.runtimeInfo.Value
            runtime.Shutdown()
            ()


        [<Test>]
        member test.SerializationTest () =
            shouldFailwith<MBraceException> (fun () -> <@ cloud { return new System.Net.HttpListener() } @> |> test.ExecuteExpression |> ignore)

        [<Test>] 
        member __.``Test Cloud Log`` () = 
            let ps = <@ cloud { do! Cloud.Log "Cloud Log Test Msg" } @> |> test.Runtime.CreateProcess
            Threading.Thread.Sleep(delay) // storelogger flushes every 2 seconds
            let dumps = test.Runtime.GetUserLogs(ps.ProcessId) 
            ()
//                dumps |> Seq.find(fun dump -> dump.Print().Contains("Cloud Log Test Msg")) |> ignore
            
        [<Test>] 
        member test.``Test Cloud Trace`` () = 
            let ps = <@ testTrace 1 @> |> test.Runtime.CreateProcess
            if test.Name = "MultiNode" then
                Threading.Thread.Sleep(delay)
                let dumps = test.Runtime.GetUserLogs(ps.ProcessId) 
                should equal true (traceHasValue dumps "a" "1")
                should equal true (traceHasValue dumps "x" "2")

        [<Test>] 
        member test.``Test Serialization Exception`` () = 
            test.SerializationTest()

        [<Test>]
        member test.``Test Cloud Trace Exception `` () = 
            let ps = <@ cloud { return 1 / 0 } |> Cloud.Trace @> |> test.Runtime.CreateProcess
            if test.Name = "MultiNode" then
                Threading.Thread.Sleep(delay)
                shouldFailwith<CloudException> (fun () -> ps.AwaitResult() |> ignore)
                let dumps = test.Runtime.GetUserLogs(ps.ProcessId)
                should equal true (dumps |> Seq.exists(function Trace info -> info.Message.Contains("DivideByZeroException") | _ -> false))
                
        [<Test>] 
        member test.``Test Cloud Trace handle Exception`` () = 
            let ps = <@ testTraceExc () @> |> test.Runtime.CreateProcess
            if test.Name = "MultiNode" then
                Threading.Thread.Sleep(delay)
                let dumps = test.Runtime.GetUserLogs(ps.ProcessId) 
                should equal true (traceHasValue dumps "ex" "error")

        [<Test>] 
        member test.``Test Cloud Trace For Loop`` () = 
            let ps = <@ testTraceForLoop () @> |> test.Runtime.CreateProcess
            if test.Name = "MultiNode" then
                Threading.Thread.Sleep(delay)
                let dumps = test.Runtime.GetUserLogs(ps.ProcessId) 
                should equal true (traceHasValue dumps "i" "1")
                should equal true (traceHasValue dumps "i" "2")
                should equal true (traceHasValue dumps "i" "3")

        [<Test>]
        member test.``Test Quotation Cloud Trace`` () = 
            let ps = <@ cloud { let! x = cloud { return 1 } in return x } |> Cloud.Trace @> |> test.Runtime.CreateProcess
            if test.Name = "MultiNode" then
                Threading.Thread.Sleep(delay)
                let dumps = test.Runtime.GetUserLogs(ps.ProcessId) 
                should equal false (traceHasValue dumps "x" "1")

        [<Test>]
        member test.``Test Cloud NoTraceInfo Attribute`` () = 
            let ps = <@ cloud { return! cloud { return 1 } <||> cloud { return 2 } } |> Cloud.Trace @> |> test.Runtime.CreateProcess
            if test.Name = "MultiNode" then
                Threading.Thread.Sleep(delay)
                let dumps = test.Runtime.GetUserLogs(ps.ProcessId)
                let result = 
                    dumps |> Seq.exists (function 
                                            | Trace info -> info.Function = Some ("op_LessBarBarGreater")
                                            | _ -> false)  
                should equal false result

        [<Test>]
        member test.``Test Parallel Cloud Trace`` () = 
            let proc = <@ testParallelTrace () @> |> test.Runtime.CreateProcess 
            proc.AwaitResult() |> ignore
            if test.Name = "MultiNode" then
                Threading.Thread.Sleep(delay) 
                let dumps = test.Runtime.GetUserLogs(proc.ProcessId) 
                should equal true (traceHasValue dumps "x" "2")
                should equal true (traceHasValue dumps "y" "3")
                should equal true (traceHasValue dumps "r" "5")

        [<Test; Repeat 10>]
        member test.``Z3 Test Kill Process - Process killed, process stops`` () =
            if test.Name = "MultiNode" then
                let mref = MutableCloudRef.New 0 |> MBrace.RunLocal
                let ps = 
                    <@ cloud {
                        while true do
                            do! Async.Sleep 100 |> Cloud.OfAsync
                            let! curval = MutableCloudRef.Read mref
                            do! MutableCloudRef.Force(mref, curval + 1)
                    } @> |> test.Runtime.CreateProcess

                test.Runtime.KillProcess(ps.ProcessId)
                Threading.Thread.Sleep(delay)
                let val1 = MutableCloudRef.Read mref |> MBrace.RunLocal
                Threading.Thread.Sleep(delay)
                let val2 = MutableCloudRef.Read mref |> MBrace.RunLocal

                should equal ps.Result ProcessResult.Killed
                should equal val1 val2
            else
                ()

        [<Test; Repeat 10>]
        member test.``Z4 Test Kill Process - Fork bomb`` () =
            if test.Name = "MultiNode" then
                let m = MutableCloudRef.New 0 |> MBrace.RunLocal
                let rec fork () : ICloud<unit> = 
                    cloud { do! MutableCloudRef.Force(m,1)
                            let! _ = fork() <||> fork() in return () }
                let ps = test.Runtime.CreateProcess <@ fork () @>
                Async.RunSynchronously <| Async.Sleep 4000
                test.Runtime.KillProcess(ps.ProcessId)
                Async.RunSynchronously <| Async.Sleep 2000
                MutableCloudRef.Force(m, 0) |> MBrace.RunLocal
                Async.RunSynchronously <| Async.Sleep 1000
                let v = MutableCloudRef.Read(m) |> MBrace.RunLocal
                should equal ProcessResult.Killed ps.Result
                should equal 0 v

        [<Test>]
        member __.``Test Fetch logs`` () =
            if test.Name = "MultiNode" then
                test.Runtime.GetLogs() |> Seq.isEmpty |> should equal false
                test.Runtime.Nodes.Head.GetLogs() |> Seq.isEmpty |> should equal false
