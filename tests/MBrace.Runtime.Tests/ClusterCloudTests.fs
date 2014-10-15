namespace Nessos.MBrace.Runtime.Tests

    open System
    open System.IO
    open System.Threading

    open FsUnit
    open NUnit.Framework

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Client
    open Nessos.MBrace.Core.Tests

    #nowarn "0444" // Disable compiler warnings emitted by MBrace API
    

    [<ClusterTestsCategory>]
    type ``Cluster Cloud Tests``() =
        inherit ``Core Tests``()

        let currentRuntime : MBraceRuntime option ref = ref None
        
        override __.Name = "Cluster Cloud Tests"
        override __.IsLocalTesting = false
        override __.ExecuteExpression<'T>(expr: Quotations.Expr<Cloud<'T>>): 'T =
            MBrace.RunRemote __.Runtime expr

        member __.Runtime =
            match currentRuntime.Value with
            | None -> invalidOp "No runtime specified in test fixture."
            | Some r -> r

        [<TestFixtureSetUp>]
        member test.InitRuntime() =
            lock currentRuntime (fun () ->
                match currentRuntime.Value with
                | Some runtime -> runtime.Kill()
                | None -> ()
            
                MBraceSettings.MBracedExecutablePath <- Path.Combine(Directory.GetCurrentDirectory(), "mbraced.exe")
                MBraceSettings.DefaultTimeout <- 60 * 1000
                let runtime = MBraceRuntime.InitLocal(3, debug = true)
                currentRuntime := Some runtime)

        [<TestFixtureTearDown>]
        member test.FiniRuntime() =
            lock currentRuntime (fun () -> 
                match currentRuntime.Value with
                | None -> invalidOp "No runtime specified in test fixture."
                | Some r -> r.Shutdown() ; currentRuntime := None)



        //
        //  Section A : generic cluster tests
        //


        [<Test; ClusterTestsCategory()>]
        member t.``Z1. Cluster Tests: Cloud Log`` () = 
            let msg = "Cloud Log Test Msg"
            let ps = <@ cloud { do! Cloud.Log msg } @> |> t.Runtime.CreateProcess
            ps.AwaitResult()
            let dumps = ps.GetLogs()
            dumps.Length |> should equal 1
            (dumps |> Seq.head).Message |> should equal msg

        [<Test; ClusterTestsCategory()>]
        member t.``Z1. Cluster Tests: Cloud Logf - multiple`` () =
            let computation =
                cloud {
                    let taskF (_ : int) = cloud {
                        for i in [1 .. 20] do
                            do! Cloud.Sleep 10
                            do! Cloud.Logf "msg = %d" i
                    }

                    do!
                        [1..5]
                        |> Seq.map taskF
                        |> Cloud.Parallel
                        |> Cloud.Ignore

                    do! Cloud.Sleep 100
                }

            let ps = t.Runtime.CreateProcess computation

            ps.AwaitResult()

            let logs = ps.GetLogs()

            logs.Length |> should equal 100

        [<Test; ClusterTestsCategory()>]
        member t.``Z1. Cluster Tests: Cloud Log - delete`` () = 
            let msg = "Cloud Log Test Msg"
            let ps = <@ cloud { do! Cloud.Log msg } @> |> t.Runtime.CreateProcess
            ps.AwaitResult()
            Seq.isEmpty (ps.GetLogs()) |> should equal false
            ps.DeleteLogs()
            ps.GetLogs() |> should equal Seq.empty
            
            
        [<Test; ClusterTestsCategory()>]
        member test.``Z1. Cluster Tests: Cloud Trace`` () = 
            let ps = <@ testTrace 1 @> |> test.Runtime.CreateProcess
            ps.AwaitResult() |> ignore
            let dumps = ps.GetLogs()
            ()
            should equal true (traceHasValue dumps "a" "1")
            should equal true (traceHasValue dumps "x" "2")

        [<Test; ClusterTestsCategory()>]
        member test.``Z1. Cluster Tests: Cloud Trace Exception`` () = 
            let ps = <@ cloud { return 1 / 0 } |> Cloud.Trace @> |> test.Runtime.CreateProcess
            shouldFailwith<CloudException> (fun () -> ps.AwaitResult() |> ignore)
            let dumps = ps.GetLogs()
            should equal true (dumps |> Seq.exists(fun e -> e.Message.Contains("DivideByZeroException")))
                
        [<Test; ClusterTestsCategory()>]
        member test.``Z1. Cluster Tests: Cloud Trace handle Exception`` () = 
            let ps = <@ testTraceExc () @> |> test.Runtime.CreateProcess
            ps.AwaitResult() |> ignore
            let dumps = ps.GetLogs()
            should equal true (traceHasValue dumps "ex" "error")

        [<Test; ClusterTestsCategory()>]
        member test.``Z1. Cluster Tests: Cloud Trace For Loop`` () = 
            let ps = <@ testTraceForLoop () @> |> test.Runtime.CreateProcess
            ps.AwaitResult() |> ignore
            let dumps = ps.GetLogs()
            should equal true (traceHasValue dumps "i" "1")
            should equal true (traceHasValue dumps "i" "2")
            should equal true (traceHasValue dumps "i" "3")

        [<Test; ClusterTestsCategory()>]
        member test.``Z1. Cluster Tests: Cloud Trace Quotation`` () = 
            let ps = <@ cloud { let! x = cloud { return 1 } in return x } |> Cloud.Trace @> |> test.Runtime.CreateProcess
            ps.AwaitResult() |> ignore
            let dumps = ps.GetLogs()
            should equal false (traceHasValue dumps "x" "1")

        [<Test; ClusterTestsCategory()>]
        member test.``Z1. Cluster Tests: Cloud NoTraceInfo Attribute`` () = 
            let ps = <@ cloud { return! cloud { return 1 } <||> cloud { return 2 } } |> Cloud.Trace @> |> test.Runtime.CreateProcess
            ps.AwaitResult() |> ignore
            let dumps = ps.GetLogs()
            let result = 
                dumps 
                |> Seq.exists 
                    (fun e -> 
                        match e.TraceInfo with
                        | Some i -> i.Function = Some ("op_LessBarBarGreater")
                        | None -> false)

            should equal false result

        [<Test; ClusterTestsCategory()>]
        member test.``Z1. Cluster Tests: Parallel Cloud Trace`` () = 
            let proc = <@ testParallelTrace () @> |> test.Runtime.CreateProcess 
            proc.AwaitResult() |> ignore
            let dumps = proc.GetLogs()
            should equal true (traceHasValue dumps "x" "2")
            should equal true (traceHasValue dumps "y" "3")
            should equal true (traceHasValue dumps "r" "5")

        [<Test; ClusterTestsCategory() ; Repeat 2>]
        member test.``Z1. Cluster Tests: Process killed, process stops`` () =
            let mref = MutableCloudRef.New 0 |> MBrace.RunLocal
            let ps = 
                <@ cloud {
                    while true do
                        do! Async.Sleep 100 |> Cloud.OfAsync
                        let! curval = MutableCloudRef.Read mref
                        do! MutableCloudRef.Force(mref, curval + 1)
                } @> |> test.Runtime.CreateProcess

            test.Runtime.KillProcess(ps.ProcessId)
            let val1 = MutableCloudRef.Read mref |> MBrace.RunLocal
            let val2 = MutableCloudRef.Read mref |> MBrace.RunLocal

            ps.Result |> should equal ProcessResult<unit>.Killed
            should equal val1 val2

        [<Test; ClusterTestsCategory() ; Repeat 5>]
        member test.``Z1. Cluster Tests: Kill Process - Fork bomb`` () =
            let m = MutableCloudRef.New 0 |> MBrace.RunLocal

            let ps = 
                test.Runtime.CreateProcess 
                    <@ 
                        let rec fork () = cloud { 
                            do! MutableCloudRef.Force(m,1)
                            let! _ = fork() <||> fork() 
                            return () } in 
                        
                        fork () 
                    @>

            Thread.Sleep 4000
            test.Runtime.KillProcess(ps.ProcessId)
            Thread.Sleep 2000
            MutableCloudRef.Force(m, 0) |> MBrace.RunLocal
            Thread.Sleep 1000
            let v = MutableCloudRef.Read(m) |> MBrace.RunLocal
            ps.Result |> should equal ProcessResult<unit>.Killed
            should equal 0 v

        [<Test; Repeat 5>]
        member test.``Z1. Cluster Tests: Parallel with exception cancellation`` () =
            let flag = MutableCloudRef.New false |> MBrace.RunLocal
            test.Runtime.Run <@ testParallelWithExceptionCancellation flag @>
            let v = MutableCloudRef.Read flag |> MBrace.RunLocal
            v |> should equal true

        [<Test; ClusterTestsCategory()>]
        member test.``Z1. Cluster Tests: Process.StreamLogs`` () =
            let ps =
                <@ cloud {
                        for i in [|1..10|] do
                            do! Cloud.Logf "i = %d" i 
                } @> |> test.Runtime.CreateProcess
            ps.AwaitResult()
            ps.StreamLogs()

        [<Test; ClusterTestsCategory()>]
        member test.``Z1. Cluster Tests: Concurrent process creation`` () =
            let cloudJob () = cloud {
                let! n = Cloud.GetWorkerCount()
                let! results = Array.init (2*n) (fun i -> cloud { return i * i }) |> Cloud.Parallel
                return Array.sum results
            }

            // create 21 processes, in groups of 3 concurrent requests

            let procs =
                Array.init 7 (fun _ ->
                    Array.Parallel.init 3 (fun _ -> test.Runtime.CreateProcess (cloudJob())))
                |> Array.concat

            // run early tests on proc objects
            procs |> Seq.distinctBy (fun p -> p.ProcessId) |> Seq.length |> should equal 21

            let results = 
                procs 
                |> Array.map (fun p -> p.AwaitResultAsync()) 
                |> Async.Parallel
                |> Async.RunSynchronously

            results |> Seq.distinct |> Seq.length |> should equal 1

        //
        //  Secion B: Runtime administration
        //


        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Fetch logs`` () =
            __.Runtime.GetSystemLogs() |> Seq.isEmpty |> should equal false
            __.Runtime.Nodes.Head.GetSystemLogs() |> Seq.isEmpty |> should equal false

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Process delete container`` () =
            let ps = __.Runtime.CreateProcess <@ cloud { return! CloudRef.New(42) } @>
            ps.AwaitResult() |> ignore
            ps.DeleteContainer()

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Ping the Runtime``() =
            should greaterThan TimeSpan.Zero (__.Runtime.Ping())
            
        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Get Runtime Status`` () =
            __.Runtime.Active |> should equal true

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Get Runtime Information`` () =
            __.Runtime.ShowInfo ()

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Get Runtime Performance Information`` () =
            __.Runtime.ShowInfo (true)

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Get Runtime Deployment Id`` () =
            __.Runtime.Id |> ignore

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Get Runtime Nodes`` () =
            __.Runtime.Nodes |> Seq.length |> should greaterThan 0

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Get Master Node`` () =
            __.Runtime.Master |> ignore

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Get Alt Nodes`` () =
            __.Runtime.Alts |> ignore

        [<Test;RuntimeAdministrationCategory; ExpectedException(typeof<Nessos.MBrace.NonExistentObjectStoreException>)>]
        member __.``Z2. Cluster Admin: Delete container`` () =
            let s = __.Runtime.Run <@ CloudSeq.New([1..10]) @> 
            __.Runtime.GetStoreClient().DeleteContainer(s.Container) 
            Seq.toList s |> ignore

        [<Test; Repeat 4;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Reboot runtime`` () =
            __.Runtime.Reboot()
            __.Runtime.Run <@ cloud { return 1 + 1 } @> |> should equal 2

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Attach Node`` () =
            let n = __.Runtime.Nodes |> List.length
            __.Runtime.AttachLocal 1

            let n' = __.Runtime.Nodes |> List.length 
            n' - n |> should equal 1

            __.Runtime.Run <@ cloud { return 1 + 1 } @> |> should equal 2

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Attach existing Node`` () =
            let n = __.Runtime.Nodes |> List.length
            try
                __.Runtime.Attach(__.Runtime.Nodes |> Seq.last)
            with ex ->
                ex :? MBraceException |> should equal true //not sure about that, but currently throws timeoutexception
                __.Runtime.Nodes.Length |> should equal n

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Detach Node`` () =
            let nodes = __.Runtime.Nodes 
            let n = nodes.Length
            let node2 = nodes.[1] 
            __.Runtime.Detach node2 

            let n' =__.Runtime.Nodes |> List.length
            n - n' |> should equal 1

            __.Runtime.Run <@ cloud { return 1 + 1 } @> |> should equal 2

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Detach Node not contained in cluster`` () =
            let n = __.Runtime.Nodes |> List.length
            let node = Node.Spawn()
            try
                __.Runtime.Detach(node)
            with ex ->
                ex :? MBraceException |> should equal true 
                __.Runtime.Nodes.Length |> should equal n

        [<Test;RuntimeAdministrationCategory>]
        member __.``Z2. Cluster Admin: Node Permissions`` () =
            let nodes = __.Runtime.Nodes 
            let n = nodes.Head

            // Node client caches information for a few milliseconds
            // sleep to force that values are up to date

            let p = n.Permissions

            n.Permissions <- Permissions.None
            do Thread.Sleep 500
            n.Permissions |> should equal Permissions.None

            n.Permissions <- Permissions.Slave
            do Thread.Sleep 500
            n.Permissions |> should equal Permissions.Slave

            n.Permissions <- p
            do Thread.Sleep 500
            n.Permissions |> should equal p