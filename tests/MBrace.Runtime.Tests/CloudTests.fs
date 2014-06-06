#nowarn "0044" // 'While loop considered harmful' message.

namespace Nessos.MBrace.Runtime.Tests

    open System
    open System.IO
    open Microsoft.FSharp.Quotations

    open NUnit.Framework
    open FsUnit

    open Nessos.MBrace
    open Nessos.MBrace.Client
    open Nessos.MBrace.Lib
    open Nessos.MBrace.Lib.Concurrency

    [<TestFixture>]
    [<AbstractClass>]
    type ``Cloud Tests`` () =

        abstract Name : string
        abstract IsLocalTesting : bool
        abstract ExecuteExpression : Expr<Cloud<'T>> -> 'T


        [<Test>] 
        member test.``simple values`` () =
            <@ cloud { return 1 } @> |> test.ExecuteExpression |> should equal 1

        [<Test>] 
        member test.``arithmetic`` () =
            <@ cloud { return 1 + 1 } @> |> test.ExecuteExpression |> should equal 2

        [<Test>] 
        member test.``let binding `` () =
            <@ cloud { let x = 1 in return x + 1 } @> |> test.ExecuteExpression |> should equal 2

        
        [<Test>] 
        member test.``let private binding `` () =
            let testPrivate = 42 
            <@ cloud { return testPrivate } @> |> test.ExecuteExpression |> should equal 42

        [<Test>] 
        member test.``let rec binding `` () =
            <@ let rec f n = cloud { if n <= 0 then return 1 else let! p = f (n-1) in return n * p } in f 2 @>
            |> test.ExecuteExpression |> should equal 2

        [<Test>] 
        member test.``variable hiding`` () =
            <@ cloud { let x = 1 in return (fun x -> x + 1) (x + 1) } @> |> test.ExecuteExpression |> should equal 3

        [<Test>] 
        member test.``variable binding with null and ()`` () =
            <@ (fun (x : string) -> cloud { return x } ) null @> |> test.ExecuteExpression |> should equal null;
            <@ (fun x -> cloud { return x } ) () @> |> test.ExecuteExpression |> should equal ();

        [<Test>] 
        member test.``customer object`` () = 
            <@ cloud { return new Foo(42) } @> |> test.ExecuteExpression |> (fun x -> should equal 42 <| x.Get() )

        [<Test>] 
        member test.``curry uncurry calls`` () = 
            <@ add 1 2  @> |> test.ExecuteExpression |> should equal 3;
            <@ addUnCurry (1, 2) @> |> test.ExecuteExpression |> should equal 3;

        [<Test>] 
        member test.``Custom Recursive Type`` () =
            <@ peanoAddition largePeano Zero @> |> test.ExecuteExpression |> should equal largePeano |> ignore ;
            <@ peanoTest 17 25 @> |> test.ExecuteExpression |> should equal 42


        [<Test>] 
        member test.``Ackermann Function`` () =
            <@ ackermann 3 4 @> |> test.ExecuteExpression |> should equal 125
        
        [<Test>] 
        member test.``McCarthy 91 Function`` () =
            <@ mcCarthy91 50 @> |> test.ExecuteExpression |> should equal 91;
            <@ mcCarthy91 150 @> |> test.ExecuteExpression |> should equal 140;

        [<Test>] 
        member test.``let mutable binding `` () =
            <@ cloud { 
                    let mutable x = 1
                    x <- x + 1
                    return x 
               } @> |> test.ExecuteExpression |> should equal 2 
            
        [<Test>] 
        member test.``.Net calls `` () =
            <@ cloud { return Int32.Parse("1") } @> |> test.ExecuteExpression |> should equal 1;
            <@ cloud { return (1 + 1).ToString() } @> |> test.ExecuteExpression |> should equal "2"

        [<Test>] 
        member test.``Array comprehension `` () =
            <@ cloud { return [| for i = 1 to 3 do yield i * 2 |] } @>
            |> test.ExecuteExpression |> should equal [|2; 4; 6|]

        [<Test>] 
        member test.``CloudAttribute `` () =
            <@ add 2 3 @> |> test.ExecuteExpression |> should equal 5

        [<Test>] 
        member test.``CloudAttribute on PropertyGet`` () =
            <@ testPropGet @> |> test.ExecuteExpression |> should equal 42

        [<Test>] 
        member test.``Bind `` () = 
            <@ cloud { let! a = cloud { return 1 } in return a + 1 } @> |> test.ExecuteExpression |> should equal 2

        [<Test>] 
        member test.``Parallel `` () =
            let result : int[] = <@ cloud { return! Cloud.Parallel [|cloud { return 1 }; cloud { return 2 }|] } @> |> test.ExecuteExpression
            result.Length |> should equal 2

        [<Test>] 
        member test.``Parallel Combinator <||>`` () =
            let result = <@ cloud { return 1 } <||> cloud { return 2 } @> |> test.ExecuteExpression
            result |> should equal (1, 2)

        [<Test>] 
        member test.``Parallel Map`` () =
            <@ parallelIncrements () @> |> test.ExecuteExpression |> should equal 65;
            <@ cloud { let! r = [|1..10|] |> Array.map (fun x -> cloud { return x }) |> Cloud.Parallel in return r } @> |> test.ExecuteExpression |> (fun r -> r.Length |> should equal 10)

        [<Test>]
        member test.``Parallel Exception`` () =
            <@
                let worker i = cloud { if i = 5 then invalidOp "kaboom" }
                cloud {
                    try
                        let! results =
                            [1 .. 10] 
                            |> List.map worker
                            |> Cloud.Parallel

                        return false

                    with :? InvalidOperationException -> return true
                }
            @> |> test.ExecuteExpression |> should equal true

        [<Test>] 
        member test.``Try With Exception`` () =
            <@ testTryWithException 0 @> |> test.ExecuteExpression |> should equal -1

        [<Test>] 
        member test.``Try With in non-monadic expr`` () =
            <@ cloud { return try let l = 42 in l with _ -> 0 } @> |> test.ExecuteExpression |> should equal 42

        [<Test>] 
        member test.``Try With Unhandled Exception`` () =
            fun () ->
                <@ testTryWithUnhandledException 0 @> |> test.ExecuteExpression |> ignore

            |> shouldFailwith<CloudException>

        [<Test>] 
        member test.``Try without Exception`` () =
            <@ testTryWithOutException 0 @> |> test.ExecuteExpression |> should equal 1

        [<Test>] 
        member test.``Try Finally With Exception`` () =
            <@ testTryFinallyWithException 0 @> |> test.ExecuteExpression |> should equal (ref 1)

        [<Test>] 
        member test.``Try Finally WithOut Exception`` () =
            <@ testTryFinallyWithOutException 0 @> |> test.ExecuteExpression |> should equal (ref 2)

        [<Test>] 
        member test.``Parallel computations with exceptions`` () =
            fun () -> 
                <@ testParallelWithExceptions() @> |> test.ExecuteExpression |> ignore //|> should equal -1
            |> shouldFailwith<CloudException>

        [<Test; Repeat 5>]
        member test.``Parallel Recursive calls (Parallel Fib)`` () =
            <@ testParallelFib 10 @> |> test.ExecuteExpression |> should equal 55

        [<Test>] 
        member test.``Primes example `` () = 
            <@ PrimesTest.parallelPrimes 20 2 @> |> test.ExecuteExpression |> should equal (Seq.ofArray [| 2; 3; 5; 7; 11; 13; 17; 19 |])

        [<Test>]
        member test.``valid return type Cloud<_> from quoted function call (general example '<|') `` () = 
            <@ cloud { let! x = (fun x -> cloud { return x }) <| 1 in return x } @> |> test.ExecuteExpression |> should equal 1

        [<Test>]
        member test.``valid return type Cloud<_> from Cloud<_> `` () = 
            let expectedValue : Cloud<int> = <@ cloud { return cloud { return 1 } } @> |> test.ExecuteExpression 
            let expectedValue : Cloud<int> = <@ cloud { let! x = cloud { return cloud { return 1 } } in return x } @> |> test.ExecuteExpression 
            ()

        [<Test>] 
        member test.``if then else `` () = 
            <@ testIfThenElse 42 @> |> test.ExecuteExpression |> should equal "Magic" |> ignore
            <@ testIfThenElse 41 @> |> test.ExecuteExpression |> should equal "Boring"

        [<Test>] 
        member test.``match with `` () = 
            <@ testMatchWith 42 @> |> test.ExecuteExpression |> should equal "Magic" |> ignore
            <@ testMatchWith 41 @> |> test.ExecuteExpression |> should equal "Boring"

        [<Test>] 
        member test.``Combine `` () = 
            <@ cloud { return (); return 42 } @> |> test.ExecuteExpression |> should equal 42

        [<Test>]
        member test.``Sequential `` () = 
            <@ testSequential @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``For Loop `` () = 
            <@ testForLoop @> |> test.ExecuteExpression |> should equal 1000

        [<Test>]
        member test.``For Loop as last expr `` () = 
            <@ cloud { for _ in [|1..10|] do () } @> 
            |> test.ExecuteExpression |> should equal ()

        [<Test>]
        member test.``While Loop `` () = 
            <@ testWhileLoop @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``While Loop as last expr `` () = 
            <@ cloud { while false do () } @> 
            |> test.ExecuteExpression |> should equal ()


        [<Test>]
        member test.``Cloud Using`` () =
            <@ testUseWithCloudDisposable () @> |> test.ExecuteExpression |> should equal true

        [<Test>]
        member test.``Cloud Using With Exception`` () =
            <@ testUseWithException () @> |> test.ExecuteExpression |> should equal true

        [<Test>]
        member test.``Recursion `` () = 
            <@ testRecursion 2 @> |> test.ExecuteExpression |> should equal 3

        [<Test>]
        member test.``Tail Recursion (return!)`` () =
            let delta = 
                <@ cloud {
                        let start = GC.GetTotalMemory(true) 
                        let! end' = testTailRecursion 5000000 
                        return (end' - start)  
                    }
                @> |> test.ExecuteExpression 
            if delta > 100000000L then
                failwith "Suspiciously large allocation rate"

        [<Test>]
        member test.``Point-free Lambda Generation`` () =
            <@ pow 10 (fun x -> x + 1) @> |> test.ExecuteExpression |> fun f -> f 0 |> should equal 10

        [<Test>]
        member test.``Mutual Recursion `` () = 
            <@ even 2 @> |> test.ExecuteExpression |> should equal true;
            <@ even 3 @> |> test.ExecuteExpression |> should equal false

        [<Test>]
        member test.``sequence application `` () = 
            <@ (cloud { return 2 }, cloud { return 1 }) ||> (fun first second -> cloud { let! _ = first in return! second }) @> |> test.ExecuteExpression |> should equal 1;
            

        [<Test>]
        member test.``sequential combine (<.>) `` () = 
            <@ cloud { return 1 } <.> cloud { return "2" } @> |> test.ExecuteExpression |> should equal (1, "2")

        [<Test>]
        member test.``Cloud OfAsync`` () =
            <@ cloud { 
                let! value = Cloud.OfAsync(async { return 1 })
                return value + 1
               } @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``Cloud OfAsync Exception handling`` () =
            <@ cloud { 
                try
                    let! value = Cloud.OfAsync(async { return raise <| new InvalidOperationException() })
                    return 1
                with _ -> return -1
               } @> |> test.ExecuteExpression |> should equal -1

        [<Test>]
        member test.``Cloud OfAsync Looping `` () =
            <@ cloud {
                for i in [|1..100000|] do
                    let! value = Cloud.OfAsync(async { return 1 })
                    ()
                return 42
               } @> |> test.ExecuteExpression |> should equal 42

        [<Test>]
        member test.``mapReduce Lib`` () =
            <@ mapReduce mapF reduceF 0 [1..2] @>
            |> test.ExecuteExpression |> should equal 3

        [<Test>]
        member test.``Cloud returned closure serialization`` () =
            let f : int -> int =
                <@ cloud { 
                        let y = 1
                        return (fun x -> x + y)
                   } @> |> test.ExecuteExpression 
            f 2 |> should equal 3

        [<Test>]
        member test.``parallel random sum for GZipStream behavior`` () =
            <@ randomSumParallel() @> |> test.ExecuteExpression |> ignore

        [<Test>]
        member test.``ambiguous match exception in parallel cloud exception construction`` () =
            (fun () -> test.ExecuteExpression <@ testAmbiguousParallelException () @> |> ignore)
            |> shouldFailwith<MBraceException>

        [<Test>]
        member test.``simple CloudRef`` () = 
            <@ testSimpleCloudRef 42 @> |> test.ExecuteExpression |> should equal 42

        [<Test>]
        member test.``CloudRef (New-Get) by name`` () = 
            <@ cloud { 
                let container = Guid.NewGuid().ToString()
                let! r = CloudRef.New(container, 42)
                let! cloudRef = CloudRef.Get<int>(container, r.Name) 
                return cloudRef.Value } @> |> test.ExecuteExpression |> should equal 42

        [<Test>]
        member test.``CloudRef Get by name - type mismatch`` () = 
            fun () -> 
                 test.ExecuteExpression
                    <@ cloud { 
                        let container = Guid.NewGuid().ToString()
                        let! r = CloudRef.New(container, 42)
                        let! cloudRef = CloudRef.Get<obj>(container, r.Name) 
                        () } 
                    @> |> ignore

            |> shouldFailwith<MBraceException>

        [<Test>]
        member test.``CloudRef Get all in container`` () = 
            <@ cloud { 
                let container = Guid.NewGuid().ToString()
                let! x = CloudRef.New(container, 40)
                let! y = CloudRef.New(container, 1)
                let! z = CloudRef.New(container, 1)
                let! refs = CloudRef.Get(container)
                let refs = refs |> Array.map unbox<ICloudRef<int>>
                return refs 
                       |> Seq.map (fun r -> r.Value)
                       |> Seq.sum
               } @> |> test.ExecuteExpression |> should equal 42

        [<Test>]
        member test.``CloudRef Get by random name - failure`` () = 
            <@  cloud {
                    let container, id = Guid.NewGuid().ToString(), Guid.NewGuid().ToString()
                    return! CloudRef.TryGet<int>(container, id)
                } @> |> test.ExecuteExpression |> should equal None

        [<Test>]
        member test.``CloudRef - inside cloud non-monadic deref`` () = 
            <@ cloud { let! x = newRef 42 in return x.Value } @> |> test.ExecuteExpression |> should equal 42

        [<Test>]
        member test.``CloudRef - outside cloud non-monadic deref`` () = 
            <@ cloud { let! x = newRef 42 in return x } @> |> test.ExecuteExpression |> (fun x -> x.Value) |> should equal 42

        [<Test>]
        member test.``Parallel CloudRef dereference`` () = 
            <@ testParallelCloudRefDeref 1 @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``Cloud List`` () = 
            let cloudList = <@ testBuildCloudList 2 @> |> test.ExecuteExpression 
            <@ testReduceCloudList cloudList @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``Cloud Tree`` () = 
            let cloudTree = <@ testBuildCloudTree 2 @> |> test.ExecuteExpression 
            <@ testReduceCloudTree cloudTree @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``CloudRef Tree Node Composition`` () = 
            let firstRef = <@ cloud { return! newRef <| Leaf 1 } @> |> test.ExecuteExpression 
            let secondRef = <@ cloud { return! newRef <| Leaf 2 } @> |> test.ExecuteExpression 
            <@ cloud { return! newRef <| TestFunctions.Node (firstRef, secondRef) } @> |> test.ExecuteExpression |> ignore


        [<Test>]
        member test.``RunLocal Cloud`` () = 
            cloud { 
                let x = ref 1
                let! _ = cloud { 
                            do x := !x + 1
                            return ()
                            }
                return !x 
            } |> MBrace.RunLocal |> should equal 2

        [<Test; Repeat 10>]
        member test.``Cloud Side effects`` () =
            <@ cloud {
                let  x = ref 0
                let! n = Cloud.GetWorkerCount()
                let  n = n * 50
                do! [|1..n|] 
                    |> Array.map (fun i -> cloud { do x := i })
                    |> Cloud.Parallel
                    |> Cloud.Ignore

                return x.Value
            } @> |> test.ExecuteExpression |> should equal 0

        [<Test>]
        member test.``Cloud ToLocal``() = 
            <@ cloud { 
                let! (values : int []) = local <| Cloud.Parallel [| for i in [1..1000] -> cloud { return i } |]
                return values |> Array.sum
               } @> |> test.ExecuteExpression |> should equal 500500

        [<Test>]
        member test.``Cloud ToLocal (Side-Effects)`` () =
            <@ cloud { 
                let testRef = ref 1
                let! value = local <| Cloud.Parallel [| cloud { return testRef := !testRef + 1 } |]
                return !testRef
               } @> |> test.ExecuteExpression |> should equal 1

        [<Test>]
        member test.``Local Cloud GetWorkerCount``() = 
            <@ cloud { 
                let! c' = local <| cloud { let! c = Cloud.GetWorkerCount() in return c }
                return c'
               } @> |> test.ExecuteExpression |> should equal Environment.ProcessorCount

        [<Test>]
        member test.``Distrib Cloud GetWorkerCount``() = 
            <@ cloud { 
                let! c' = cloud { let! c = Cloud.GetWorkerCount() in return c }
                return c'
               } @> |> test.ExecuteExpression |> shouldMatch (fun c -> c <> 0)

        [<Test>]
        member test.``Cloud GetProcessId``() = 
            <@ cloud { 
                return! Cloud.GetProcessId() 
               } @> |> test.ExecuteExpression |> shouldMatch (fun pid -> if test.IsLocalTesting then pid = 0 else pid > 0)

        [<Test>]
        member test.``Cloud GetTaskId``() = 
            <@ cloud { return! Cloud.GetTaskId() } @> 
            |> test.ExecuteExpression 
            |> shouldMatch (not << String.IsNullOrEmpty)

        [<Test>] 
        member test.``Choice `` () =
            let result : int option = <@ cloud { let! r = Cloud.Choice [|cloud { return None }; cloud { return Some 1 }|] in return r } @> |> test.ExecuteExpression
            result |> should equal (Some 1)

        [<Test>] 
        member test.``Local Choice `` () =
            let result : int option = <@ cloud { let! r = local <| Cloud.Choice [|cloud { return None }; cloud { return Some 1 }|] in return r } @> |> test.ExecuteExpression
            result |> should equal (Some 1)

        [<Test>] 
        member test.``Choice Exception`` () =
            fun () ->
                test.ExecuteExpression 
                    <@ 
                        cloud { 
                            let! r = Cloud.Choice [|cloud { return None }; cloud { return raise <| new InvalidOperationException() }|] 
                            return r } 
                    @>     
                |> ignore
            |> shouldFailwith<CloudException>

        [<Test>]
        member test.``Choice Exception handling`` () =
            <@
                cloud { 
                    try
                        let! r = Cloud.Choice [|cloud { return None }; cloud { return raise <| new InvalidOperationException() }|] 
                        return false
                    with :? InvalidOperationException -> return true
                } 
            @> |> test.ExecuteExpression |> should equal true
        
        [<Test; Repeat 5>]
        member test.``Choice Recursive`` () =
            <@  let rec test depth id = cloud {
                    if depth = 0 then
                        if id = 4 then return Some 4 else return None
                    else
                        return! Cloud.Choice [| test (depth-1) (2 * id)
                                                test (depth-1) (2 * id + 1) |]
                }

                test 5 0
            @>
            |> test.ExecuteExpression |> should equal (Some 4)

        [<Test>] 
        member test.``Cloud Parallel Log`` () = 
            <@  cloud {
                    do! [| for i in 1..10 -> cloud { do! Cloud.Log "____________________________________________"  } |]
                        |> Cloud.Parallel
                        |> Cloud.Ignore
                } @> |> test.ExecuteExpression |> ignore

        [<Test>] 
        member test.``CloudSeq`` () = 
            <@  cloud {
                    let! cloudSeq = CloudSeq.New [1..100]
                    return cloudSeq |> Seq.length
                } @> |> test.ExecuteExpression |> should equal 100
        
        [<Test>] 
        member test.``CloudSeq exta info - Count`` () = 
            <@  cloud {
                    let! cloudSeq =  CloudSeq.New [1..100]
                    return cloudSeq.Count = (cloudSeq |> Seq.length)
                } @> |> test.ExecuteExpression |> should equal true

        [<Test>] 
        member test.``CloudSeq exta info - Size`` () = 
            <@  cloud {
                    let! cloudSeq =  CloudSeq.New [1..100]
                    return cloudSeq.Size > 0L
                } @> |> test.ExecuteExpression |> should equal true

        [<Test>] 
        member test.``CloudSeq (New-Get) by name`` () = 
            <@  cloud {
                    let container = Guid.NewGuid().ToString()
                    let! s = CloudSeq.New(container, [1..100])
                    let! cloudSeq = CloudSeq.Get<int>(container, s.Name)
                    return cloudSeq |> Seq.length
                } @> |> test.ExecuteExpression |> should equal 100

        [<Test>]
        member test.``CloudSeq Get by name - type mismatch`` () = 
            fun () ->
                test.ExecuteExpression
                    <@ 
                        cloud { 
                            let container = Guid.NewGuid().ToString()
                            let! r = CloudSeq.New(container, [42])
                            let! cloudRef = CloudSeq.Get<obj>(container, r.Name) 
                            () 
                        } 
                    @> 
                |> ignore

            |> shouldFailwith<CloudException>

        [<Test>]
        member test.``CloudSeq TryGet by name - failure`` () = 
            <@  cloud {
                    let container, id = Guid.NewGuid().ToString(), Guid.NewGuid().ToString()
                    return! CloudSeq.TryGet<int>(container, id)
                } @> |> test.ExecuteExpression |> should equal None
         
         
        [<Test>]
        member test.``CloudSeq Get all in container`` () = 
            <@ cloud { 
                let container = Guid.NewGuid().ToString()
                let! x = CloudSeq.New(container, [40])
                let! y = CloudSeq.New(container, [1])
                let! z = CloudSeq.New(container, [1])
                let! s = CloudSeq.Get(container)
                let seqs = s |> Seq.cast<ICloudSeq<int>>
                return seqs 
                       |> Seq.concat
                       |> Seq.sum
               } @> |> test.ExecuteExpression |> should equal 42       

        [<Test>]
        member test.``CloudFile Create/Read - Lines`` () =
            let words () = Array.init (1024 * 512) (fun i -> "test" + (string i))
            let (lines, text) = 
                <@ cloud {
                    let container = Guid.NewGuid().ToString()
                    let words = words ()
                    let! f = CloudFile.Create(container, "cloudfile.txt",
                                (fun (stream : Stream) -> async {
                                    use sw = new StreamWriter(stream)
                                    words |> Array.iter (sw.WriteLine) }))
                    let! lines = CloudFile.ReadLines(f)
                    let! text  = CloudFile.ReadAllText(f)
                    return (lines, text)
                } @>
                |> test.ExecuteExpression
            should equal (words ()) (lines |> Seq.toArray)
            should equal ((String.concat System.Environment.NewLine (words ())) + System.Environment.NewLine) text

        [<Test>]
        member test.``CloudFile Create/Read - Stream #1`` () =
            let mk a = Array.init (a * 1024) byte
            let n = 512
            let bytes = 
                <@ cloud {
                    let container = Guid.NewGuid().ToString()
                    let! f = CloudFile.Create(container, "cloudfile.txt",
                                    fun (stream : Stream) -> async {
                                            let b = mk n
                                            stream.Write(b, 0, b.Length)
                                            stream.Flush()
                                            stream.Dispose() })
                    let! bytes = CloudFile.ReadAllBytes(f)
                    return bytes
                } @>
                |> test.ExecuteExpression 
            should equal (mk n) bytes

        [<Test>]
        member test.``CloudFile Get`` () =
            let folder = Guid.NewGuid().ToString()
            let f = 
                <@ cloud {
                    let! s = CloudSeq.New(folder, [1..10])
                    let! fs = CloudFile.Get(s.Container)
                    let  s = Seq.head fs
                    return s
                } @>
                |> test.ExecuteExpression
            should equal folder f.Container

        [<Test>]
        member test.``CloudFile TryGet by name - failure`` () =
            let folder = Guid.NewGuid().ToString()
            let f = 
                <@ cloud {
                    return! CloudFile.TryGet(Guid.NewGuid().ToString("N"),Guid.NewGuid().ToString("N"))
                } @>
                |> test.ExecuteExpression
            should equal f None

        [<Test>]
        member test.``MutableCloudRef - Simple For Loop`` () =
            <@ cloud {
                let! x = MutableCloudRef.New(-1)
                for i in [|0..10|] do
                    do! MutableCloudRef.SpinSet(x, fun _ -> i)
                return! MutableCloudRef.Read(x)
            } @> |> test.ExecuteExpression |> should equal 10
          
        [<Test; Repeat 10>]
        member test.``MutableCloudRef - Set`` () = 
            <@
                cloud {
                    let! x = MutableCloudRef.New(-1)
                    let! (x,y) = 
                        cloud { return! MutableCloudRef.Set(x, 1) } <||>
                        cloud { return! MutableCloudRef.Set(x, 2) }
                    return x <> y
                } 
            @> 
            |> test.ExecuteExpression |> should equal true

        [<Test; Repeat 10>]
        member test.``MutableCloudRef - Set multiple`` () = 
            <@
                cloud {
                    let! x = MutableCloudRef.New(-1)
                    let! n = Cloud.GetWorkerCount()
                    let  n = 50 * n
                    let! v = 
                        [|1..n|] |> Array.map (fun i -> cloud { return! MutableCloudRef.Set(x, 1) })
                                 |> Cloud.Parallel
                    let f = Seq.filter ((=) false) v
                    let t = Seq.findIndex ((=) true) v
                    return (Seq.length f = n-1), (Seq.nth t v = true)
                } 
            @>
            |> test.ExecuteExpression |> should equal (true,true)

        [<Test>]
        member test.``MutableCloudRef - Force`` () = 
            <@
                cloud {
                 let! x = MutableCloudRef.New(-1)
                 let! _ = cloud { return! MutableCloudRef.Force(x, 1) } <||>
                          cloud { do! Cloud.OfAsync(Async.Sleep 3000)
                                  return! MutableCloudRef.Force(x, 2) }
                 return! MutableCloudRef.Read(x)
                }
            @>
            |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``MutableCloudRef - Free`` () =
            <@ cloud {
                let! x = MutableCloudRef.New(0)
                do! MutableCloudRef.Free(x)
                return! MutableCloudRef.TryRead(x)
            } @> |> test.ExecuteExpression |> should equal None

        [<Test; Repeat 10>]
        member test.``MutableCloudRef - High contention`` () = 
            <@
                cloud {
                    let! n = Cloud.GetWorkerCount()
                    let  m = (System.Random()).Next(2, 43)
                    let! x = MutableCloudRef.New(0)
                    do! [|1..n|] 
                        |> Array.map (fun _ -> cloud {
                            for _ in [|1..m|] do
                                do! MutableCloudRef.SpinSet(x, (+) 1)
                            })
                        |> Cloud.Parallel
                    
                        |> Cloud.Ignore
                    let! result = MutableCloudRef.Read(x)
                    return result = n * m
                }
            @>
            |> test.ExecuteExpression |> should equal true

        [<Test; Repeat 10>]
        member test.``MutableCloudRef - High contention - Large obj`` () = 
            <@ 
                cloud {
                    let! n = Cloud.GetWorkerCount()
                    let  m = (System.Random()).Next(2, 43)
                    let make i = Array.create i (byte i)
                    let len = 1024 * 1024
                    let! x = MutableCloudRef.New(make len)
                    do! [|1..n|] 
                        |> Array.map (fun _ -> cloud {
                            for i in [|1..m|] do
                                do! MutableCloudRef.SpinSet(x, fun j -> make (j.Length+1))
                            })
                        |> Cloud.Parallel
                        |> Cloud.Ignore
                    let! result = MutableCloudRef.Read(x)
                    return result = make (len + n * m)
                }
            @>
            |> test.ExecuteExpression |> should equal true

        [<Test>]
        member test.``MutableCloudRef - Token passing`` () = 
            <@
                cloud {
                    let rec run (id : int) (locks : MVar<unit> []) (token : IMutableCloudRef<int>) : Cloud<int option> = 
                      cloud {
                        do! MVar.take(locks.[id])
                        let! tok = MutableCloudRef.Read(token)
                        match tok with
                        | 0 -> return Some id
                        | t -> let! ok = MutableCloudRef.Set(token, t - 1)
                               if ok then 
                                    do! MVar.put locks.[(id+1) % locks.Length] ()
                                    return! run id locks token
                               else 
                                    return None
                      }

                    let! nodes = Cloud.GetWorkerCount()
                    let value = 1000
                    let! locks = [|1..nodes|] |> Array.map (fun _ -> cloud { return! MVar.newEmpty })
                                              |> Cloud.Parallel
                    let! token = MutableCloudRef.New(value)
                    do! MVar.put locks.[0] ()
                    let! r = [|1..nodes|] 
                             |> Array.map (fun i -> run (i-1) locks token)
                             |> Cloud.Choice
                    return r.Value = value % nodes
                } 
            @>
            |> test.ExecuteExpression |> should equal true

        [<Test>]
        member test.``MutableCloudRef - Get all in container`` () = 
            <@ cloud { 
                let container = Guid.NewGuid().ToString()
                let! x = MutableCloudRef.New(container, 40)
                let! y = MutableCloudRef.New(container, 1)
                let! z = MutableCloudRef.New(container, 1)
                let! refs = MutableCloudRef.Get(container)
                let refs = refs |> Array.map unbox<IMutableCloudRef<int>>
                let! r = refs
                         |> Array.map MutableCloudRef.Read
                         |> Cloud.Parallel
                return Seq.sum r
               } @> |> test.ExecuteExpression |> should equal 42

        [<Test>]
        member test.``MutableCloudRef - TryGet by name - failure`` () = 
            <@ cloud { 
                return! MutableCloudRef.TryGet(Guid.NewGuid().ToString("N"),Guid.NewGuid().ToString("N"))
               } @> |> test.ExecuteExpression |> should equal None

        [<Test>]
        member test.``UnQuote Exception`` () =
            <@ cloud { 
                    try
                        return raise <| System.InvalidOperationException() 
                    with :? System.InvalidOperationException -> return -1
                         | _ -> return -2
               } 
            @> |> test.ExecuteExpression |> should equal -1

        [<Test>]
        member test.``Cloud.Catch`` () =
            let result : Choice<unit,exn> = 
                <@ cloud { 
                        return! Cloud.Catch <| cloud { return raise <| exn() }
                   } 
                @> |> test.ExecuteExpression
            match result with
            | Choice1Of2 _ -> Assert.Fail("Expected exception but got result.")
            | Choice2Of2 e -> ()

            let result : Choice<unit,exn> = 
                <@ cloud { 
                        return! Cloud.Catch <| cloud { return () }
                   } 
                @> |> test.ExecuteExpression
            match result with
            | Choice1Of2 _ -> ()
            | Choice2Of2 e -> Assert.Fail("Expected result but got exception.")

        [<Test>]
        member test.``Cloud.Sleep`` () =
            <@ cloud { do! Cloud.Sleep 1000 } @>
            |> test.ExecuteExpression
            

        [<Test>]
        member test.``Concurrent cache writes (Parallel CloudSeq read after cleaning cache)`` () =

            let cs = <@ cloud { return! CloudSeq.New(Array.init (10 * 1024 * 1024) id) } @> |> test.ExecuteExpression
            let storeId = Runtime.Store.StoreRegistry.DefaultStoreInfo.Id
            // Clear client cache
            let cacheDir = Path.Combine(MBraceSettings.WorkingDirectory, "LocalCache", sprintf "fscache-%d" <| hash storeId)
            Directory.Delete(cacheDir, true)

            let read () = async { return cs |> Seq.toArray }

            let r = Async.Parallel [read (); read () ]
                    |> Async.RunSynchronously

            should equal r.[0] r.[1]


        [<Test>]
        member test.``StoreClient CloudRef`` () =
            let sc = StoreClient.Default
            let c = Guid.NewGuid().ToString("N")
            let cr = sc.CreateCloudRef(c, 42)
            cr.Value |> should equal 42
            let cr' = sc.GetCloudRef(c, cr.Name) :?> ICloudRef<int>
            cr'.Value |> should equal 42
            sc.DeleteContainer(c)

        [<Test>]
        member test.``StoreClient CloudSeq`` () =
            let sc = StoreClient.Default
            let c = Guid.NewGuid().ToString("N")
            let cr = sc.CreateCloudSeq(c, [42])
            cr |> Seq.toList |> should equal [42]
            let cr' = sc.GetCloudSeq(c, cr.Name) :?> ICloudSeq<int>
            cr' |> Seq.toList |> should equal [42]
            sc.DeleteContainer(c)

        [<Test>]
        member test.``StoreClient CloudFile`` () =
            let sc = StoreClient.Default
            let c = Guid.NewGuid().ToString("N")
            let cr = sc.CreateCloudFile(c, fun _ -> async.Return () )
            use s = cr.Read() |> Async.RunSynchronously
            s.Length |> should equal 0
            let cr' = sc.GetCloudFile(c, cr.Name) 
            use s' = cr.Read() |> Async.RunSynchronously
            s'.Length |> should equal 0
            sc.DeleteContainer(c)

        [<Test>]
        member test.``StoreClient MutableCloudRef`` () =
            let sc = StoreClient.Default
            let c = Guid.NewGuid().ToString("N")
            let cr = sc.CreateMutableCloudRef(c, -1)
            cr.Value |> should equal -1
            let cr' = sc.GetMutableCloudRef(c, cr.Name) :?> IMutableCloudRef<int>
            cr'.Value |> should equal -1
            cr.ForceUpdate(42) |> Async.RunSynchronously
            cr'.Value |> should equal 42
            sc.DeleteContainer(c)