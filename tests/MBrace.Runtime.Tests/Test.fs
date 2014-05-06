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
    open FsUnit
    
    [<AutoOpen>]
    module TestFunctions = 

        let shouldMatch (f : 'a -> bool) (x : 'a) = //Assert.That(f x)
            if f x then () else raise <| new AssertionException(sprintf "Got Result: %A." x)

        let shouldFailwith<'Exn when 'Exn :> exn> (f : unit -> unit) =
            try
                f() ; raise <| new AssertionException(sprintf "Expected exception of type %s" (typeof<'Exn>.ToString()))
            with :? 'Exn as ex -> ()

        // custom type for test scenarios
        type Foo(x:int) = 
            member self.Get() = x

        // recursive type test
        type Peano = Zero | Succ of Peano

        let rec constructPeano n =
            match n with
            | 0 -> Zero
            | n -> Succ <| constructPeano (n-1)

        let largePeano = constructPeano 42

        let delay = 5*1024

        let traceHasValue dumps name value =
            dumps 
            |> Seq.exists(
                function 
                | Trace info -> 
                    info.Environment.ContainsKey(name) && info.Environment.[name].Contains value
                | _ -> false)
            

        [<Cloud>]
        let testPropGet = cloud { return 42 }

        [<Cloud>]
        let rec int2Peano n =
            cloud {
                match n with
                | 0 -> return Zero
                | n ->
                    let! pred = int2Peano (n-1)
                    return Succ pred
            }

        [<Cloud>]
        let rec peano2Int p =
            cloud {
                match p with
                | Zero -> return 0
                | Succ p' ->
                    let! n = peano2Int p'
                    return n+1
            }

        [<Cloud>]
        let rec peanoAddition p q =
            cloud {
                match p with
                | Zero -> return q
                | Succ p' ->
                    let! pred = peanoAddition p' q
                    return Succ pred
            }

        [<Cloud>]
        let peanoTest m n =
            cloud {
                let! p,q = int2Peano m <||> int2Peano n
                let! sum = peanoAddition p q
                return! peano2Int sum
            }

        [<Cloud>]
        let rec ackermann m n =
            cloud {
                match m, n with
                | 0, n -> return n + 1
                | m, 0 -> return! ackermann (m-1) 1
                | m, n ->
                    let! right = ackermann m (n-1)
                    return! ackermann (m-1) right
            }

        [<Cloud>]
        let rec mcCarthy91 n = 
            cloud {
                if n > 100 then
                    return n - 10
                else // n <= 100
                    let! r = mcCarthy91 (n + 11)
                    return! mcCarthy91 r
            }

        [<Cloud>] 
        let mapF (v : int) = cloud { return v }
        [<Cloud>] 
        let reduceF left right = cloud { return left + right }

        [<Cloud>] 
        let add a b = cloud { return a + b }
        [<Cloud>] 
        let addUnCurry (a, b) = cloud { return a + b }

        [<Cloud>] 
        let parallelIncrements () = 
            cloud {
                let! numbers = [| for i = 1 to 10 do yield cloud { return i + 1 } |] |> Cloud.Parallel
                return numbers |> Array.sum
            }


        [<Cloud>] 
        let rec testParallelFib n = 
            cloud {
                if n = 1 || n = 0 then 
                    return n
                else 
                    let! (nMinusOne, nMinusTwo) = testParallelFib (n - 1) <||> testParallelFib (n - 2)
                    return nMinusOne + nMinusTwo
            }

        [<Cloud>] 
        let testTryWithException a =
            cloud {
                try
                    let x = a / 0 
                    return x + 1 // unreachable  code
                with 
                    | :? DivideByZeroException as exn -> return -1
            }

        [<Cloud>] 
        let testTryWithUnhandledException a =
            cloud {
                let x = a / 0 
                return x + 1 // unreachable  code
            }

        [<Cloud>] 
        let testTryWithOutException a =
            cloud {
                try
                    let x = a + 1
                    return x 
                with 
                    | :? DivideByZeroException as exn -> return -1
            }

        [<Cloud>] 
        let testTryFinallyWithException a =
            cloud {
                let flag = ref 0
                try
                    try
                        let x = a / 0 
                        flag := !flag + 1
                        return flag // unreachable  code
                    finally
                        flag := !flag + 1
                with 
                    | :? DivideByZeroException as exn -> return flag
            }

        [<Cloud>] 
        let testTryFinallyWithOutException a =
            cloud {
                let flag = ref 0
                try
                    try
                        let x = a / 1 
                        flag := !flag + 1
                        return flag 
                    finally
                        flag := !flag + 1
                with 
                    | :? DivideByZeroException as exn -> return flag
            }

        [<Cloud>] 
        let testParallelWithExceptions () = 
            cloud {
                let! numbers = Cloud.Parallel [| for i = 1 to 10 do yield cloud { return i / 0 } |] 
                return numbers
            }
        
        let nonQuotedCloud() = cloud { return 1 }
        let nonQuotedArrayCloud() = [| cloud { return 1 } |]
        let nonQuotedOptionCloud() = Some <| cloud { return 1 }

        [<Cloud>] 
        let testIfThenElse a = 
            cloud {
                if a = 42 then
                    return "Magic"
                else
                    return "Boring"
            }

        [<Cloud>]
        let testMatchWith a = 
            cloud {
                match a with
                | 42 -> return "Magic"
                | n -> return "Boring"
            }

        [<Cloud>]
        let testSequential = 
            cloud {
                let list = new List<int>()
                list.Add 1
                list.Add 2
                return list.Count
            }

        [<Cloud>]
        let testForLoop = 
            cloud {
                let list = new List<int>()
                for i in [|1..1000|] do
                    list.Add(i)
                return list.Count
            }

        [<Cloud>]
        let testWhileLoop = 
            cloud {
                let list = new List<int>()
                while list.Count <> 2 do
                    list.Add 1
                return list.Count
            }


        [<Cloud>]
        let rec testRecursion a = 
            cloud {
                if a = 0 then return 0
                else
                    let! result = testRecursion (a - 1)
                    return a + result
            }

        [<Cloud>]
        let rec testTailRecursion a = 
            cloud {
                if a = 0 then return GC.GetTotalMemory(true)
                else
                    return! testTailRecursion (a - 1)
            }

        [<Cloud>]
        let rec even number = 
            cloud {
                if number = 0 then
                    return true
                else
                    return! odd (number - 1)
            }
        and [<Cloud>] odd number = 
            cloud {
                if number = 0 then
                    return false
                else
                    return! even (number - 1)
            }

        [<Cloud>]
        let getRandomNumber () = cloud { return (new Random()).Next() }

        [<Cloud>]
        let randomSumParallel () =
            cloud {
                let! (first, second) = getRandomNumber() <||> getRandomNumber()

                return first + second
            }

        [<Cloud>]
        let testAmbiguousParallelException () =
            cloud {
                let! (first, second) = (cloud { failwith "Wrong"; return 1 :> obj } : ICloud<obj>) <||> (cloud { failwith "Wrong"; return 2 :> obj } : ICloud<obj>)

                return 0
            }

        [<Cloud>]
        let testSimpleCloudRef a = 
            cloud {
                let! ref = newRef a
                return ref.Value
            }

        [<Cloud>]
        let testParallelCloudRefDeref a = 
            cloud {
                let! ref = newRef a
                let! (x, y) = cloud { return ref.Value } <||> cloud { return ref.Value }
                return x + y
            }

        type CloudList<'T> = Nil | Cons of 'T * ICloudRef<CloudList<'T>>
        [<Cloud>]
        let rec testBuildCloudList a = 
            cloud {
                if a = 0 then 
                    return! newRef Nil
                else
                    let! tail = testBuildCloudList (a - 1)
                    return! newRef <| Cons (1, tail)
            }
        [<Cloud>]
        let rec testReduceCloudList (cloudRefList : ICloudRef<CloudList<_>>) = 
            cloud {
                let cloudList = cloudRefList.Value
                match cloudList with
                | Cons (v, cloudRefList') ->
                    let! result = testReduceCloudList cloudRefList'
                    return 1 + result
                | Nil -> return 0
            }
        type CloudTree<'T> = Leaf of 'T | Node of ICloudRef<CloudTree<'T>> * ICloudRef<CloudTree<'T>>
        [<Cloud>]
        let rec testBuildCloudTree a = 
            cloud {
                if a = 1 then 
                    return! newRef <| Leaf 1
                else
                    let! left, right = testBuildCloudTree (a - 1) <.> testBuildCloudTree (a - 1)
                    return! newRef <| Node (left, right)
            }
        [<Cloud>]
        let rec testReduceCloudTree (cloudRefTree : ICloudRef<CloudTree<_>>) = 
            cloud {
                let cloudTree = cloudRefTree.Value
                match cloudTree with
                | Node (leftRef, rightRef) ->
                    let! left, right = testReduceCloudTree leftRef <.> testReduceCloudTree rightRef
                    return left + right
                | Leaf value -> return value
            }

        [<Cloud>]
        let rec mapReduce   (mapF : 'T -> ICloud<'R>) 
                            (reduceF : 'R -> 'R -> ICloud<'R>) (identity : 'R) 
                            (values : 'T list) : ICloud<'R> =
            cloud {
                match values with
                | [] -> return identity
                | [value] -> return! mapF value
                | _ -> 
                    let (leftList, rightList) = List.split values
                    let! (left, right) = 
                        (mapReduce mapF reduceF identity leftList) <||> 
                                    (mapReduce mapF reduceF identity rightList)
                    return! reduceF left right
            }

        [<Cloud>]
        let testTrace a =
            cloud { 
                let! x = Cloud.Trace <| cloud { return a + 1 } 
                return x
            } |> Cloud.Trace

        [<Cloud>]
        let testTraceExc () =
            cloud { 
                try
                    return raise <| new Exception("error")
                with ex ->
                    return 42
            } |> Cloud.Trace

        [<Cloud>]
        let testTraceForLoop () =
            cloud {
                let r = ref 0
                for i in [|1..3|] do
                    r := !r + 1
                return !r
            } |> Cloud.Trace 

        [<Cloud>]
        let testParallelTrace () =
            cloud { 
                let a = 1
                let b = 2
                let! (x, y) = cloud { return a + 1 } <||> cloud { return b + 1 }
                let! r = cloud { return x + y }
                return r
            } |> Cloud.Trace

        [<Cloud>]
        let rec pow n (f : int -> int) =
            cloud {
                match n with
                | 0 -> return id
                | n ->
                    let! prev = pow (n-1) f
                    return f >> prev
            }

//        let disposable (flagRef : bool ref) = 
//            
//            { new ICloudDisposable with
//                member self.Dispose() =
//                    async { 
//                        if flagRef.Value then
//                            return raise <| new ObjectDisposedException("Object Disposed!")
//                        else
//                            flagRef := true
//                            return () 
//                    } }

        [<Cloud>]
        let testUseWithCloudDisposable () = 
            cloud {
                try
                    use! value = CloudRef.New(false)
                    do! Cloud.OfAsync <| value.Dispose()
                    return! CloudRef.Read(value)
                with :? NonExistentObjectStoreException -> return true
            }

        [<Cloud>] 
        let testUseWithException () =
            cloud {
                let! cr = CloudRef.New(42)
                try
                    use x = cr
                    return raise <| new DivideByZeroException()
                with 
                    | :? DivideByZeroException as exn -> return cr.TryValue.IsNone
            }

    [<TestFixture>]
    [<AbstractClass>]
    type TestBed =
        new () = { }

        abstract ExecuteExpression: Expr<ICloud<'T>> -> 'T
        abstract InitRuntime: unit -> unit
        abstract FiniRuntime: unit -> unit
        abstract Runtime: MBraceRuntime
        abstract Name : string

        [<TestFixtureSetUp>]
        member test.Init() =
            test.InitRuntime()

        [<TestFixtureTearDown>]
        member test.Fini() =
            test.FiniRuntime()

        [<Test>] member test.
         ``Test simple values`` () =
            <@ cloud { return 1 } @> |> test.ExecuteExpression |> should equal 1

        [<Test>] member test.
         ``Test arithmetic`` () =
            <@ cloud { return 1 + 1 } @> |> test.ExecuteExpression |> should equal 2

        [<Test>] member test.
         ``Test let binding `` () =
            <@ cloud { let x = 1 in return x + 1 } @> |> test.ExecuteExpression |> should equal 2

        
        [<Test>] 
        member test.``Test let private binding `` () =
            let testPrivate = 42 
            <@ cloud { return testPrivate } @> |> test.ExecuteExpression |> should equal 42

        [<Test>] 
        member test.``Test let rec binding `` () =
            <@ let rec f n = cloud { if n <= 0 then return 1 else let! p = f (n-1) in return n * p } in f 2 @>
            |> test.ExecuteExpression |> should equal 2

        [<Test>] member test.
         ``Test variable hiding`` () =
            <@ cloud { let x = 1 in return (fun x -> x + 1) (x + 1) } @> |> test.ExecuteExpression |> should equal 3

        [<Test>] member test.
         ``Test variable binding with null and ()`` () =
            <@ (fun (x : string) -> cloud { return x } ) null @> |> test.ExecuteExpression |> should equal null;
            <@ (fun x -> cloud { return x } ) () @> |> test.ExecuteExpression |> should equal ();

        [<Test>] member test.
         ``Test customer object`` () = 
            <@ cloud { return new Foo(42) } @> |> test.ExecuteExpression |> (fun x -> should equal 42 <| x.Get() )

        [<Test>] member test.
         ``Test curry uncurry calls`` () = 
            <@ add 1 2  @> |> test.ExecuteExpression |> should equal 3;
            <@ addUnCurry (1, 2) @> |> test.ExecuteExpression |> should equal 3;

        [<Test>] member test.
         ``Custom Recursive Type`` () =
            <@ peanoAddition largePeano Zero @> |> test.ExecuteExpression |> should equal largePeano |> ignore ;
            <@ peanoTest 17 25 @> |> test.ExecuteExpression |> should equal 42


        [<Test>] member test.
         ``Ackermann Function`` () =
            <@ ackermann 3 4 @> |> test.ExecuteExpression |> should equal 125
        
        [<Test>] member test.
         ``McCarthy 91 Function`` () =
            <@ mcCarthy91 50 @> |> test.ExecuteExpression |> should equal 91;
            <@ mcCarthy91 150 @> |> test.ExecuteExpression |> should equal 140;

        [<Test>] member test.
         ``Test let mutable binding `` () =
            <@ cloud { 
                    let mutable x = 1
                    x <- x + 1
                    return x 
               } @> |> test.ExecuteExpression |> should equal 2 
            
        [<Test>] member test.
         ``Test .Net calls `` () =
            <@ cloud { return Int32.Parse("1") } @> |> test.ExecuteExpression |> should equal 1;
            <@ cloud { return (1 + 1).ToString() } @> |> test.ExecuteExpression |> should equal "2"

        [<Test>] member test.
         ``Test Array comprehension `` () =
            <@ cloud { return [| for i = 1 to 3 do yield i * 2 |] } @>
            |> test.ExecuteExpression |> should equal [|2; 4; 6|]

        [<Test>] member test.
         ``Test CloudAttribute `` () =
            <@ add 2 3 @> |> test.ExecuteExpression |> should equal 5

        [<Test>] member test.
         ``Test CloudAttribute on PropertyGet`` () =
            <@ testPropGet @> |> test.ExecuteExpression |> should equal 42

        [<Test>] member test.
         ``Test Bind `` () = 
            <@ cloud { let! a = cloud { return 1 } in return a + 1 } @> |> test.ExecuteExpression |> should equal 2

        [<Test>] 
        member test.``Test Parallel `` () =
            let result : int[] = <@ cloud { return! Cloud.Parallel [|cloud { return 1 }; cloud { return 2 }|] } @> |> test.ExecuteExpression
            result.Length |> should equal 2

        [<Test>] 
        member test.``Test Parallel Combinator <||>`` () =
            let result = <@ cloud { return 1 } <||> cloud { return 2 } @> |> test.ExecuteExpression
            result |> should equal (1, 2)

        [<Test>] member test.
         ``Test Parallel Map`` () =
            <@ parallelIncrements () @> |> test.ExecuteExpression |> should equal 65;
            <@ cloud { let! r = [|1..10|] |> Array.map (fun x -> cloud { return x }) |> Cloud.Parallel in return r } @> |> test.ExecuteExpression |> (fun r -> r.Length |> should equal 10)

        [<Test>] member test.
         ``Test Try With Exception`` () =
            <@ testTryWithException 0 @> |> test.ExecuteExpression |> should equal -1

        [<Test>] member test.
         ``Test Try With in non-monadic expr`` () =
            <@ cloud { return try let l = 42 in l with _ -> 0 } @> |> test.ExecuteExpression |> should equal 42

        [<Test>] 
        [<ExpectedException(typeof<CloudException>)>]
        member test.
         ``Test Try With Unhandled Exception`` () =
            <@ testTryWithUnhandledException 0 @> |> test.ExecuteExpression |> ignore

        [<Test>] member test.
         ``Test Try WithOut Exception`` () =
            <@ testTryWithOutException 0 @> |> test.ExecuteExpression |> should equal 1

        [<Test>] member test.
         ``Test Try Finally With Exception`` () =
            <@ testTryFinallyWithException 0 @> |> test.ExecuteExpression |> should equal (ref 1)

        [<Test>] member test.
         ``Test Try Finally WithOut Exception`` () =
            <@ testTryFinallyWithOutException 0 @> |> test.ExecuteExpression |> should equal (ref 2)

        [<Test>] 
        [<ExpectedException(typeof<CloudException>)>]
        member test.
         ``Test Parallel computations with exceptions`` () =
            <@ testParallelWithExceptions() @> |> test.ExecuteExpression |> should equal -1

        [<Test; Repeat 5>]
        member test.
            ``Test Parallel Recursive calls (Parallel Fib)`` () =
            <@ testParallelFib 10 @> |> test.ExecuteExpression |> should equal 55

        [<Test>] member test.
         `` Test primes example `` () = 
            <@ PrimesTest.parallelPrimes 20 2 @> |> test.ExecuteExpression |> should equal (Seq.ofArray [| 2; 3; 5; 7; 11; 13; 17; 19 |])

//        [<Test>]
//        [<ExpectedException(typeof<MBrace.Exception>)>]
//        member test.
//         `` Test Compiler exception, invalid return type Cloud<_> from arbitary-non quoted function `` () = 
//            <@ cloud { let! x = nonQuotedCloud() in return x } @> |> test.ExecuteExpression |> ignore
//
//        [<Test>]
//        [<ExpectedException(typeof<MBrace.Exception>)>]
//        member test.
//         `` Test Compiler exception, invalid return type Array Cloud<_> from arbitary-non quoted function `` () = 
//            <@ cloud { let x = nonQuotedArrayCloud() in return x } @> |> test.ExecuteExpression |> ignore
//
//        [<Test>]
//        [<ExpectedException(typeof<MBrace.Exception>)>]
//        member test.
//         `` Test Compiler exception, invalid return type Option Cloud<_> from arbitary-non quoted function `` () = 
//            <@ cloud { let x = nonQuotedOptionCloud() in return x } @> |> test.ExecuteExpression |> ignore

        [<Test>]
        member test.
         `` Test valid return type Cloud<_> from quoted function call (general example '<|') `` () = 
            <@ cloud { let! x = (fun x -> cloud { return x }) <| 1 in return x } @> |> test.ExecuteExpression |> should equal 1

        [<Test>]
        member test.
         `` Test valid return type Cloud<_> from Cloud<_> `` () = 
            let expectedValue : ICloud<int> = <@ cloud { return cloud { return 1 } } @> |> test.ExecuteExpression 
            let expectedValue : ICloud<int> = <@ cloud { let! x = cloud { return cloud { return 1 } } in return x } @> |> test.ExecuteExpression 
            ()

        [<Test>] 
        member test.`` Test if then else `` () = 
            <@ testIfThenElse 42 @> |> test.ExecuteExpression |> should equal "Magic" |> ignore
            <@ testIfThenElse 41 @> |> test.ExecuteExpression |> should equal "Boring"

        [<Test>] 
        member test.`` Test match with `` () = 
            <@ testMatchWith 42 @> |> test.ExecuteExpression |> should equal "Magic" |> ignore
            <@ testMatchWith 41 @> |> test.ExecuteExpression |> should equal "Boring"

        [<Test>] 
        member test.`` Test Combine `` () = 
            <@ cloud { return (); return 42 } @> |> test.ExecuteExpression |> should equal 42

        [<Test>]
        member test.`` Test Sequential `` () = 
            <@ testSequential @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.`` Test For Loop `` () = 
            <@ testForLoop @> |> test.ExecuteExpression |> should equal 1000

        [<Test>]
        member test.`` Test For Loop as last expr `` () = 
            <@ cloud { for _ in [|1..10|] do () } @> 
            |> test.ExecuteExpression |> should equal ()

        [<Test>]
        member test.`` Test While Loop `` () = 
            <@ testWhileLoop @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.`` Test While Loop as last expr `` () = 
            <@ cloud { while false do () } @> 
            |> test.ExecuteExpression |> should equal ()


        [<Test>]
        member test.``Test Cloud Using`` () =
            <@ testUseWithCloudDisposable () @> |> test.ExecuteExpression |> should equal true

        [<Test>]
        member test.``Test Cloud Using With Exception`` () =
            <@ testUseWithException () @> |> test.ExecuteExpression |> should equal true

        [<Test>]
        member test.`` Test Recursion `` () = 
            <@ testRecursion 2 @> |> test.ExecuteExpression |> should equal 3

        [<Test>]
        member test.`` Test Tail (return!) Recursion `` () = 
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
        member test.`` Test Mutual Recursion `` () = 
            <@ even 2 @> |> test.ExecuteExpression |> should equal true;
            <@ even 3 @> |> test.ExecuteExpression |> should equal false

        [<Test>]
        member test.``Test sequence application `` () = 
            <@ (cloud { return 2 }, cloud { return 1 }) ||> (fun first second -> cloud { let! _ = first in return! second }) @> |> test.ExecuteExpression |> should equal 1;
            

        [<Test>]
        member test.``Test sequential combine (<.>) `` () = 
            <@ cloud { return 1 } <.> cloud { return "2" } @> |> test.ExecuteExpression |> should equal (1, "2")

        [<Test>]
        member test.``Test Cloud OfAsync`` () =
            <@ cloud { 
                let! value = Cloud.OfAsync(async { return 1 })
                return value + 1
               } @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``Test Cloud OfAsync Exception handling`` () =
            <@ cloud { 
                try
                    let! value = Cloud.OfAsync(async { return raise <| new InvalidOperationException() })
                    return 1
                with _ -> return -1
               } @> |> test.ExecuteExpression |> should equal -1

        [<Test>]
        member test.``Test Cloud OfAsync Looping `` () =
            <@ cloud {
                for i in [|1..100000|] do
                    let! value = Cloud.OfAsync(async { return 1 })
                    ()
                return 42
               } @> |> test.ExecuteExpression |> should equal 42

        [<Test>]
        member test.``Test mapReduce Lib`` () =
            <@ mapReduce mapF reduceF 0 [1..2] @>
            |> test.ExecuteExpression |> should equal 3

        [<Test>]
        member test.``Test Cloud returned closure serialization`` () =
            let f : int -> int =
                <@ cloud { 
                        let y = 1
                        return (fun x -> x + y)
                   } @> |> test.ExecuteExpression 
            f 2 |> should equal 3

        [<Test>]
        member test.``Test parallel random sum for GZipStream behavior`` () =
            <@ randomSumParallel() @> |> test.ExecuteExpression |> ignore

        [<Test>][<ExpectedException(typeof<CloudException>)>]
        member test.``Test ambiguous match exception in parallel cloud exception construction`` () =
            <@ testAmbiguousParallelException () @> |> test.ExecuteExpression |> ignore

        [<Test>]
        member test.``Test simple CloudRef`` () = 
            <@ testSimpleCloudRef 42 @> |> test.ExecuteExpression |> should equal 42

        [<Test>]
        member test.``Test CloudRef (New-Get) by name`` () = 
            <@ cloud { 
                let container = Guid.NewGuid().ToString()
                let! r = CloudRef.New(container, 42)
                let! cloudRef = CloudRef.Get<int>(container, r.Name) 
                return cloudRef.Value } @> |> test.ExecuteExpression |> should equal 42

        [<Test>][<ExpectedException(typeof<CloudException>)>]
        member test.``Test CloudRef Get by name - type mismatch`` () = 
            <@ cloud { 
                let container = Guid.NewGuid().ToString()
                let! r = CloudRef.New(container, 42)
                let! cloudRef = CloudRef.Get<obj>(container, r.Name) 
                () } @> |> test.ExecuteExpression |> ignore

        [<Test>]
        member test.``Test CloudRef Get all in container`` () = 
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
        member test.``Test CloudRef Get by random name - failure`` () = 
            <@  cloud {
                    let container, id = Guid.NewGuid().ToString(), Guid.NewGuid().ToString()
                    return! CloudRef.TryGet<int>(container, id)
                } @> |> test.ExecuteExpression |> should equal None

        [<Test>]
        member test.``Test CloudRef - inside cloud non-monadic deref`` () = 
            <@ cloud { let! x = newRef 42 in return x.Value } @> |> test.ExecuteExpression |> should equal 42

        [<Test>]
        member test.``Test CloudRef - outside cloud non-monadic deref`` () = 
            <@ cloud { let! x = newRef 42 in return x } @> |> test.ExecuteExpression |> (fun x -> x.Value) |> should equal 42

        [<Test>]
        member test.``Test Parallel CloudRef dereference`` () = 
            <@ testParallelCloudRefDeref 1 @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``Test Cloud List`` () = 
            let cloudList = <@ testBuildCloudList 2 @> |> test.ExecuteExpression 
            <@ testReduceCloudList cloudList @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``Test Cloud Tree`` () = 
            let cloudTree = <@ testBuildCloudTree 2 @> |> test.ExecuteExpression 
            <@ testReduceCloudTree cloudTree @> |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``Test CloudRef Tree Node Composition`` () = 
            let firstRef = <@ cloud { return! newRef <| Leaf 1 } @> |> test.ExecuteExpression 
            let secondRef = <@ cloud { return! newRef <| Leaf 2 } @> |> test.ExecuteExpression 
            <@ cloud { return! newRef <| Node (firstRef, secondRef) } @> |> test.ExecuteExpression |> ignore


        [<Test>]
        member test.``Test RunLocal Cloud`` () = 
              cloud { 
                    let x = ref 1
                    let! _ = cloud { 
                                do x := !x + 1
                                return ()
                             }
                    return !x 
              } |> MBrace.RunLocal |> should equal 2

        [<Test>]
        member test.``Test Cloud ToLocal``() = 
            <@ cloud { 
                let! (values : int []) = local <| Cloud.Parallel [| for i in [1..1000] -> cloud { return i } |]
                return values |> Array.sum
               } @> |> test.ExecuteExpression |> should equal 500500

        [<Test>]
        member test.``Test Cloud ToLocal (Side-Effects)`` () =
            <@ cloud { 
                let testRef = ref 1
                let! value = local <| Cloud.Parallel [| cloud { return testRef := !testRef + 1 } |]
                return !testRef
               } @> |> test.ExecuteExpression |> should equal 1

        [<Test>]
        member test.``Test Local Cloud GetWorkerCount``() = 
            <@ cloud { 
                let! c' = local <| cloud { let! c = Cloud.GetWorkerCount() in return c }
                return c'
               } @> |> test.ExecuteExpression |> should equal Environment.ProcessorCount

        [<Test>]
        member test.``Test Distrib Cloud GetWorkerCount``() = 
            <@ cloud { 
                let! c' = cloud { let! c = Cloud.GetWorkerCount() in return c }
                return c'
               } @> |> test.ExecuteExpression |> shouldMatch (fun c -> c <> 0)

        [<Test>]
        member test.``Test Cloud GetProcessId``() = 
            <@ cloud { 
                return! Cloud.GetProcessId() 
               } @> |> test.ExecuteExpression |> shouldMatch (fun pid -> pid > 0)

        [<Test>]
        member test.``Test Cloud GetTaskId``() = 
            <@ cloud { 
                return! Cloud.GetTaskId()
               } @> |> test.ExecuteExpression |> shouldMatch (fun taskId -> Microsoft.FSharp.Core.Operators.not <| String.IsNullOrEmpty(taskId))

        [<Test>] 
        member test.``Test Choice `` () =
            let result : int option = <@ cloud { let! r = Cloud.Choice [|cloud { return None }; cloud { return Some 1 }|] in return r } @> |> test.ExecuteExpression
            result |> should equal (Some 1)

        [<Test>] 
        member test.``Test Local Choice `` () =
            let result : int option = <@ cloud { let! r = local <| Cloud.Choice [|cloud { return None }; cloud { return Some 1 }|] in return r } @> |> test.ExecuteExpression
            result |> should equal (Some 1)

        [<Test>] 
        [<ExpectedException(typeof<CloudException>)>]
        member test.``Test Choice Exception`` () =
            <@ cloud { let! r = Cloud.Choice [|cloud { return None }; cloud { return raise <| new InvalidOperationException() }|] in return r } @> |> test.ExecuteExpression |> ignore
        
        [<Test; Repeat 80>]
        member test.``Test Choice Recursive`` () =
            <@  let rec test maxDepth depth id = cloud {
                    if depth >= maxDepth then
                        if id = 4 then return Some () else return None
                    else
                        return! Cloud.Choice [| test maxDepth (depth+1) (id + 1)
                                                test maxDepth (depth+1) (id + 2) |]
                }
                test 3 0 0
            @>
            |> test.ExecuteExpression |> should equal (Some ())

        abstract SerializationTest : unit -> unit
        [<Test>] 
        member test.``Test Serialization Exception`` () = 
            test.SerializationTest()

        [<Test>] 
        member test.``Test Cloud Log`` () = 
            let ps = <@ cloud { do! Cloud.Log "Cloud Log Test Msg" } @> |> test.Runtime.CreateProcess
            if test.Name = "MultiNode" then
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

        [<Test>] 
        member test.``Test Cloud Parallel Log`` () = 
            <@  cloud {
                    do! [| for i in 1..10 -> cloud { do! Cloud.Log "____________________________________________"  } |]
                        |> Cloud.Parallel
                        |> Cloud.Ignore
                } @> |> test.ExecuteExpression |> ignore

        [<Test>] 
        member test.``Test CloudSeq`` () = 
            <@  cloud {
                    let! cloudSeq = CloudSeq.New [1..100]
                    return cloudSeq |> Seq.length
                } @> |> test.ExecuteExpression |> should equal 100
        
        [<Test>] 
        member test.``Test CloudSeq exta info - Count`` () = 
            <@  cloud {
                    let! cloudSeq =  CloudSeq.New [1..100]
                    return cloudSeq.Count = (cloudSeq |> Seq.length)
                } @> |> test.ExecuteExpression |> should equal true

        [<Test>] 
        member test.``Test CloudSeq exta info - Size`` () = 
            <@  cloud {
                    let! cloudSeq =  CloudSeq.New [1..100]
                    return cloudSeq.Size > 0L
                } @> |> test.ExecuteExpression |> should equal true

        [<Test>] 
        member test.``Test CloudSeq (New-Get) by name`` () = 
            <@  cloud {
                    let container = Guid.NewGuid().ToString()
                    let! s = CloudSeq.New(container, [1..100])
                    let! cloudSeq = CloudSeq.Get<int>(container, s.Name)
                    return cloudSeq |> Seq.length
                } @> |> test.ExecuteExpression |> should equal 100

        [<Test>][<ExpectedException(typeof<CloudException>)>]
        member test.``Test CloudSeq Get by name - type mismatch`` () = 
            <@ cloud { 
                let container = Guid.NewGuid().ToString()
                let! r = CloudSeq.New(container, [42])
                let! cloudRef = CloudSeq.Get<obj>(container, r.Name) 
                () } @> |> test.ExecuteExpression |> ignore

        [<Test>]
        member test.``Test CloudSeq TryGet by name - failure`` () = 
            <@  cloud {
                    let container, id = Guid.NewGuid().ToString(), Guid.NewGuid().ToString()
                    return! CloudSeq.TryGet<int>(container, id)
                } @> |> test.ExecuteExpression |> should equal None
         
         
        [<Test>]
        member test.``Test CloudSeq Get all in container`` () = 
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
        member test.``Test CloudFile Create/Read - Lines`` () =
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
        member test.``Test CloudFile Create/Read - Stream #1`` () =
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
        member test.``Test CloudFile Get`` () =
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
        member test.``Test MutableCloudRef - Simple For Loop`` () =
            <@ cloud {
                let! x = MutableCloudRef.New(-1)
                for i in [|0..10|] do
                    do! MutableCloudRef.SpinSet(x, fun _ -> i)
                return! MutableCloudRef.Read(x)
            } @> |> test.ExecuteExpression |> should equal 10
            
        [<Test; Repeat 100>]
        member test.``Test MutableCloudRef - Set`` () = 
            let run () = 
                cloud {
                    let! x = MutableCloudRef.New(-1)
                    let! (x,y) = 
                        cloud { return! MutableCloudRef.Set(x, 1) } <||>
                        cloud { return! MutableCloudRef.Set(x, 2) }
                    return x <> y
                } 
            if test.Name = "MultiNode" then <@ run () @> else <@ run () |> local @>
            |> test.ExecuteExpression |> should equal true

        [<Test; Repeat 100>]
        member test.``Test MutableCloudRef - Set multiple`` () = 
            let run () =
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
            if test.Name = "MultiNode" then <@ run () @> else <@ run () |> local @>
            |> test.ExecuteExpression |> should equal (true,true)

        [<Test>]
        member test.``Test MutableCloudRef - Force`` () = 
            let run () = 
                cloud {
                 let! x = MutableCloudRef.New(-1)
                 let! _ = cloud { return! MutableCloudRef.Force(x, 1) } <||>
                          cloud { do! Cloud.OfAsync(Async.Sleep 3000)
                                  return! MutableCloudRef.Force(x, 2) }
                 return! MutableCloudRef.Read(x)
                }
            if test.Name = "MultiNode" then <@ run () @> else <@ run () |> local @>
            |> test.ExecuteExpression |> should equal 2

        [<Test>]
        member test.``Test MutableCloudRef - Free`` () =
            <@ cloud {
                let! x = MutableCloudRef.New(0)
                do! MutableCloudRef.Free(x)
                return! MutableCloudRef.TryRead(x)
            } @> |> test.ExecuteExpression |> should equal None

        [<Test; Repeat 100>]
        member test.``Test MutableCloudRef - High contention`` () = 
            let run () =
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
            if test.Name = "MultiNode" then <@ run () @> else <@ run () |> local @>
            |> test.ExecuteExpression |> should equal true

        [<Test; Repeat 100>]
        member test.``Test MutableCloudRef - High contention - Large obj`` () = 
            let run () = 
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
            if test.Name = "MultiNode" then <@ run () @> else <@ run () |> local @>
            |> test.ExecuteExpression |> should equal true

        [<Test>]
        member test.``Test MutableCloudRef - Token passing`` () = 
            let run () = 
                cloud {
                    let rec run (id : int) (locks : MVar<unit> []) (token : IMutableCloudRef<int>) : ICloud<int option> = 
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
            if test.Name = "MultiNode" then <@ run () @> else <@ run () |> local @>
            |> test.ExecuteExpression |> should equal true

        [<Test>]
        member test.``Test MutableCloudRef - Get all in container`` () = 
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


        [<Test; Repeat 10>]
        member test.``Test UnQuote Exception`` () =
            <@ cloud { 
                    try
                        return raise <| System.InvalidOperationException() 
                    with :? System.InvalidOperationException -> return -1
                         | _ -> return -2
               } 
            @> |> test.ExecuteExpression |> should equal -1

        [<Test>]
        member test.``Test Fetch logs`` () =
            if test.Name = "MultiNode" then
                test.Runtime.GetLogs() |> Seq.isEmpty |> should equal false
                test.Runtime.Nodes.Head.GetLogs() |> Seq.isEmpty |> should equal false


    open System.Net

    open Nessos.Thespian
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Remote.TcpProtocol

    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Client
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils 
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime.Definitions

    type internal RuntimeMsg = CommonAPI.Runtime
    type internal RuntimeInfo = Actor<RuntimeMsg> * MBraceRuntime

//    [<TestFixture>]
//    type ``Cloud scenaria`` =
//        inherit TestBed
//        
//        val runtimeInfo: RuntimeInfo option ref
//
//        new () = {
//            inherit TestBed()
//
//            runtimeInfo = ref None
//        }
//
//        member private test.GetRuntime () =
//            match test.runtimeInfo.Value with
//            | Some(_, runtime) -> runtime
//            | None -> test.Init(); test.GetRuntime()
//
//        override test.Name = "Local"
//        override test.Runtime = test.GetRuntime()
//
//        override test.InitRuntime() =
//            IoC.SetOverrideBehaviour(OverrideBehaviour.Override)
//            do Defaults.setCachingDirectory()
//            
//            MBraceSettings.StoreProvider <- LocalFS
//            MBraceSettings.MBracedExecutablePath <- Defaults.mbracedExe
//            MBraceSettings.ClientSideExpressionCheck <- false
//            
////            MBrace.ClientInit(
////                ILoggerFactory = (Logger.createConsoleLogger >> Logger.wrapAsync),
////                IStoreFactory = (fun () -> new FileSystemStore("teststore", System.Configuration.ConfigurationManager.AppSettings.["StorePath"]) :> IStore),
////                MBracedPath = Defaults.mbracedExe,
////                ClientSideExprCheck = false,
////                InitSocketListenerPool = false,
////                DefaultSerializer = Defaults.defaultSerializer
////            )
//
//            IoC.RegisterValue(false, "IsolateProcesses", behaviour = Override)
//            IoC.Register<ILogger>(Logger.createConsoleLogger, behaviour = Override)
//            
////            TcpListenerPool.Clear()
//
//            match test.runtimeInfo.Value with
//            | None -> 
//                let actor = Address.Parse(sprintf' "localhost:%d" Defaults.RuntimeDefaultPort) |> Service.bootSingle
//
//                let slaveNodeCount = 5
//                
//
//                (!actor <!= fun ch -> MasterBoot(ch, Configuration.Null)) |> ignore
//                let runtime = MBraceRuntime.FromActor(actor)
//                test.runtimeInfo := Some(actor, runtime)
//            | Some _ -> ()
//
//        override test.FiniRuntime() =
//            match test.runtimeInfo.Value with
//            | Some(actor, runtime) -> 
//                runtime.Shutdown()
//                use d = actor
//               // use d' = runtime
//                test.runtimeInfo := None
//            | None -> ()
//
//        override test.ExecuteExpression<'T>(expr: Expr<ICloud<'T>>): 'T =
//            let runtime = test.GetRuntime()
//
//            MBrace.RunRemote runtime expr
//
//        override test.SerializationTest() = 
//            ()

    
