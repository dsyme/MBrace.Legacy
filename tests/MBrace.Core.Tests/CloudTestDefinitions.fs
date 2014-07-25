#nowarn "0044" // 'While loop considered harmful' message.

namespace Nessos.MBrace.Core.Tests

    open System
    open System.Runtime.Serialization
    open System.Collections.Generic

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime.Logging
    
    [<AutoOpen>]
    module TestFunctions = 

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

        let traceHasValue (dumps : seq<CloudLogEntry>) name value =
            dumps 
            |> Seq.exists(fun e ->
                match e.TraceInfo with
                | None -> false
                | Some t ->
                    t.Environment.ContainsKey(name) && t.Environment.[name].Contains value)

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
        let mapF (v : int) = cloud { return v }

        [<Cloud>] 
        let reduceF left right = cloud { return left + right }

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

//        [<Cloud>]
//        let testSequential = 
//            cloud {
//                let list = new List<int>()
//                list.Add 1
//                list.Add 2
//                return list.Count
//            }

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
        let testSimpleCloudRef a = 
            cloud {
                let! ref = CloudRef.New a
                return ref.Value
            }

        [<Cloud>]
        let testParallelCloudRefDeref a = 
            cloud {
                let! ref = CloudRef.New a
                let! (x, y) = cloud { return ref.Value } <||> cloud { return ref.Value }
                return x + y
            }

        type CloudList<'T> = Nil | Cons of 'T * ICloudRef<CloudList<'T>>
        [<Cloud>]
        let rec testBuildCloudList a = 
            cloud {
                if a = 0 then 
                    return! CloudRef.New Nil
                else
                    let! tail = testBuildCloudList (a - 1)
                    return! CloudRef.New <| Cons (1, tail)
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
                    return! CloudRef.New <| Leaf 1
                else
                    let! left, right = testBuildCloudTree (a - 1) <.> testBuildCloudTree (a - 1)
                    return! CloudRef.New <| Node (left, right)
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
        let rec mapReduce   (mapF : 'T -> Cloud<'R>) 
                            (reduceF : 'R -> 'R -> Cloud<'R>) (identity : 'R) 
                            (values : 'T list) : Cloud<'R> =
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

        type DummyDisposable private (isDisposed : IMutableCloudRef<bool>) =
            member __.IsDisposed = isDisposed.Value
            interface ICloudDisposable with
                member __.Dispose () = isDisposed.ForceUpdate true
                member __.GetObjectData(si,_) = si.AddValue("isDisposed", isDisposed)

            internal new (si : SerializationInfo, _ : StreamingContext) =
                new DummyDisposable(si.GetValue("isDisposed", typeof<IMutableCloudRef<bool>>) :?> IMutableCloudRef<bool>)
            
            [<Cloud>]
            static member Create () = cloud { let! ref = MutableCloudRef.New false in return new DummyDisposable(ref) }

        [<Cloud>]
        let testUseWithCloudDisposable () = cloud {
            let! d, wasDisposed =
                cloud {
                    use! d = DummyDisposable.Create ()
                    return d, d.IsDisposed
                }

            return not wasDisposed && d.IsDisposed // should be true
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

        [<Cloud>]
        let schedulerNoOp = Cloud.Ignore <| Cloud.Parallel [cloud.Zero(); cloud.Zero()]
        [<Cloud>]
        let rec spinWait mref =
            cloud {
                let! v = MutableCloudRef.Read mref
                if v then return () 
                else
                    do! Cloud.OfAsync <| Async.Sleep 100
                    return! spinWait mref
            }
        [<Cloud>]
        let testParallelWithExceptionCancellation flag =
            cloud {
                try
                    do! Cloud.Ignore <| 
                        (cloud { 
                            return invalidOp "Fratricide!"
                        }
                        <||>
                        cloud { //Task A
                            do! spinWait flag
                            //scheduler has received the exception and cancellation was triggered
                            //we force new scheduler transition
                            //1). the current task is cancelled before the transition is triggered => nothing happens
                            //2). the transition is triggered before cancellation occurs => scheduler will ignore transition, no new tasks
                            do! schedulerNoOp
                            //everything following this is dead code
                            do! Cloud.Ignore <| MutableCloudRef.Force(flag, false)
                        })
                with _ ->
                    //cancellation has already been triggered
                    //1). Task A will not have time to see the flag set
                    //2). Task A will see the flag set => Task A performs a scheduler transition
                    do! Cloud.Ignore <| MutableCloudRef.Set(flag, true)
            }
            

    module PrimesTest = 

        let isPrime (n:int) =
            let bound = int (System.Math.Sqrt(float n))
            seq {2 .. bound} |> Seq.exists (fun x -> n % x = 0) |> not

        let primes (fromN, toN) = [| for i = fromN to toN do if isPrime i then yield i |]

        let partitionPairs length n =
            [| 
                for i in 0 .. n - 1 ->
                    let i, j = length * i / n, length * (i + 1) / n in (i + 1, j) 
            |]

        [<Cloud>]
        let prepareCloudPrimes n numberOfPartitions = [| for (fromN, toN) in partitionPairs n numberOfPartitions -> cloud { return primes (fromN, toN) }  |]

        [<Cloud>]
        let parallelPrimes n numberOfPartitions =
            cloud { 
                let! values = Cloud.Parallel(prepareCloudPrimes n numberOfPartitions)
                return values |> Array.concat |> Seq.skip 1 |> Seq.toArray
            }