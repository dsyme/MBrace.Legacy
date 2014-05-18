namespace Nessos.MBrace.Runtime.Tests

    open System
    open System.Runtime.Serialization
    open System.Collections.Generic

    open Nessos.MBrace
    open Nessos.MBrace.Utils

    type MVar<'T> = IMutableCloudRef<'T option>

    [<Cloud>]
    module MVar =
        let newEmpty<'T> : ICloud<MVar<'T>> = MutableCloudRef.New(None)
        let newValue<'T> value : ICloud<MVar<'T>> = MutableCloudRef.New(Some value)
        let rec put (mvar : MVar<'T>) value = 
            cloud {
                let! v = MutableCloudRef.Read(mvar)
                match v with
                | None -> 
                    let! ok = MutableCloudRef.Set(mvar, Some value)
                    if not ok then return! put mvar value
                | Some _ ->
                    return! put mvar value
            }
        let rec take (mvar : MVar<'T>) =
            cloud {
                let! v = MutableCloudRef.Read(mvar)
                match v with
                | None -> 
                    return! take mvar
                | Some v -> 
                    let! ok = MutableCloudRef.Set(mvar, None)
                    if not ok then return! take mvar
                    else return v
            }
    

    [<AutoOpen>]
    module CloudFileExtensions =
        open System
        open System.IO
        open System.Text
        open System.Collections
        open System.Collections.Generic
        open System.Runtime.Serialization

        open Nessos.MBrace.Client

        type CloudFile with
        
            [<Cloud>]
            static member ReadLines(file : ICloudFile, ?encoding : Encoding) =
                cloud {
                    let reader (stream : Stream) = async {
                        let s = seq {
                            use sr = 
                                match encoding with
                                | None -> new StreamReader(stream)
                                | Some e -> new StreamReader(stream, e)
                            while not sr.EndOfStream do
                                yield sr.ReadLine()
                        }
                        return s
                    }
                    return! CloudFile.ReadAsSeq(file, reader)
                }

            [<Cloud>]
            static member WriteLines(container : string, name : string, lines : seq<string>, ?encoding : Encoding) =
                cloud {
                    let writer (stream : Stream) = async {
                        use sw = 
                            match encoding with
                            | None -> new StreamWriter(stream)
                            | Some e -> new StreamWriter(stream, e)
                        for line in lines do
                            do! Async.AwaitTask(sw.WriteLineAsync(line).ContinueWith(ignore))
                    }
                    return! CloudFile.Create(container, name, writer)
                }

            [<Cloud>]
            static member ReadAllText(file : ICloudFile, ?encoding : Encoding) =
                cloud {
                    let reader (stream : Stream) = async {
                        use sr = 
                            match encoding with
                            | None -> new StreamReader(stream)
                            | Some e -> new StreamReader(stream, e)
                        return sr.ReadToEnd()
                    }
                    return! CloudFile.Read(file, reader)
                }

            [<Cloud>]
            static member WriteAllText(container : string, name : string, text : string, ?encoding : Encoding) =
                cloud {
                    let writer (stream : Stream) = async {
                        use sw = 
                            match encoding with
                            | None -> new StreamWriter(stream)
                            | Some e -> new StreamWriter(stream, e)
                        do! Async.AwaitTask(sw.WriteAsync(text).ContinueWith(ignore))
                    }
                    return! CloudFile.Create(container, name, writer)
                }
        
            [<Cloud>]
            static member ReadAllBytes(file : ICloudFile) =
                cloud {
                    let reader (stream : Stream) = async {
                        use ms = new MemoryStream()
                        do! Async.AwaitTask(stream.CopyToAsync(ms).ContinueWith(ignore))
                        return ms.ToArray() :> seq<byte>
                    }
                    return! CloudFile.Read(file, reader)
                }

            [<Cloud>]
            static member WriteAllBytes(container : string, name : string, buffer : byte []) =
                cloud {
                    let writer (stream : Stream) = async {
                        do! Async.AwaitTask(stream.WriteAsync(buffer, 0, buffer.Length).ContinueWith(ignore))
                    }
                
                    return! CloudFile.Create(container, name, writer)
                }


    
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

        type DummyDisposable private (isDisposed : IMutableCloudRef<bool>) =
            member __.IsDisposed = isDisposed.Value
            interface ICloudDisposable with
                member __.Dispose () = isDisposed.ForceUpdate true
                member __.GetObjectData(si,_) = si.AddValue("isDisposed", isDisposed)

            internal new (si : SerializationInfo, _ : StreamingContext) =
                new DummyDisposable(si.GetValue("isDisposed", typeof<IMutableCloudRef<bool>>) :?> IMutableCloudRef<bool>)
            
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
            

//    module Defaults =
//
//        let mbracedExe = Path.GetTempPath
////#if DEBUG
////            "../../../Nessos.MBrace.Runtime.Daemon/bin/Debug/mbraced.exe" |> Path.GetFullPath
////#else
////            "../../../Nessos.MBrace.Runtime.Daemon/bin/Release/mbraced.exe" |> Path.GetFullPath
////#endif
//
//        let setCachingDirectory =
//            fun () ->
//                let name = sprintf "nunit.%d" <| System.Diagnostics.Process.GetCurrentProcess().Id
//                let workingDir = Path.Combine(Path.GetTempPath(), name)
//                if not <| Directory.Exists workingDir then Directory.CreateDirectory workingDir |> ignore
//
//                do 
//                    Directory.SetCurrentDirectory workingDir
//                    AssemblyCache.SetCacheDir workingDir
////                IoC.RegisterValue(Override,workingDir,"AssemblyCachePath")
//            |> runOnce
    