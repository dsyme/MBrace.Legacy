namespace Nessos.MBrace.Runtime.Tests

    open System
    open System.IO

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.AssemblyCache
    open Nessos.MBrace

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
            

