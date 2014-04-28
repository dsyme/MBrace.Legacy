#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Utils.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Actors.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Base.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Store.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Client.dll"

open Nessos.MBrace.Client


let partitionPairs length n =
    [| for i in 0 .. n - 1 ->
        let i, j = length * i / n, length * (i + 1) / n
        (i + 1, j) |]

let partitionedMult (A : float[][]) (b : float[]) (startPos : int) (endPos : int) = Array.init (endPos - startPos + 1) (fun x1 -> b |> Array.map2 (fun r c -> r*c) (A.[x1 + startPos]) |> Array.fold (+) 0.0)
let partitionedDot (a : float[]) (b : float[]) (startPos : int) (endPos : int) =  b.[startPos..endPos] |> Array.map2 (fun r c -> r*c) (a.[startPos..endPos]) |> Array.fold (+) 0.0
let partitionedScale (a : float) (b : float[]) (c : float[]) (startPos : int) (endPos : int) =  Array.map2 (fun bV cV -> a*bV+cV) b.[startPos..endPos] c.[startPos..endPos]
    
[<Cloud>]
let prepareMults (A : float[][]) (b : float[]) n numberOfPartitions = [| for (fromN, toN) in partitionPairs n numberOfPartitions -> cloud { return partitionedMult A b (fromN - 1) (toN - 1) } |]
[<Cloud>]
let prepareDots (a : float[]) (b : float[]) n numberOfPartitions = [| for (fromN, toN) in partitionPairs n numberOfPartitions -> cloud { return partitionedDot a b (fromN - 1) (toN - 1) } |]
[<Cloud>]
let prepareScales (a : float) (b : float[]) (c : float[]) n numberOfPartitions = [| for (fromN, toN) in partitionPairs n numberOfPartitions -> cloud { return partitionedScale a b c (fromN - 1) (toN - 1) } |]

[<Cloud>]
let parallelMults (A : float[][]) (b : float[]) n numberOfPartitions =
    cloud { 
        let! values = Cloud.Parallel(prepareMults A b n numberOfPartitions)
        return values |> Seq.concat |> Seq.toArray
    }
[<Cloud>]
let parallelDots (a : float[]) (b : float[]) n numberOfPartitions =
    cloud { 
        let! values = Cloud.Parallel(prepareDots a b n numberOfPartitions)
        return values |> Array.fold (+) 0.0
    }
[<Cloud>]
let parallelScales n numberOfPartitions (a : float) (b : float[]) (c : float[]) =
    cloud { 
        let! values = Cloud.Parallel(prepareScales a b c n numberOfPartitions)
        return values |> Seq.concat |> Seq.toArray
    }

//[<Cloud>]
//let testlog = 
//    cloud { 
//        do! Cloud.Log("Skatakia!")
//    }

let readData() = 
    let s1 = System.IO.File.ReadAllLines(@"c:\KPCG.csv");
    let partitionSize = 2;
    let arraySize = s1.Length;
    let A = Array.init arraySize (fun r -> 
                                    let a = s1.[r].Split(',');
                                    Array.init arraySize (fun c -> System.Double.Parse(a.[c], System.Globalization.CultureInfo.InvariantCulture))
                                    );
    let s2 = System.IO.File.ReadAllLines(@"c:\fPCG.csv");
    let b = Array.init arraySize (fun r -> System.Double.Parse(s2.[r], System.Globalization.CultureInfo.InvariantCulture));
    (A, b, arraySize, partitionSize)

[<Cloud>]
let initialize (A, b, arraySize, partitionSize) = 
    cloud {
        let x = Array.init arraySize (fun _ -> 0.0);
        let r = b;
        let z = r; // should precondition
        let p = z;

        let! q = parallelMults A p arraySize partitionSize
        let! (a, b) = (parallelDots p r arraySize partitionSize) <||> (parallelDots p q arraySize partitionSize)
        let eta = a / b
        do! Cloud.Logf "%A" q
        do! Cloud.Logf "%A" a
        do! Cloud.Logf "%A" b
        do! Cloud.Logf "%A" eta
        return (x, r, z, p, q, eta)
    }

[<Cloud>]
let pcg (A, b, arraySize, partitionSize, maxIterations) = 
        cloud {
    //        let parallelScalesPartial = parallelScales arraySize partitionSize
            let x = Array.init arraySize (fun x -> 0.0) |> ref
            let r = Array.init arraySize (fun x -> 0.0) |> ref
            let z = Array.init arraySize (fun x -> 0.0) |> ref
            let p = Array.init arraySize (fun x -> 0.0) |> ref
            let q = Array.init arraySize (fun x -> 0.0) |> ref
            let eta = ref 0.0
            let x0 = Array.init arraySize (fun x -> 0.0) |> ref
            let r0 = Array.init arraySize (fun x -> 0.0) |> ref
            let z0 = Array.init arraySize (fun x -> 0.0) |> ref
            let p0 = Array.init arraySize (fun x -> 0.0) |> ref
            let! (xT, rT, zT, pT, qT, etaT) = initialize (A, b, arraySize, partitionSize)
            x := xT
            r := rT
            z := zT
            p := pT
            q := qT
            eta := etaT
            let! errorDenominator = parallelDots !r !r arraySize partitionSize
            let error = ref 80.0
            let iterations = ref 0
            let errorList = new System.Collections.Generic.List<float>()
            while (!error > 1e-5 && !iterations < maxIterations) do
                //do! Cloud.Logf "%A" !error
                x0 := !x;
                r0 := !r;
                let! xT = parallelScales arraySize partitionSize !eta !p !x0 
                let! rT = parallelScales arraySize partitionSize -(!eta) !q !r0
                x := xT;
                r := rT;
                let! errorNominator = parallelDots !r !r arraySize partitionSize
                error := sqrt errorNominator / errorDenominator
                errorList.Add(!error)
                z0 := !z;
                z := !r; // should precondition
                let! (a, b) = (parallelDots !z !r arraySize partitionSize) <||> (parallelDots !z0 !r0 arraySize partitionSize)
                p0 := !p
                let! pT = parallelScales arraySize partitionSize (a / b) !p0 !z
                p := pT
                let! qT = parallelMults A !p arraySize partitionSize
                q := qT
                let! (a, b) = (parallelDots !z !r arraySize partitionSize) <||> (parallelDots !p !q arraySize partitionSize)
                eta := a / b
                iterations := !iterations + 1

            return (!x, errorList, !iterations)
        }


let runtime = MBrace.InitLocal 4
//let (A, b, arraySize, partitionSize) = readData()
//let proc = runtime.Run <@ pcg (A, b, arraySize, partitionSize, 2) @>

let foo = readData()
let proc = 
    let (A, b, arraySize, partitionSize) = foo
    runtime.Run <@ pcg (A, b, arraySize, partitionSize, 51) @>


runtime.ShowProcessInfo()
printf "%A" A

proc.AwaitResult()
//let proc = runtime.CreateProcess <@ pcg (A, b, arraySize, partitionSize) @>
runtime.ShowUserLogs()

runtime.ShowProcessInfo()

//
// -----------------------------------------------
// PCG perf bottleneck

[<Cloud>]
let rec testIterative n =
    cloud {
        for i in [|1..n|] do
            let! (x, y) = cloud { return i * 2 } <||> cloud { return i * 3 }
            ()
    }

let runtime = MBrace.InitLocal 20

let proc10 = runtime.CreateProcess <@ testIterative 10 @>
let proc100 = runtime.CreateProcess <@ testIterative 100 @>
let proc1000 = runtime.CreateProcess <@ testIterative 1000 @>


(*
Quoatations Compiler | N | Time | Workers
F | 10 | 00:00:04.5722615 | 2
F | 10 | 00:00:02.5151439 | 2
F | 10 | 00:00:02.5121437 | 2
F | 100 | 00:00:26.0124879 | 2 
F | 100 | 00:00:23.9533700 | 2
F | 100 | 00:00:24.2453868 | 2
F | 1000 | 00:04:09.1322496 | 2
F | 1000 | 00:04:06.0020705 | 2
F | 1000 | 00:04:08.4192088 | 2
T | 10 | 00:00:05.6953258 | 2
T | 10 | 00:00:03.5732044 | 2
T | 10 | 00:00:03.1421798 | 2
T | 100 | 00:00:35.0260034 | 2
T | 100 | 00:00:29.5216886 | 2
T | 100 | 00:00:28.8866522 | 2
T | 1000 | 00:05:01.3082339 | 2
T | 1000 | 00:05:00.5211888 | 2
T | 1000 | 00:04:53.1107650 | 2
*)
