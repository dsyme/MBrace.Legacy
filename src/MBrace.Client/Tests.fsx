#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Shell.Shared.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Utils.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Actors.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Actors.Remote.dll"
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

let readData() = 
    let s1 = System.IO.File.ReadAllLines(@"C:\Users\anirothan\Documents\KPCG.csv");
    let partitionSize = 2;
    let arraySize = s1.Length;
    let A = Array.init arraySize (fun r -> 
                                    let a = s1.[r].Split(',');
                                    Array.init arraySize (fun c -> System.Double.Parse(a.[c]))
                                 );
    let s2 = System.IO.File.ReadAllLines(@"C:\Users\anirothan\Documents\fPCG.csv");
    let b = Array.init arraySize (fun r -> System.Double.Parse(s2.[r]));
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
        return (x, r, z, p, q, eta)
    }

[<Cloud>]
let pcg (A, b, arraySize, partitionSize) =
    cloud {
        let parallelScalesPartial = parallelScales arraySize partitionSize
        let! (x, r, z, p, q, eta) = initialize (A, b, arraySize, partitionSize)
        let! errorDenominator = parallelDots r r arraySize partitionSize
        let error = ref 100.0
        let iterations = ref 0
        let errorList = new System.Collections.Generic.List<float>()
        while (!error > 1e-5 && !iterations < 200) do
            let x0 = x;
            let r0 = r;
            let! x = parallelScalesPartial eta p x0 
            let! r = parallelScalesPartial -eta q r0
            let! errorNominator = parallelDots r r arraySize partitionSize
            error := errorNominator / errorDenominator
            errorList.Add(!error)
            let z0 = z;
            let z = r; // should precondition
            let! (a, b) = (parallelDots z r arraySize partitionSize) <||> (parallelDots z0 r0 arraySize partitionSize)
            let! p = parallelScalesPartial (a / b) p z
            let! q = parallelMults A p arraySize partitionSize
            let! (a, b) = (parallelDots p r arraySize partitionSize) <||> (parallelDots p q arraySize partitionSize)
            let eta = a / b
            iterations := !iterations + 1

        return (x, errorList, iterations)
    }


let runtime = MBrace.InitLocal 2
let (A, b, arraySize, partitionSize) = readData();;
let proc = runtime.CreateProcess <@ pcg (A, b, arraySize, partitionSize) @>



// end of init
#time
[<Cloud>]
let hello : ICloud<int> =
    cloud {
        return failwith "fuck!"
    }

let runtime = MBrace.InitLocal 3

runtime.Run <@ hello @>

[<Cloud>]
let rec test i =
    cloud { 
        if i = 0 then return 42
        else 
            return! test (i - 1)
    } 

runtime.Run <@ test 1000000 @>

runtime.Run <@ cloud { let! r = [1..1000] |> Cloud.parmap (fun i -> cloud { return i }) in return r |> List.length } @>

runtime.ShowProcessInfo()



runtime.GetProcess<int> 9131

runtime.ShowInfo ()

runtime.Reboot()


[<Cloud>]
let testFunc x y =
    cloud { return x + y }

let comp = Cloud.Compile <@ testFunc @>

comp.Invoke (1,2) |> runtime.Run

let proc = runtime.CreateProcess <| comp'.Invoke 9

proc.Result

runtime.ProcessInfo

let proc' = runtime.GetProcess<int> "221730c3-3764-4a90-af01-287dd93a78ab"

proc'.Result

//
// Demo : Thumbnail resize
//

#load "/Users/eirik/Desktop/thumbnail.fsx"

open System.IO

[<Cloud>]
let createThumbnails (sourceFolder : string) (destinationFolder : string) = 
    cloud {
        let sourceImages = Directory.GetFiles sourceFolder

        do!
            sourceImages
            |> Array.toList
            |> Cloud.mapf (fun img -> Thumbnail.Create img destinationFolder)
            |> Cloud.ignore
    }

let sourceDir = "/Users/eirik/Desktop/Mathematicians/"
let thumbDir = sourceDir + "Thumbs"

runtime.Run <@ createThumbnails sourceDir thumbDir @>

//
// mapreduce, wordcount
//

open Nessos.MBrace.Utils

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
let rec mapReduceD  (depth : int)
                    (mapF : 'T -> ICloud<'R>)
                    (reduceF : 'R -> 'R -> ICloud<'R>) 
                    (identity : 'R) (values : 'T list) : ICloud<'R> =
    cloud {
        match values with
        | [] -> return identity
        | [value] -> return! mapF value
        | _ -> 
            let (leftList, rightList) = List.split values
            let decomposeOp, depth' = 
                match depth with
                | n when n < 0 -> (<||>), depth
                | 0 -> (<.>), 0
                | _ -> (<||>), depth-1
            let! (left, right) = 
                decomposeOp (mapReduceD depth' mapF reduceF identity leftList) (mapReduceD depth' mapF reduceF identity rightList)
            return! reduceF left right
    }


//
// Example : Shakespeare
//

open System
open System.Collections.Generic

let noiseWords = 
    seq [
        "a"; "about"; "above"; "all"; "along"; "also"; "although"; "am"; "an"; "any"; "are"; "aren't"; "as"; "at";
        "be"; "because"; "been"; "but"; "by"; "can"; "cannot"; "could"; "couldn't"; "did"; "didn't"; "do"; "does"; 
        "doesn't"; "e.g."; "either"; "etc"; "etc."; "even"; "ever";"for"; "from"; "further"; "get"; "gets"; "got"; 
        "had"; "hardly"; "has"; "hasn't"; "having"; "he"; "hence"; "her"; "here"; "hereby"; "herein"; "hereof"; 
        "hereon"; "hereto"; "herewith"; "him"; "his"; "how"; "however"; "I"; "i.e."; "if"; "into"; "it"; "it's"; "its";
        "me"; "more"; "most"; "mr"; "my"; "near"; "nor"; "now"; "of"; "onto"; "other"; "our"; "out"; "over"; "really"; 
        "said"; "same"; "she"; "should"; "shouldn't"; "since"; "so"; "some"; "such"; "than"; "that"; "the"; "their"; 
        "them"; "then"; "there"; "thereby"; "therefore"; "therefrom"; "therein"; "thereof"; "thereon"; "thereto"; 
        "therewith"; "these"; "they"; "this"; "those"; "through"; "thus"; "to"; "too"; "under"; "until"; "unto"; "upon";
        "us"; "very"; "viz"; "was"; "wasn't"; "we"; "were"; "what"; "when"; "where"; "whereby"; "wherein"; "whether";
        "which"; "while"; "who"; "whom"; "whose"; "why"; "with"; "without"; "would"; "you"; "your" ; "have"; "thou"; "will"; 
        "shall"
    ]
    |> fun words -> new HashSet<string>(words)

let works = 
    //let location = "/mbrace/shakespeare/" in
    let location = "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/"
    [   
        "allswellthatendswell.txt" ; "comedyoferrors.txt" ; "cymbeline.txt" 
        "hamlet.txt" ; "henryiv1.txt" ; "henryv.txt" ; "henryvi2.txt" ; 
        "henryvi3.txt" ; "juliuscaesar.txt" ; "kinglear.txt" ; 
        "loveslobourslost.txt" ; "macbeth.txt" ; "merchantofvenice.txt" ;
        "othello.txt" ; "richardiii.txt" ; "romeoandjuliet.txt" ; "titus.txt"
        "twelfthnight.txt" ; "winterstale.txt"
    ]
    |> List.map (fun file -> location + file)
        

[<Cloud>]
let mapF (path : string) =
    cloud {
        let! text = Cloud.ofAsync <| AsyncUtils.Download (Uri path)
        let words = text.Split([|' '; '.'; ','|], StringSplitOptions.RemoveEmptyEntries)
        return 
            words
            |> Seq.map (fun word -> word.ToLower())
            |> Seq.map (fun t -> t.Trim())
            |> Seq.filter (fun word -> Seq.length word > 3 && not <| noiseWords.Contains(word) )
            |> Seq.groupBy id 
            |> Seq.map (fun (key, values) -> (key, values |> Seq.length))
            |> Seq.toArray
    }

[<Cloud>]
let reduceF (left: (string * int) []) (right: (string * int) []) = 
    cloud {
        return 
            Seq.append left right 
            |> Seq.groupBy fst 
            |> Seq.map (fun (key, value) -> (key, value |> Seq.sumBy snd ))
            |> Seq.sortBy (fun (_,t) -> -t)
            |> Seq.toArray
    }


// not working!
runtime.AttachLocal 3

runtime.Reboot()

open System.Reflection

let proc = runtime.CreateProcess <@ mapReduceD 3 mapF reduceF [||] works @>

proc.Info

//
//
//

#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Serialization.dll"

open Nessos.MBrace.Serialization
open Nessos.MBrace.Serialization.SerializationBuffer
open Nessos.MBrace.Serialization.QuotationSerialization


open Microsoft.FSharp.Quotations
open Nessos.MBrace.Utils
open Nessos.MBrace.Utils.Quotations

type Foo = Bar

let quot =
    <@
        cloud {
            try
                let! x = cloud { let rec fib n = if n <= 1 then 1 else fib(n-2) + fib(n-1) in return fib 12 }
                let! y = cloud { let! (x,y) = cloud { return 2 } <||> cloud { return 3 } in return x }
                //let! z = cloud { return Bar }

                if x > 0 then return 3
                else return 4
            with
            | :? System.ArgumentException as e -> return 2
            | _ -> return 1
        }
    @> |> Expr.Erase


let ndc = new NDCSerializer() :> ISerializer
let bfs = new BinaryFormatterSerializer() :> ISerializer
let dns' = new DynamicSerializer(ndc, compress = true)
let dns = dns' :> ISerializer


dns'.Test Bar

#time
open System

let ndcTest n =
    ndc.Serialize n |> ndc.Deserialize |> ignore

let bfsTest n =
    bfs.Serialize n |> bfs.Deserialize |> ignore

let dnsTest n =
    dns.Serialize n |> dns.Deserialize |> ignore

// try this!!!! :D
let foo = [1..10000] |> List.map (fun i -> (i, <@@ if true then i else i - 1 @@> )) |> Map.ofList

ndc.Serialize [1..2200] |> ndc.Deserialize

[1..30000].GetHashCode()

ndcTest foo
bfsTest foo
dnsTest foo

dns'.Test quot |> ignore




for _ in 1 .. 1000 do
    quot |> ndc.Serialize |> ndc.Deserialize |> ignore

for _ in 1..1000 do
    quot |> bfs.Serialize |> bfs.Deserialize |> ignore
    
for _ in  1..1000 do
    quot |> dns.Serialize |> dns.Deserialize |> ignore


open Nessos.MBrace.Serialization
open System

let stringArray mb =
    let s() = new String('0', 1024 * 1024)
    Array.init mb (fun _ -> s() )

let ndc = new NDCSerializer() :> ISerializer
let serializer = new DynamicSerializer(new NDCSerializer()) :> ISerializer
let s = stringArray 1024
//let s' = serializer.Serialize(s)
//let s'' = serializer.Deserialize(s')

GC.Collect(3)

s |> Array.map serializer.Serialize |> Array.map serializer.Deserialize |> ignore

#time
serializer.Serialize s |> serializer.Deserialize |> ignore
ndc.Serialize s |> ndc.Deserialize |> ignore

ndc.Deserialize bytes


type Peano = Zero | Succ of Peano

[<Cloud>]
let rec int2Peano n =   
    cloud {
        match n with
        | 0 -> return Zero
        | n ->
            let! pred = int2Peano (n-1)
            return Succ pred
    }

let r = MBrace.InitLocal 3

r.Run <@ int2Peano 12 @>

let r = MBrace.Connect("grothendieck", 53770)

r.ShowProcessInfo()

let p = r.GetProcess 7410

p


let r = MBrace.InitLocal 8
for i = 0 to 10000000 do
    r.Run <@ cloud.Return () @>
    printfn "%d" i

let r = MBrace.InitLocal 8
for i = 0 to 10000000 do
    r.Reboot()
    printfn "%d" i


[<Cloud>]
let test() = 
    cloud {
        while true do
            let! _ = cloud { return 1 } <||> cloud { return 2 } <||> cloud { return 3 } <||> cloud { return 4 } <||> cloud { return 5 }
            //let! _ = Cloud.Parallel [| cloud { return i }; cloud { return i }; cloud { return i }; cloud { return i }; cloud { return i } |]
            ()
    }

<@ test() @> |> r.CreateProcess
r.ShowProcessInfo()

[<Cloud>]
let test a = 
    cloud {
        return 1 / a
    }


let proc = r.CreateProcess <@ test 0 @>

r.ShowProcessInfo()

MBrace.RunLocal <| test 0


[<Cloud>]
let test () =
    cloud {
        let k = Some <| cloud { return 42 }
        return! k.Value
    }

r.Run <@ test () @>

