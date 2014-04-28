#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Utils.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Actors.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Base.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Store.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Client.dll"

open Nessos.MBrace.Client

// end of initF


// An implementation of the mapReduce function (see wordcount example).
// Uses somewhat dynamic parallelism. It spawns a number of cloud computations proportional to the
// number of the workers, and then creates some async computations (proportional to the number of
// hardware threads on a machine). Finally the execution is sequential.

#r "../Nessos.MBrace.Lib/bin/debug/Nessos.MBrace.Lib.dll"
open Nessos.MBrace.Lib.MapReduce


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
    |> fun words -> new Set<string>(words)


open System
open System.IO
open Nessos.MBrace.Utils

[<Cloud>]
let mapCloudTree (paths : string []) =
    cloud {
        let texts = paths |> Array.map (fun path -> File.ReadAllText (path))
        return! newRef <| Leaf texts
    }
[<Cloud>]
let reduceCloudTree (left : ICloudRef<CloudTree<'T>>) (right : ICloudRef<CloudTree<'T>>) =
    cloud {
        return! newRef <| Branch (left, right)
    }


let mapF (texts : string[]) =
        let words = texts |> Array.map (fun text -> text.Split([|' '; '.'; ','|], StringSplitOptions.RemoveEmptyEntries)) |> Seq.concat
        words
        |> Seq.map (fun word -> word.ToLower())
        |> Seq.map (fun t -> t.Trim())
        |> Seq.filter (fun word -> Seq.length word > 3 && not <| noiseWords.Contains(word) )
        |> Seq.groupBy id
        |> Seq.map (fun (key, values) -> (key, values |> Seq.length))
        |> Seq.toArray
    


let reduceF (left: (string * int) []) (right: (string * int) []) = 
    Seq.append left right 
    |> Seq.groupBy fst 
    |> Seq.map (fun (key, value) -> (key, value |> Seq.sumBy snd ))
    |> Seq.toArray
    

[<Cloud>]
let mapF' (path : string) =
    cloud {
        let text = File.ReadAllText (path)
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
let reduceF' (left: (string * int) []) (right: (string * int) []) = 
    cloud {
        return 
            Seq.append left right 
            |> Seq.groupBy fst 
            |> Seq.map (fun (key, value) -> (key, value |> Seq.sumBy snd ))
            |> Seq.toArray
    }



#time

let runtime = MBrace.InitLocal 3

type Foo<'T> = Foo of ((unit -> 'T) -> ICloud<'T>)
    
[<Cloud>]
let apply foo initF = 
    let (Foo f) = foo
    f initF

runtime.Run <@ apply (Foo (fun initF  -> cloud { return initF() })) (fun () -> 42)  @>

let proc = runtime.CreateProcess <@ cloud { return 42 } @>

let files = System.IO.Directory.GetFiles(@"c:\\wiki\\") |> Seq.take 10 |> Seq.toArray
//files |> Array.Parallel.map (fun file -> System.IO.File.ReadAllText(file).Length) 
let p = runtime.CreateProcess <@ mapReduce mapF' reduceF' [||] (files |> Array.toList) @>

let result = mapReduceArray mapCloudTree reduceCloudTree (fun () -> cloud { return! newRef Empty }) files 100 |> Cloud.RunLocal
//mapReduceCloudTree (Cloud.lift mapF) (Cloud.lift2 reduceF) undefined result |> Cloud.RunLocal
mapReduceCloudTree (Cloud.lift mapF) (Cloud.lift2 reduceF) undefined result |> Cloud.RunLocal

let proc = runtime.CreateProcess <@ mapReduceArray mapCloudTree reduceCloudTree (fun () -> cloud { return! newRef Empty }) files 1 @>
let result' = proc.AwaitResult()
let proc' = runtime.CreateProcess <@ mapReduceCloudTree (Cloud.lift mapF) (Cloud.lift2 reduceF) undefined result' @>

[<Cloud>]
let testExc () = cloud { return raise <| new System.Exception("42") }

[<Cloud>]
let hello () = 
    cloud {
        let r = [|1..1000000|]
        for i in [|1..10|] do
            let! _ = cloud { return Array.length r } <||> cloud { return Array.length r }
            ()
    } 

runtime.Run <@ hello () @>
runtime.ShowProcessInfo()
runtime.ShowUserLogs(2680)

[<Cloud>]
let test i = 
    cloud {
        let r = ref 1
        while r.Value < i do
            r := r.Value + 1
        ()
    }
runtime.Run <@ cloud { return! test 30 } @>

runtime.ShowProcessInfo()

[<Cloud>]
let rec bin (i : int) : ICloud<unit> = cloud {
    if i = 0 then 
        do! Cloud.Log "base"
        return ()
    else
        do! Cloud.Logf "Depth %A" i
        let! _ = bin (i - 1) <||> bin (i - 1) in return ()
  }

[<Cloud>]
let rec loop () : ICloud<unit> = 
    cloud {
        return! loop()
    }

loop () |> MBrace.RunLocal

runtime.CreateProcess <@ loop () @>

for i  = 1 to 100 do
    runtime.CreateProcess <@ bin 4 @> |> ignore


runtime.ShowProcessInfo()
runtime.ShowUserLogs(proc.ProcessId)
runtime.Reboot()
runtime.Kill()

testAdd () |> MBrace.RunLocal

[<Cloud>]
let rec iter n =
    cloud {
        if n = 0 then return n
        else
            return! iter (n - 1)
    }

#time
runtime.Run <@ iter 1000000 @>
iter 1000000 |> Cloud.RunLocal
cloud { return! newRef [|1..100000000|] } |> Cloud.RunLocal

let proc = runtime.CreateProcess <@ cloud { return! [|1..10000000|] |> Array.map (fun i -> string i) |> newRef } @>


runtime.ShowProcessInfo()



runtime.Run <@ cloud { return 1 + 1 } @>


let r : int = runtime.Run <|
    <@ cloud { 
            try
                return raise <| System.InvalidOperationException() 
            with :? System.InvalidOperationException -> return -1
                 | _ -> return -2
       } 
    @>

runtime.Reboot()

let proc = runtime.CreateProcess <@ cloud { 
                                        while true do
                                            do! Cloud.OfAsync <| Async.Sleep 1000
                                            do! Cloud.Log <| System.DateTime.Now.ToString()
                                            ()
                                    } @>




runtime.ShowUserLogs 8166
runtime.KillProcess 8166

runtime.ShowProcessInfo()

runtime.CreateProcess(<@ cloud { return "hello" } @>, "test")


let proc' = runtime.CreateProcess <@ cloud { let! x = Cloud.Choice [|cloud { let! _ = Cloud.OfAsync <| Async.Sleep(10000) in return Some 1 }; cloud { let! _ = Cloud.OfAsync <| Async.Sleep(100000) in return Some 2 } |] in return x } @>

runtime.KillProcess proc.ProcessId

runtime.ShowProcessInfo()

runtime.CreateProcess <@ cloud { 
                            let! _ = [| for i in [|1..4|] do 
                                                yield cloud {  
                                                        while true do
                                                            ()
                                                    } |] |> Cloud.Parallel
                            return ()
                        } @>


[<Cloud>]
let f a b = cloud { return a + b }

runtime.Run <@ f 1 2 @>

runtime.ShowProcessInfo()


let proc = runtime.CreateProcess <@ cloud { return! [1..10000] |> Cloud.map newRef } @>

runtime.ShowProcessInfo()


runtime.Run <@ cloud { return! newRef 42 } @>

let test1 a = 
    1 / a

let test0 a = 
    test1 a

[<Cloud>]
let testCloud a = 
    cloud { 
        let! y = cloud { return a } 
        return test0 (y + a) 
    }

let proc = runtime.CreateProcess <@ testCloud 0 @>

runtime.Run <@ testCloud 0 @>

[<Cloud>]
let f a = 
    cloud {
        return a + 1
    }
<@ f 2 |> Cloud.Trace @> |> runtime.Run



runtime.ShowUserLogs()
runtime.ShowLogs()
runtime.ShowProcessInfo()
runtime.ShowInfo(true)

[<Cloud>]
let f = 
    cloud {
        let data = [|1..10000000|]
        for i in [|1..1000|] do
            let! _ = CloudRef.New data 
            ()
    }

runtime.CreateProcess <@ f @>

runtime.ShowProcessInfo()


let closure = runtime.Run <@ cloud { let y = 2 in return fun x -> x + y } |> Cloud.Trace @>



[<Cloud>]
let testParallelTrace a b =
    cloud { 
        let! (x, y) = cloud { return (a, b) }
        return x + y + a + b
    } |> Cloud.Trace

runtime.Run <@ testParallelTrace 1 2 @>

runtime.ShowProcessInfo()
runtime.ShowUserLogs(pid = 5978)


[<Cloud>]
let testLog () =
    cloud {
        //let! data = newRef [|1..10000000|]
        //let! data = CloudSeq.New [|1..10000000|]
        let data = [|1..10000000|]
        let! _ = Cloud.GetWorkerCount()
        data.[0] <- 42
        let! _ = Cloud.GetWorkerCount()
        return data.[0]
    } 

<@ testLog () |> Cloud.Trace @> |> runtime.CreateProcess

runtime.ShowProcessInfo()

runtime.ShowUserLogs(3525)

runtime.Reboot()



[<Cloud>]
let test () = 
    cloud {
        let! data = CloudRef.New [|1..1000000|]
        for i in [|1..1000|] do
            let! _ = cloud { return data.Value.Length } <||> cloud { return data.Value.Length }
            ()
    }

<@ test() @> |> runtime.Run 


let r = ref Unchecked.defaultof<unit -> unit>
async {
    let! _ = Async.FromContinuations(fun (cont, _, _) -> r := cont)
    printfn "test"
    let! _ = Async.Sleep(System.Int32.MaxValue)
    return ()
} |> Async.Start

r.Value ()


#load @"C:\Users\developer001\Desktop\public-master\Excel\Charting\ExcelEnv.fsx"
#load @"C:\Users\developer001\Desktop\public-master\Excel\Charting\StocksScript.fsx"


open StockScript
open System
open System.Net
open Microsoft.Office.Interop.Excel


let now = System.DateTime.Now

let chart = Excel.NewChart().Value

let msft = read ("MSFT", now.AddMonths(-6), now)
chart |> addStockHistory msft
chart |> addMovingAverages msft

chart |> clear 

for stock in ["AAPL";"MSFT";"GOOG"] do 
    chart |> addStockHistory (read (stock, now.AddMonths(-6), now))
    chart |> addMovingAverages (read (stock, now.AddMonths(-6), now))





open System.Linq.Expressions

#r "FSharp.PowerPack.Linq.dll"

open Microsoft.FSharp.Linq.QuotationEvaluation

#time
<@ [1..10] |> List.map (fun x -> x + 1) @>




module Reducer =
    open System 
    open System.Text
    open System.Collections.Generic
    open System.Linq

    type Reducer<'T, 'R> = (('T -> 'R -> 'R) -> ('R -> 'R -> 'R) -> (unit -> 'R) -> ICloud<'R>)
    
    

    [<Cloud>]
    let rec reduceCombine seqReduceCount (values : 'T []) reduceF combineF initF s e =
        cloud { 
            if e - s <= seqReduceCount then
                let s' = if s > 0 then s + 1 else s
                let result = 
                    let mutable r = initF()
                    for i = s' to e do
                        r <- reduceF values.[i] r
                    r
                return result
            else 
                let m = (s + e) / 2
                let! result =  Cloud.Parallel [| reduceCombine seqReduceCount values reduceF combineF initF s m; reduceCombine seqReduceCount values reduceF combineF initF m e |]
                return combineF result.[0] result.[1]
        }

    [<Cloud>]
    let toParallelReducer (seqReduceCount : int) (values : 'T []) : Reducer<'T, 'R> =
        (fun reduceF combineF initF ->
            let rec reduceCombine s e =
                cloud { 
                    if e - s <= seqReduceCount then
                        let s' = if s > 0 then s + 1 else s
                        let result = 
                            let mutable r = initF()
                            for i = s' to e do
                                r <- reduceF values.[i] r
                            r
                        return result
                    else 
                        let m = (s + e) / 2
                        let! result =  Cloud.Parallel [| reduceCombine s m; reduceCombine m e |]
                        return combineF result.[0] result.[1]
                }
            reduceCombine 0 (values.Length - 1)) 
        

    // transform functions
    [<Cloud>]
    let collect (f : 'A -> seq<'B>) (input : Reducer<'A, 'R>) : Reducer<'B, 'R> = 
        (fun reduceF combineF initF ->
                                 input (fun a r ->                     
                                                let mutable r' = r
                                                for value in f a do
                                                    r' <- reduceF value r'
                                                r') combineF initF)
                    
    [<Cloud>]
    let map (f : 'A -> 'B) (input : Reducer<'A, 'R>) : Reducer<'B, 'R> =
        (fun reduceF combineF initF -> 
                    input (fun a r -> reduceF (f a) r) combineF initF)
        
    [<Cloud>]
    let filter (p : 'A -> bool) (input : Reducer<'A, 'R>) : Reducer<'A, 'R> =
        (fun reduceF combineF initF ->
                    input (fun a r -> if p a then reduceF a r else r) combineF initF)
        

    // reduce functions
    [<Cloud>]
    let reduce (reducef : 'T -> 'R -> 'R) (combineF : 'R -> 'R -> 'R) (initF : (unit -> 'R)) (reducer : Reducer<'T, 'R>) : ICloud<'R> = 
        reducer reducef combineF initF

    let sum (reducer : Reducer<int, int>) : ICloud<int> = 
        reduce (+) (+) (fun () -> 0) reducer

    let length (reducer : Reducer<'T, int>) : ICloud<int> =
        reduce (fun _ r -> r + 1) (+) (fun () -> 0) reducer

    [<Cloud>]
    let inline toArray (reducer : Reducer<'T, _>) : ICloud<'T []> =
        let reduceCloud = 
            reduce (fun v (list : List<'T>) -> list.Add(v); list)
                    (fun (left : List<'T>) (right : List<'T>) -> left.AddRange(right); left) 
                    (fun () -> new List<'T>())
                    reducer 
        cloud { let! result = reduceCloud in return result.ToArray() }
//
//    let inline groupBy (selectorF : 'T -> 'Key) 
//                        (transformF : 'T -> 'Elem) 
//                        (aggregateF : 'Key * seq<'Elem> -> 'Elem) 
//                        (reducer : Reducer<'T>) : ICloud<seq<'Key * 'Elem>> =
//        let inline reduceF (v : 'T) (r : Dictionary<'Key, List<'Elem>>) =
//            let key = selectorF v
//            let elem = transformF v
//            if r.ContainsKey(key) then
//                r.[key].Add(elem)
//            else 
//                r.Add(key, new List<_>([| elem |]))
//            r
//        let inline combineF (left : Dictionary<'Key, List<'Elem>>) (right : Dictionary<'Key, List<'Elem>>) =
//            for keyValue in right do
//                if left.ContainsKey(keyValue.Key) then
//                    left.[keyValue.Key].AddRange(right.[keyValue.Key])
//                    let result = (keyValue.Key, left.[keyValue.Key]) |> aggregateF
//                    left.[keyValue.Key].Clear()
//                    left.[keyValue.Key].Add(result)
//                else
//                    left.[keyValue.Key] <- new List<_>([| (keyValue.Key, keyValue.Value) |> aggregateF |])
//            left
//                    
//        let reduceCloud = 
//            reduce  reduceF combineF
//                    (fun () -> new Dictionary<'Key, List<'Elem>>())
//                    reducer
//        cloud { let! result = reduceCloud in return result |> Seq.map (fun keyValue -> (keyValue.Key, (keyValue.Key, keyValue.Value) |> aggregateF)) }

    [<Cloud>]
    let countBy (selectorF : 'T -> 'Key) (reducer : Reducer<'T, _>) : ICloud<seq<'Key * int>> =
        let reduceF (v : 'T) (r : Dictionary<'Key, int>) =
            let key = selectorF v
            if r.ContainsKey(key) then
                r.[key] <- r.[key] + 1
            else 
                r.[key] <- 1
            r
        let combineF (left : Dictionary<'Key, int>) (right : Dictionary<'Key, int>) =
            for keyValue in right do
                if left.ContainsKey(keyValue.Key) then
                    left.[keyValue.Key] <- left.[keyValue.Key] + right.[keyValue.Key]
                else
                    left.[keyValue.Key] <- keyValue.Value
            left
                    
        let reduceCloud = 
            reduce  reduceF combineF
                    (fun () -> new Dictionary<'Key, int>())
                    reducer
        cloud { let! result = reduceCloud in return result |> Seq.map (fun keyValue -> (keyValue.Key, keyValue.Value)) }
        

let rt = MBrace.InitLocal 4

[<Cloud>]
let test () = 
    [|"1 2 1 3 3"; "1 2 1 3 3"|]
    |> Reducer.toParallelReducer 20
    |> Reducer.collect (fun line -> line.Split(' ') :> _)
    |> Reducer.countBy id 



rt.Run <@ test () @>


let orig = System.IO.File.ReadAllText("c:\\orig.txt")
let radio = System.IO.File.ReadAllText("c:\\sfera.txt")

let orig = System.IO.File.ReadAllText("c:\\skyfall.txt")
let radio = System.IO.File.ReadAllText("c:\\kiss.txt")


{0 .. radio.Length - 3 } 
|> Seq.filter (fun i -> orig.Contains(radio.Substring(i, 3)))
|> Seq.length


open System.Threading

[<Cloud>]
let test = 
    cloud {
        let! t1 = Cloud.GetTaskId()
        let! t2, t3 = cloud { return! Cloud.GetTaskId() } <||> cloud { return! Cloud.GetTaskId() } // Parallel fork-join
        let! t4 = Cloud.GetTaskId()
        return (t1, t2, t3, t4)
    } 

runtime.Run <@ test @>





