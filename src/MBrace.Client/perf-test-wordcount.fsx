#I @"../Installer/bin/Debug"
#r "Nessos.MBrace.Utils.dll"
#r "Nessos.MBrace.Actors.dll"
#r "Nessos.MBrace.Base.dll"
#r "Nessos.MBrace.Store.dll"
#r "Nessos.MBrace.Client.dll"
#r "Nessos.MBrace.Core.dll"
#r "Nessos.MBrace.Serialization.dll"

open Nessos.MBrace.Client

// end of init

open System
open System.IO
open System.Collections.Generic

// Helper functions
let rec partition size (input : 'T []) =
    let q, r = input.Length / size , input.Length % size
    [|  for i in 0 .. q-1 do
            yield input.[ i * size .. (i + 1) * size - 1]
        if r > 0 then yield input.[q * size .. ] 
    |]

let uploadOne _ = 
    cloud {
        let path = @"c:\input.txt"
        let container = "mbracetest"
        return! CloudSeq.New (container, File.ReadLines(path)) 
    } 
    //|> MBrace.RunLocal :> _ seq
  
//let uploadData (count : int) = cloud {
//        
//        [|1..count|]
//        |> partition npar
//        |> Array.map (fun n -> n |> Array.Parallel.map (uploadOne))
//        |> Array.concat
//    }

// MAP - REDUCE

let map (file : seq<string>)  =
    file
    |> Seq.collect (fun line -> line.Split([|' '; '\n'; '\r'; '\t'; ','; ';'; '.'|], StringSplitOptions.RemoveEmptyEntries))
    |> Seq.filter (fun word -> word.Length >= 3)
    |> Seq.map (fun word -> word, 1L)

let reduce (kvs : seq<string * int64>) = 
    let dict = Dictionary<_,_>()
    Seq.iter (fun (k,v) -> if dict.ContainsKey k 
                            then dict.[k] <- dict.[k] + v
                            else dict.[k] <- v) kvs
    seq { for KeyValue kv in dict do yield kv }

[<Cloud>]
let mapReduce (files : seq<string> []) = 
    cloud {
        let results = files |> Seq.collect (map)
                            |> reduce
        return! CloudSeq.New results
    }

[<Cloud>]
let wordcount (files : seq<string> []) = 
    cloud {
        let! workers = Cloud.GetWorkerCount()
        let splits = files |> partition (files.Length / workers)
        let! results = splits |> Array.map mapReduce
                              |> Cloud.Parallel
        return! results |> Seq.concat
                        |> reduce
                        |> Seq.toArray
                        |> Array.sortBy snd
                        |> CloudSeq.New
    }


let rt = MBrace.InitLocal 4

let files = uploadData 10

rt.Run <@ wordcount files @>


wordcount files |> MBrace.RunLocal








///////////////////////



open System
open System.IO
open System.Collections.Generic

type File = seq<string>
type Pair = string * int64

// Helper functions
let rec partition size (input : 'T []) =
    let q, r = input.Length / size , input.Length % size
    [|  for i in 0 .. q-1 do
            yield input.[ i * size .. (i + 1) * size - 1]
        if r > 0 then yield input.[q * size .. ] 
    |]

let uploadOne _ = 
    cloud {
        let path = @"c:\input.txt"
        let container = "mbracetest"
        return! CloudSeq.New (container, File.ReadLines(path)) 
    } |> MBrace.RunLocal :> _ seq
  
let uploadData (count : int) =
        let npar = 4

        [|1..count|]
        |> partition npar
        |> Array.map (fun n -> n |> Array.Parallel.map (uploadOne))
        |> Array.concat

// MAP - REDUCE

let map (file : seq<string>)  =
    file
    |> Seq.collect (fun line -> line.Split([|' '; '\n'; '\r'; '\t'; ','; ';'; '.'|], StringSplitOptions.RemoveEmptyEntries))
    |> Seq.filter (fun word -> word.Length >= 3)
    |> Seq.map (fun word -> word, 1L)

let reduce (kvs : seq<string * int64>) = 
    let dict = Dictionary<_,_>()
    Seq.iter (fun (k,v) -> if dict.ContainsKey k 
                            then dict.[k] <- dict.[k] + v
                            else dict.[k] <- v) kvs
    seq { for KeyValue kv in dict do yield kv }

[<Cloud>]
let mapReduce (files : seq<string> []) = 
    cloud {
        let results = files |> Seq.collect (map)
                            |> reduce
        return! CloudSeq.New results
    }

[<Cloud>]
let wordcount (files : seq<string> []) = 
    cloud {
        let! workers = Cloud.GetWorkerCount()
        let splits = files |> partition (files.Length / workers)
        let! results = splits |> Array.map mapReduce
                              |> Cloud.Parallel
        return! results |> Seq.concat
                        |> reduce
                        |> Seq.toArray
                        |> Array.sortBy snd
                        |> CloudSeq.New
    }


let rt = MBrace.InitLocal 4

let files = uploadData 10

rt.Run <@ wordcount files @>


wordcount files |> MBrace.RunLocal
