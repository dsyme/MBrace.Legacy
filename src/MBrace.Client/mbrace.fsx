#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Utils.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Actors.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Base.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Store.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Client.dll"
#r "../Nessos.MBrace.Lib/bin/debug/Nessos.MBrace.Lib.dll"

open Nessos.MBrace.Client

// end of init

open Nessos.MBrace.Lib

[<Cloud>]
let hello n =
    cloud {
        if 1 / n = 0 then
            return "Hello, World!"
        else return failwith "oh no!"
    } 


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
            |> Cloud.iterf (fun img -> Script.CreateThumb img destinationFolder)
    }

let sourceDir = "/Users/eirik/Desktop/Mathematicians/"
let thumbDir = sourceDir + "Thumbs"

runtime.CreateProcess <@ createThumbnails sourceDir thumbDir @>

runtime.ShowProcessInfo ()

//
// Test init local
//

let instanceCount = 10
let runtime = MBrace.InitLocal instanceCount
runtime.Kill()


let attempts = 100
for _ in 1..attempts do
    let runtime = MBrace.InitLocal instanceCount
    runtime.Kill()

//
// TEST process state updates
//
[<Cloud>]
let expensive () = cloud {
    do! Cloud.OfAsync <| Async.Sleep 20000
    return "Done"
}

let p = runtime.CreateProcess <@ expensive () @>

p.ShowInfo()

type SomeType<'T> = SomeValue of 'T | NoneValue

[<Cloud>]
let moreExpensive () = cloud {
    do! Cloud.OfAsync <| Async.Sleep 10000
    return SomeValue "Done"
}

let p = runtime.CreateProcess <@ moreExpensive () @>

p.ShowInfo()

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
    |> fun words -> new Set<string>(words)


open System
open System.Net


let download(url : Uri) =
    async {
        let client = new WebClient()
        let! html = client.AsyncDownloadString(url)
        return html
    }
        

[<Cloud>]
let mapF (path : string) =
    cloud {
        let! text = Cloud.OfAsync <| download (Uri path)
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


let works = 
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



let proc = runtime.CreateProcess <@ mapReduce mapF reduceF [||] works @>

type T1 = { Prop1 : int }
 
[<Cloud>]
let func () = cloud {
    let x = [| {Prop1 = 42} |]
    return ()
}

let rt = MBrace.InitLocal 3

rt.Run <@ func () @>