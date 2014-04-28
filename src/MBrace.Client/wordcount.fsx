#r "bin/debug/Nessos.MBrace.Utils.dll"
#r "bin/debug/Nessos.MBrace.Actors.dll"
#r "bin/debug/Nessos.MBrace.Base.dll"
#r "bin/debug/Nessos.MBrace.Store.dll"
#r "bin/debug/Nessos.MBrace.Client.dll"

open Nessos.MBrace.Client

// end of init

#r "../Nessos.MBrace.Lib/bin/debug/Nessos.MBrace.Lib.dll"
open Nessos.MBrace.Lib


// Implementations of the mapReduce functions


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
open System.Net
open Nessos.MBrace.Utils

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
        let words = text.Split([|' '; '.'; ','; '\\'; '-'; '"'; '\''; '\t'; '\r'; '\n'|], StringSplitOptions.RemoveEmptyEntries)
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
let mapArray' (mapping : 'I -> ICloud<'R>) (inputs : 'I []) : ICloud<'R []> =
    cloud {
        let outputs = Array.zeroCreate inputs.Length
        let rec mapArray' (index: int) = //BUUUUUUUUUUUUUUUUUUUG
            cloud {
                if index = inputs.Length then return outputs
                else
                    let! output = mapping inputs.[index]
                    Array.set outputs index output

                    return! mapArray' (index + 1)
            }

        return! mapArray' 0
    }

[<Cloud>]
let mapArray (mapping : 'I -> ICloud<'R>) (inputs : 'I []) : ICloud<'R []> =
    cloud {
        let outputs = Array.zeroCreate inputs.Length
        
        for index in [| 0..(inputs.Length - 1) |] do
            let! output = mapping inputs.[index]
            Array.set outputs index output

        return outputs
    }


[<Cloud>]
let mapArrayF (paths: string []) =
    cloud {
        let! maps = mapArray mapF paths
        return Array.concat maps
    }

[<Cloud>]
let mapFfromFiles (path : string) =
    cloud {
        let text = File.ReadAllText (path)
        let words = text.Split([|' '; '.'; ','; '\\'; '-'; '"'; '\''; '\t'; '\r'; '\n'|], StringSplitOptions.RemoveEmptyEntries)
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

let links = [
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/allswellthatendswell.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/amsnd.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/antandcleo.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/asyoulikeit.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/comedyoferrors.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/cymbeline.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/hamlet.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/henryiv1.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/henryiv2.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/henryv.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/henryvi1.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/henryvi2.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/henryvi3.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/henryviii.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/juliuscaesar.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/kingjohn.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/kinglear.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/loveslobourslost.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/maan.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/macbeth.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/measureformeasure.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/merchantofvenice.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/othello.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/richardii.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/richardiii.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/romeoandjuliet.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/tamingoftheshrew.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/tempest.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/themwofw.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/thetgofv.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/timon.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/titus.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/troilusandcressida.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/twelfthnight.txt"
    "http://www.cs.ukzn.ac.za/~hughm/ap/data/shakespeare/winterstale.txt" ]

let files = Directory.GetFiles(@"C:\Users\anirothan\Documents\Visual Studio 2010\Projects\FsharpInTheCloud\Hadoop.WordCount\data")
            |> Array.toList

let runtime = MBrace.InitLocal 6
let p1 = runtime.CreateProcess <@ MapReduce.mapReduce mapF reduceF [||] links @>

let p2 = runtime.CreateProcess <@ MapReduce.mapReduce mapFfromFiles reduceF [||] files @>

let p3 = runtime.CreateProcess <@ MapReduce.mapReduceArray mapArrayF reduceF (fun () -> cloud { return [||] }) (links |> List.toArray) 4 @>

let proc = runtime.CreateProcess <@ cloud { 
    while true do
        printfn "running"
        ()
} @>

runtime.Reboot()

proc.ShowInfo()

runtime.KillProcess(proc.ProcessId)

//13