
#load "preamble.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

let rt = MBrace.InitLocal 3

open System.IO

let g (s : Stream) = 
    async {
        use sw = new StreamWriter(s)
        sw.WriteLine("Hello")
        sw.WriteLine("world")
    }

let g1 (s : Stream) = 
    async {
        use sr = new StreamReader(s)
        return sr.ReadToEnd()
    }

let g2 (s : Stream) = 
    async {
        return seq { use sr = new StreamReader(s) in while not sr.EndOfStream do yield sr.ReadLine() }
    }


[<Cloud>]
let f = cloud {
    let! cf = CloudFile.Create(g)
    let! a = CloudFile.Read(cf, g1)
    let! b = CloudFile.ReadAsSeq(cf, g2)
    return a, b
}


let a, b = rt.Run <@ f @>

b.Count

b |> Seq.toArray
