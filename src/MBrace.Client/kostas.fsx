
#load "preamble.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

let rt = MBrace.InitLocal 3

let mref = StoreClient.Default.CreateMutableCloudRef("foo", 42)
let xs = 
    [1..100]
    |> List.map (fun i -> mref.TryUpdate(i))
    |> Async.Parallel
    |> Async.RunSynchronously
    
xs |> Seq.filter id |> Seq.length
mref.Value



let f = cloud {
    let! mref = MutableCloudRef.New(42)
    return! [1..100]
            |> List.map (fun i -> MutableCloudRef.Set(mref, i))
            |> Cloud.Parallel
}

let ys = MBrace.RunLocal f
ys |> Seq.filter id |> Seq.length
