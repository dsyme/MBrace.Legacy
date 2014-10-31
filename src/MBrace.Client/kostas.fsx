
#load "bootstrap.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

/// Azure Store
#r "../../bin/MBrace.Azure.dll"
open Nessos.MBrace.Azure
let azureConn = System.IO.File.ReadAllText("/mbrace/azure.txt")
let azureStore = AzureStore.Create azureConn
MBraceSettings.DefaultStore <- azureStore


MBraceSettings.DefaultStore <- Nessos.MBrace.Azure.AzureStore.Create("mbracedatasets", "zVgpnYq2QmyRhKgFXetbqeBjJnQ61pAWBlij7wMp0m6znAxVzlvSFPwYGs/OY2fKjn069EECt03Ft5FO4gddeQ==")

let ca = StoreClient.Default.CreateCloudArray("tmp", [1..10])







let rt = MBrace.InitLocal 4 


open System.IO
open System.Collections
open System.Collections.Generic

type InnerEnumerator<'T>(predicate : unit -> bool, e : IEnumerator<'T>) = 
    let mutable sourceMoveNext = ref true
    member __.SourceMoveNext = sourceMoveNext.Value
    
    interface IEnumerator<'T> with
        member x.Current : 'T = e.Current
        member x.Current : obj = e.Current :> _
        
        member x.MoveNext() : bool = 
            if predicate() then 
                sourceMoveNext := e.MoveNext()
                sourceMoveNext.Value
            else false
        
        member x.Dispose() : unit = ()
        member x.Reset() : unit = invalidOp "Reset"
    
    member this.ToSeq() = 
        { new IEnumerable<'T> with
              member __.GetEnumerator() : IEnumerator = this :> _
              member __.GetEnumerator() : IEnumerator<'T> = this :> _ }

let partitionSeq (predicate : unit -> bool) (source : seq<'T>) : seq<seq<'T>> =
    let sourceEnumerator = source.GetEnumerator()
    let e = new InnerEnumerator<'T>(predicate, sourceEnumerator)
    let rec aux _ = seq { 
        if e.SourceMoveNext then
            yield e.ToSeq()
            yield! aux ()
    }
    aux ()

#r "../../bin/FsPickler.dll"

let fsp = Nessos.FsPickler.FsPickler.CreateBinary()
let path = @"c:\users\krontogiannis\desktop\temp\"

let mutable i = 0
let stream = ref <| File.OpenWrite(sprintf "%sfile%02d" path i)
let pred () = stream.Value.Length < 100L

let source = seq { 1..1000 }

for xs in partitionSeq pred source do
    printfn "count = %d" <| fsp.SerializeSequence<int>(stream.Value, xs, leaveOpen = true)
    printfn "length = %d" stream.Value.Length
    i <- i + 1
    stream.Value.Dispose()
    stream := File.OpenWrite(sprintf "%sfile%02d" path i)
stream.Value.Dispose()

for f in Directory.GetFiles path do
    use s = File.OpenRead(f)
    fsp.DeserializeSequence<int>(s)
    |> Seq.toArray
    |> printfn "seq = %A" 

#time "on"
let s = String.init 1024 (fun _ -> "0")
let xs = Array.create (512 * 1024) s


let cs = StoreClient.Default.CreateCloudArray("foobar", xs)

let mutable i = 0
let stream = ref <| File.OpenWrite(sprintf "%sfile%02d" path i)
let pred () = stream.Value.Length < (1024L * 1024L * 1024L)
for xs in partitionSeq pred xs do
    printfn "count = %d" <| fsp.SerializeSequence<string>(stream.Value, xs, leaveOpen = true)
    printfn "length = %d" stream.Value.Length
    i <- i + 1
    stream.Value.Dispose()
    stream := File.OpenWrite(sprintf "%sfile%02d" path i)
stream.Value.Dispose()


// Real: 00:02:26.167, CPU: 00:00:19.468, GC gen0: 241, gen1: 13, gen2: 1
// Real: 00:00:35.184, CPU: 00:00:08.346, GC gen0: 0, gen1: 0, gen2: 0

//----------------------------------------------
//-----------'PUSH'-BASED CLOUDLOGS------------- 
//----------------------------------------------

//let cloudLogSource =
//    lazy (
//    let reader = StoreCloudLogger.GetStreamingReader(processManager.RuntimeStore, processId)
//            
//    let observers = ResizeArray<IObserver<CloudLogEntry>>()
//            
//    let interval = 100
//    let rec pollingLoop () = async {
//        if processInfo.Value.ResultRaw = ProcessResultImage.Pending then
//            do! Async.Sleep interval
//            return! pollingLoop ()
//        else 
//            observers |> Seq.iter (fun o -> o.OnCompleted())
//    } 
//
//    reader.Updated.Add(fun(_, logs) -> 
//        logs |> Seq.iter (fun log -> observers |> Seq.iter (fun o -> o.OnNext(log))))
//
//    reader.StartAsync() |> Async.RunSynchronously
//    pollingLoop () |> Async.Start
//
//    { new IObservable<CloudLogEntry> with
//            member x.Subscribe(observer : IObserver<CloudLogEntry>) : IDisposable = 
//                    observers.Add(observer)
//                    { new IDisposable with
//                    member __.Dispose() = observers.Remove(observer) |> ignore }
//
//    } )
