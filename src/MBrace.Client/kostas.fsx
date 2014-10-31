
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



open System
open System.Collections.Generic

//type ICloudCollection<'T> = 
//    inherit ICloudDisposable
//    inherit IEnumerable<'T>
//    abstract Name : string
//    abstract Container : string
//    abstract Length : int64
//    abstract Type : Type
//    abstract Append : ICloudCollection<'T> -> ICloudCollection<'T>
//    abstract Partitions : int
//    abstract Item : index:int64 -> 'T with get
//    abstract GetPartition : index:int -> 'T []

type Partition<'T> = 
    { StartIndex : int64
      EndIndex : int64
      Payload : ICloudSeq<'T> }

type CollectionDescription<'T> = 
    { Length : int64
      Partitions : Partition<'T> [] }

[<StructuredFormatDisplay("{StructuredFormatDisplay}")>] 
type CloudCollection<'T> private (descriptor : ICloudRef<CollectionDescription<'T>>) =
    
    member private this.Descriptor = descriptor
    member private this.StructuredFormatDisplay = this.ToString()
    override this.ToString() = sprintf "cloudcollection:%s/%s" descriptor.Container descriptor.Name

    interface IEnumerable<'T> with
        member this.GetEnumerator(): Collections.IEnumerator = 
            (this :> IEnumerable<'T>).GetEnumerator() :> _
    
        member this.GetEnumerator(): IEnumerator<'T> = 
            (Seq.collect (fun p -> p.Payload) descriptor.Value.Partitions).GetEnumerator()
    
    interface ICloudDisposable with
        member x.Dispose(): Async<unit> = 
            async {
                for p in descriptor.Value.Partitions do
                    do! p.Payload.Dispose()
                do! descriptor.Dispose()
            }
        
        member x.GetObjectData(info: Runtime.Serialization.SerializationInfo, context: Runtime.Serialization.StreamingContext): unit = 
            info.AddValue("descriptor", descriptor, typeof<ICloudRef<CollectionDescription<'T>>>)

    member this.Name = descriptor.Name
    member this.Container = descriptor.Container
    member this.Length = descriptor.Value.Length
    member this.Type = typeof<'T>
    member this.Partitions = descriptor.Value.Partitions.Length
    member this.GetPartition(index : int) = 
        Seq.toArray descriptor.Value.Partitions.[index].Payload
    member this.Item 
        with get (index : int64) =
            let partitions = descriptor.Value.Partitions
            let partition = Array.find (fun p -> p.StartIndex <= index && index <= p.EndIndex) partitions
            let relativeIndex = int (index - partition.StartIndex)
            Seq.nth relativeIndex partition.Payload
    member left.Append(right : CloudCollection<'T>) =
        let lpart = left.Descriptor.Value.Partitions
        let rpart = right.Descriptor.Value.Partitions
        let part = Array.append lpart rpart
        let descriptor = { Length = left.Length + right.Length; Partitions = part }

        cloud { let! cr = CloudRef.New(descriptor) in return new CloudCollection<'T>(cr) }

    static member Create(source : seq<'T>) =
        cloud {
            let len = int64(Seq.length source)
            let! cseq = CloudSeq.New(source)
            let description = { Length = len; Partitions = [| { StartIndex = 0L; EndIndex = len-1L; Payload = cseq } |] }
            let! cr = CloudRef.New(description)
            return new CloudCollection<'T>(cr)
        }

let cc = MBrace.RunLocal(CloudCollection.Create([1..10]))


cc |> Seq.toArray

cc





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
