
#load "bootstrap.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

/// Azure Store
#r "../../bin/MBrace.Azure.dll"
open Nessos.MBrace.Azure
let azureConn = System.IO.File.ReadAllText("/mbrace/azure.txt")
let azureStore = AzureStore.Create azureConn
MBraceSettings.DefaultStore <- azureStore


//----------------------------------------------
//------------------CLOUDARRAY------------------ 
//----------------------------------------------
open Nessos.MBrace.Runtime

MBraceSettings.ClientId |> ignore

#time "on"

let s = 10000
// A
let cap = StoreRegistry.DefaultStoreInfo.CloudArrayProvider
let ca1 = cap.Create("foo", Array.init s id, typeof<int>) |> Async.RunSynchronously :?> ICloudArray<int>
let ca2 = cap.Create("foo", Array.init s ((+) (s+1)), typeof<int>) |> Async.RunSynchronously :?> ICloudArray<int>
let ca3 = ca1.Append(ca2)

// B
let ca1 = CloudArray.New("foo", Array.init s id) |> MBrace.RunLocal
let ca2 = CloudArray.New("foo", Array.init s ((+) (s+1))) |> MBrace.RunLocal
let ca3 = ca1.Append(ca2)


ca1.[0L], ca2.[0L], ca3.[0L]

ca1.Length
ca2.Length
ca3.Length

ca1 |> Seq.toArray
ca2 |> Seq.toArray
ca3 |> Seq.toArray 

let ca1' = ca1.Cache()

ca1.Range(100L, 1000)
ca1'.Range(100L,1000)

CloudArrayCache.State

let ca2' = ca2.Cache()
ca2'.Range(0L,2)
ca2'.Range(10L,2)
ca2'.Range(0L,20)

CloudArrayCache.State |> Seq.iter (printfn "%A")


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
