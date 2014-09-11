
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

let cap = StoreRegistry.DefaultStoreInfo.CloudArrayProvider
let s = 10000
let ca1 = cap.CreateAsync("foo", Array.init s id) |> Async.RunSynchronously
let ca2 = cap.CreateAsync("foo", Array.init s ((+) (s+1))) |> Async.RunSynchronously
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

ca1.Range(0L, s)
ca1'.Range(0L, s)


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
