
#load "bootstrap.fsx"
open Nessos.MBrace
open Nessos.MBrace.Client

#r "../../bin/MBrace.Azure.dll"
open Nessos.MBrace.Azure

let azureConn = System.IO.File.ReadAllText("/mbrace/azure.txt")
let azureStore = AzureStore.Create azureConn

MBraceSettings.DefaultStore <- azureStore

//let nodes = [1..3] 
//            |> List.map (fun n -> sprintf "mbrace://10.0.1.%d:2675" (3+n)) 
//            |> List.map (fun n -> MBraceNode(n))
//
//nodes |> List.map (fun n -> n.Ping())
//nodes |> List.iter (fun n -> n.ShowSystemLogs())





let cloudLogSource =
    lazy (
    let reader = StoreCloudLogger.GetStreamingReader(processManager.RuntimeStore, processId)
            
    let observers = ResizeArray<IObserver<CloudLogEntry>>()
            
    let interval = 100
    let rec pollingLoop () = async {
        if processInfo.Value.ResultRaw = ProcessResultImage.Pending then
            do! Async.Sleep interval
            return! pollingLoop ()
        else 
            observers |> Seq.iter (fun o -> o.OnCompleted())
    } 

    reader.Updated.Add(fun(_, logs) -> 
        logs |> Seq.iter (fun log -> observers |> Seq.iter (fun o -> o.OnNext(log))))

    reader.StartAsync() |> Async.RunSynchronously
    pollingLoop () |> Async.Start

    { new IObservable<CloudLogEntry> with
            member x.Subscribe(observer : IObserver<CloudLogEntry>) : IDisposable = 
                    observers.Add(observer)
                    { new IDisposable with
                    member __.Dispose() = observers.Remove(observer) |> ignore }

    } )
