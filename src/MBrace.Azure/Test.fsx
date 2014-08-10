//#region Preamble
#r "../../bin/MBrace.Store.dll"
#r "../../bin/MBrace.Runtime.Base.dll"
#r "../../bin/MBrace.Azure.dll"
#r "../../bin/Microsoft.WindowsAzure.Storage.dll"
#r "../../bin/Microsoft.Data.OData.dll"
#r "../../bin/Microsoft.WindowsAzure.Configuration.dll"

open Microsoft.WindowsAzure
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
open Nessos.MBrace.Azure
open Nessos.MBrace.Store
open System.IO

let name = @""
let key  = @""
let config = sprintf "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s" name key

let serialize length payload =
    fun (stream : Stream) ->
        async { stream.Write(Array.create length payload, 0, length) }

let deserialize (stream : Stream) =
    let b = ref 0
    [| while !b <> -1 do
            b := stream.ReadByte()
            if !b <> -1 then yield byte !b |]

let large = serialize (1024 * 1024 * 1) 42uy
let small = serialize (64 * 1024 + 1) 43uy

let acc = CloudStorageAccount.Parse(config)
acc.BlobStorageUri
let store = AzureStore.Create config :> ICloudStore

store.Id

//#endregion

store.Name

//#region Immutable store
store.CreateImmutable("test", "foo3", serialize 65 0uy, false) |> Async.RunSynchronously
let o = store.ReadImmutable("test", "foo3")
        |> Async.RunSynchronously
        |> deserialize

store.Exists("test")
store.Exists("test","foo")
store.Delete("test","foo")
store.Delete("test")

let ms = let ms = new MemoryStream()
         serialize (1024 * 64 + 1) 1uy ms
         ms.Position <- 0L
         ms
store.CopyFrom("test","baz", ms)
let ms' = new MemoryStream()
store.CopyTo("test","baz", ms')
ms.ToArray() = ms'.ToArray()

store.GetFiles("test")
store.GetFolders()

store.Create("test","small", small)
store.Create("test","large", large)
store.GetFiles("test")
store.Delete("test","large")
store.Delete("test")

//#endregion

//#region mutable store

do
    let t = store.CreateMutable("test", "mutable", small)
    let s1, t1 = store.ReadMutable("test","mutable")
    let o1 = deserialize s1

    let ok, t2 = store.UpdateMutable("test", "mutable", serialize 1024 2uy, t1)
    let s2, t3 = store.ReadMutable("test","mutable")
    let o2 = deserialize s2

    let ok, t4 = store.UpdateMutable("test", "mutable", large, t3)
    let s3, t5 = store.ReadMutable("test","mutable")
    let o3 = deserialize s3

    let ok, t6 = store.UpdateMutable("test", "mutable", serialize (1 * 1024 * 1024) 3uy, t5)
    let s4, t7 = store.ReadMutable("test","mutable")
    let o4 = deserialize s4

    let ok, t8 = store.UpdateMutable("test", "mutable", serialize 42 4uy, t7)
    let s5, t9 = store.ReadMutable("test","mutable")
    let o5 = deserialize s5

    let n = "http://en.wikipedia.org/wiki/Galactic_Republic" 
    let t = store.CreateMutable("test", n, small)
    let s5, t9 = store.ReadMutable("test", n)
    let o5 = deserialize s5

    ()
do
    let t = store.CreateMutable("test", "mut", large)
    let s1, t1 = store.ReadMutable("test","mut")
    let o1 = deserialize s1

    let ok, t2 = store.UpdateMutable("test", "mut", serialize 1024 2uy, t1)
    let s2, t3 = store.ReadMutable("test","mut")
    let o2 = deserialize s2

    let t4 = store.ForceUpdateMutable("test", "mut", serialize 10 10uy)
    let s3, t4 = store.ReadMutable("test", "mut")
    let o3 = deserialize s3

    ()


do
    let t = store.CreateMutable("test", "mutable", large)
    
    let read () =
        async { let s1, t1 = store.ReadMutable("test","mutable")
                return deserialize s1 }
    let r = 
        [1..10] |> List.map (fun _ -> read ())
                |> Async.Parallel
                |> Async.RunSynchronously
    Seq.forall (fun x -> x = r.[0]) r

    let ok, t2 = store.UpdateMutable("test", "mutable", serialize 128 2uy, t)
    
    let r = 
        [1..42] |> List.map (fun _ -> read ())
                |> Async.Parallel
                |> Async.RunSynchronously
    Seq.forall (fun x -> x = r.[0]) r

    let t = store.CreateMutable("test", "counter", serialize 1 0uy)
    
    let write () =
        async { 
            let ok = ref false
            while not ! ok do
                let s, t = store.ReadMutable("test","counter")
                let v = deserialize s
                v.[0] <- v.[0] + 1uy
                let isOk, _ = store.UpdateMutable("test","counter", serialize 1 v.[0], t)
                ok := isOk
        }

    [1..8]  |> List.map (fun _ -> write ())
             |> Async.Parallel
             |> Async.Ignore
             |> Async.RunSynchronously
             |> ignore

    let s, t = store.ReadMutable("test","counter")
    deserialize s
    store.ForceUpdateMutable("test","counter", serialize 1 0uy)

    ()
//#endregion
