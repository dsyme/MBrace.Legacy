namespace Nessos.MBrace.Runtime.Tests.Store

open Nessos.MBrace.Utils
open Nessos.MBrace.Core
open Nessos.MBrace.Client
open Nessos.MBrace.Runtime.Store
open System
open System.IO
open System.Collections.Generic
open Microsoft.FSharp.Quotations
open NUnit.Framework
open FsUnit

[<TestFixture>]
[<AbstractClass>]
type ``Store tests`` () =

    abstract Store : ICloudStore
    
    member val private UsedContainers = ResizeArray<string>()
    
    abstract GetTempContainer : unit -> string
    abstract GetTempFilename  : unit -> string

    default this.GetTempContainer () =
        let rnd = Random().Next()
        let c = sprintf "testcontainer%d" rnd
        this.UsedContainers.Add(c)
        c

    default this.GetTempFilename () =
        let rnd = Random().Next()
        sprintf "testfile%d" rnd

    member this.Temp = this.GetTempContainer()
    member this.TempFile = this.GetTempFilename()


    [<Test>]
    member test.``A.0 UUID is not null or empty.`` () = 
        String.IsNullOrEmpty test.Store.UUID
        |> should equal false

    [<Test>]
    member test.``A.1 Delete container if exists.`` () = 
        if (test.Store.ContainerExists >> Async.RunSynchronously) test.Temp 
        then (test.Store.DeleteContainer >> Async.RunSynchronously) test.Temp
        |> should equal ()

    [<Test>]
    member test.``A.2 Check with GetFolders.`` () = 
        test.Store.GetAllContainers()
        |> Async.RunSynchronously
        |> Seq.exists ((=) test.Temp)
        |> should equal false

    [<Test>]
    member test.``B.0 Create a file and Check if exists.`` () = 
        let c, f = test.Temp, test.GetTempFilename()
        
        test.Store.CreateImmutable(c, f, 
            (fun stream -> async {
                let data = Array.init 100 byte
                stream.Write(data, 0, data.Length) }), asFile = false)
        |> Async.RunSynchronously 
        |> should equal ()
        
        test.Store.ContainerExists c 
        |> Async.RunSynchronously    
        |> should equal true

        test.Store.Exists(c,f) 
        |> Async.RunSynchronously    
        |> should equal true

        test.Store.GetAllContainers ()   
        |> Async.RunSynchronously    
        |> Seq.exists ((=) c)
        |> should equal true

        test.Store.GetAllFiles c
        |> Async.RunSynchronously
        |> Seq.exists ((=) f)
        |> should equal true
                                       
    [<Test>]
    member test.``B.1 Create and Read a file.`` () = 
        let data = Array.init 100 byte
        let c, f = test.Temp, test.GetTempFilename()
        test.Store.CreateImmutable(c,f, (fun s -> async { s.Write(data, 0, data.Length) }), asFile = false) 
        |> Async.RunSynchronously        
        |> should equal ()
        
        use ms = new MemoryStream()
        use s = test.Store.ReadImmutable(c,f) |> Async.RunSynchronously
        s.CopyTo(ms)
        ms.ToArray() |> should equal data

    [<Test>]
    member test.``B.2 Create and Delete a file.`` () = 
        let data = Array.init 100 byte
        let c, f = test.Temp, test.GetTempFilename()
        test.Store.CreateImmutable(c,f, (fun s -> async { s.Write(data, 0, data.Length) }), asFile = false) 
        |> Async.RunSynchronously
        |> should equal ()
        test.Store.Delete(c,f) 
        |> Async.RunSynchronously
        |> should equal ()
        
    [<Test>]
    member test.``B.3 Create and Read a larger file.`` () =
        let data = Array.init (1024 * 1024 * 4) byte
        let c, f = test.GetTempContainer(), test.GetTempFilename()
        test.Store.CreateImmutable(c,f, (fun s -> async { s.Write(data, 0, data.Length) }), asFile = false) 
        |> Async.RunSynchronously
        |> should equal ()
        
        use ms = new MemoryStream()
        use s = test.Store.ReadImmutable(c,f) |> Async.RunSynchronously
        s.CopyTo(ms)
        ms.ToArray() |> should equal data

    [<Test>]
    member test.``B.4 Create and CopyTo.`` () =
        let data = Array.init (1024 * 1024) byte
        let c, f = test.GetTempContainer(), test.GetTempFilename()
        test.Store.CreateImmutable(c,f, (fun s -> async { s.Write(data, 0, data.Length) }), asFile = false) 
        |> Async.RunSynchronously
        |> should equal ()

        use ms = new MemoryStream()
        test.Store.CopyTo(c,f,ms)
        |> Async.RunSynchronously
        ms.ToArray() |> should equal data

    [<Test>]
    member test.``B.5 CopyFrom and Read.`` () =
        let data = Array.init (1024 * 1024) byte
        use ms = new MemoryStream(data)
        let c, f = test.GetTempContainer(), test.GetTempFilename()
        test.Store.CopyFrom(c,f,ms, asFile = false)
        |> Async.RunSynchronously

        use target = new MemoryStream()
        use s = test.Store.ReadImmutable(c,f) |> Async.RunSynchronously
        s.CopyTo(target)
        target.ToArray() |> should equal data

    [<Test>]
    member test.``C.0 Create mutable and Update sequentially.`` () =
        let c, f = test.GetTempContainer(), test.GetTempFilename()
        let size = 128
        let niter = 10
        let data = Array.zeroCreate size 
        let mutable tag = test.Store.CreateMutable(c, f, fun s -> async { s.Write(data, 0, data.Length) })
                          |> Async.RunSynchronously
        for i = 1 to niter do
            let data = Array.create size (byte i)
            let ok, t = test.Store.TryUpdateMutable(c, f, (fun s -> async { s.Write(data, 0, data.Length) }), tag)
                        |> Async.RunSynchronously
            tag <- t
            ok |> should equal true
        use ms = new MemoryStream()
        use s = test.Store.ReadMutable(c,f) |> Async.RunSynchronously |> fst
        s.CopyTo(ms)
        
        ms.ToArray()
        |> should equal (Array.create size (byte niter))

    [<Test>]
    member test.``C.1 Update with precondition.`` () =
        let c, f = test.GetTempContainer(), test.GetTempFilename()
        let t' = test.Store.CreateMutable(c, f, fun s -> async { s.WriteByte 42uy })
                 |> Async.RunSynchronously
        
        let t0 = test.Store.ForceUpdateMutable(c, f, fun s -> async { s.WriteByte 0uy })
                 |> Async.RunSynchronously

        let s,t1 = test.Store.ReadMutable(c,f) |> Async.RunSynchronously
        s.Dispose()
        t1 |> should equal t0

        let ok, t2 = test.Store.TryUpdateMutable(c,f, (fun s -> async { s.WriteByte 1uy }), t')
                     |> Async.RunSynchronously
        ok |> should equal false
        t2 |> should equal t'

        let ok, t3 = test.Store.TryUpdateMutable(c,f, (fun s -> async { s.WriteByte 2uy }), t1)
                     |> Async.RunSynchronously
        ok |> should equal true

        let s', t' = test.Store.ReadMutable(c,f) |> Async.RunSynchronously
        use ms = new MemoryStream()
        s'.CopyTo(ms)

        ms.ToArray() |> should equal [| 2uy |]

        t' |> should equal t3

        s'.Dispose()

    [<Test>]
    member test.``C.2 Concurrent updates - Race.`` () =
        let c, f = test.GetTempContainer(), test.GetTempFilename()
        let t0 = test.Store.CreateMutable(c, f, fun s -> async { s.WriteByte 0uy })
                 |> Async.RunSynchronously

        let n = 20

        let oks, ts =
            [0..n-1]
            |> List.map (fun i -> 
                async { return! test.Store.TryUpdateMutable(c, f, (fun s -> async {s.WriteByte (byte i) }), t0) })
            |> Async.Parallel
            |> Async.RunSynchronously
            |> Array.unzip

        oks |> Seq.filter ((=) false) |> Seq.length |> should equal (n-1)
        oks |> Seq.filter ((=) true)  |> Seq.length |> should equal 1     // yes i know :P

        ts |> Seq.filter ((=) t0) |> Seq.length |> should equal (n-1)
        
        let i = Seq.findIndex ((=) true) oks

        use ms = new MemoryStream()
        let s, _ = test.Store.ReadMutable(c,f) |> Async.RunSynchronously
        s.CopyTo(ms)
        ms.ToArray() |> should equal [| byte i |]
        s.Dispose()

    [<Test>]
    member test.``Z.0 Cleanup`` () =
        test.UsedContainers
        |> Seq.filter (test.Store.ContainerExists >> Async.RunSynchronously)
        |> Seq.iter   (test.Store.DeleteContainer >> Async.RunSynchronously)




[<TestFixture>]
type ``FileSystem tests`` () =
    inherit ``Store tests`` ()

    let testDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString())
    let factory = new FileSystemStoreFactory() :> ICloudStoreFactory
    let store = factory.CreateStoreFromConnectionString(testDir)

    override test.Store = store

[<TestFixture>]
type ``WindowsAzure tests`` () =
    inherit ``Store tests`` ()

    let conn = "DefaultEndpointsProtocol=https;AccountName=mbraceunittests;AccountKey=QpDxFgGDhjWzw1eFiXm6nDA5F6mbmkbPHVtPYPO3kTfhKDMWF9rgVcsBBq+VnvcmH+1phURoBgCDZVX2/FfQHg=="
    let factory = new Nessos.MBrace.Azure.AzureStoreFactory() :> ICloudStoreFactory
    let store = factory.CreateStoreFromConnectionString(conn)

    override test.Store = store