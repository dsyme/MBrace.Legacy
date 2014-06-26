#nowarn "0044" // 'While loop considered harmful' message.

namespace Nessos.MBrace.Runtime.Tests

    open System
    open System.IO
    open System.Threading

    open FsUnit
    open NUnit.Framework

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Client

    module ConnectionsConfig =
        open System.Xml.Linq

        let get (provider : string) = 
            let doc = XDocument.Load("connections.config")
            let X s =  XName.Get(s)
            let conn = doc.Element(X "connections").Elements()
                       |> Seq.find(fun xe -> xe.Attribute(X "id").Value = provider)
            conn.Attribute(X "connectionString").Value

    [<Category("AzureClusterTests")>]
    type ``Azure Cluster Tests``() =
        inherit ``Cluster Cloud Tests``()

        let currentRuntime : MBraceRuntime option ref = ref None
        
        override __.Name = "Azure Cluster Tests"
        override __.IsLocalTesting = false
        override __.ExecuteExpression<'T>(expr: Quotations.Expr<Cloud<'T>>): 'T =
            MBrace.RunRemote __.Runtime expr

        member __.Runtime =
            match currentRuntime.Value with
            | None -> invalidOp "No runtime specified in test fixture."
            | Some r -> r

        [<TestFixtureSetUp>]
        override test.InitRuntime() =
            lock currentRuntime (fun () ->
                match currentRuntime.Value with
                | Some runtime -> runtime.Kill()
                | None -> ()
                
                let aqn = typeof<Azure.AzureStoreFactory>.AssemblyQualifiedName
                let con = ConnectionsConfig.get "Azure"
                MBraceSettings.StoreProvider <- StoreProvider.Parse(aqn, con) //StoreProvider.Define<Azure.AzureStoreFactory>(con)
                MBraceSettings.MBracedExecutablePath <- Path.Combine(Directory.GetCurrentDirectory(), "mbraced.exe")
                let runtime = MBraceRuntime.InitLocal(3, debug = true)
                currentRuntime := Some runtime)
        
        [<TestFixtureTearDown>]
        override test.FiniRuntime() =
            lock currentRuntime (fun () -> 
                match currentRuntime.Value with
                | None -> invalidOp "No runtime specified in test fixture."
                | Some r -> r.Shutdown() ; currentRuntime := None)