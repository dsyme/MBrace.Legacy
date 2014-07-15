namespace Nessos.MBrace.Azure.Tests

    open NUnit.Framework

    open Nessos.MBrace.Store
    open Nessos.MBrace.Store.Tests
    open System.IO
    open System.Reflection

    [<TestFixture; Category("CustomStores")>]
    type ``WindowsAzure tests`` () =
        inherit ``Store tests`` ()

        
        let conn = File.ReadAllText(Path.Combine(__SOURCE_DIRECTORY__, "../../temp/azure.txt"))
        let factory = new Nessos.MBrace.Azure.AzureStoreFactory() :> ICloudStoreFactory
        let store = factory.CreateStoreFromConnectionString(conn)

        override test.Store = store