namespace Nessos.MBrace.Azure.Tests

    open NUnit.Framework

    open Nessos.MBrace.Store
    open Nessos.MBrace.Store.Tests

    [<TestFixture; Category("CustomStores")>]
    type ``WindowsAzure tests`` () =
        inherit ``Store tests`` ()

        let conn = ConnectionsConfig.get "Azure" 
        let factory = new Nessos.MBrace.Azure.AzureStoreFactory() :> ICloudStoreFactory
        let store = factory.CreateStoreFromConnectionString(conn)

        override test.Store = store