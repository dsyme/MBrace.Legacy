namespace Nessos.MBrace.Azure.Tests

    open System.IO

    open NUnit.Framework

    open Nessos.MBrace.Store
    open Nessos.MBrace.Store.Tests
    open Nessos.MBrace.Azure    

    [<TestFixture; Category("CustomStores")>]
    type ``WindowsAzure tests`` () =
        inherit ``Store tests`` ()

        
        let conn = File.ReadAllText(Path.Combine(__SOURCE_DIRECTORY__, "../../temp/azure.txt"))
        let store = AzureStore.Create conn :> ICloudStore

        override test.Store = store