namespace Nessos.MBrace.Azure.Common

    open System
    open System.IO
    open Microsoft.WindowsAzure
    open Microsoft.WindowsAzure.Storage
    open Microsoft.WindowsAzure.Storage.Table

    [<AutoOpen>]
    module internal StorageExceptions =
        // http://msdn.microsoft.com/en-us/library/windowsazure/dd179357.aspx
        // http://msdn.microsoft.com/en-us/library/windowsazure/dd179438.aspx
        // http://msdn.microsoft.com/en-us/library/windowsazure/dd179439.aspx

        let (|ConditionNotMet|Other|TableNewSuccess|TableSuccess|EntityAlreadyExists|UpdateConditionNotSatisfied|) 
          (e : Exception) =
            match e with
            | :? StorageException as se ->
                let msg = se.RequestInformation.HttpStatusMessage
                match se.RequestInformation.HttpStatusCode with
                | 201 -> TableNewSuccess e
                | 204 -> TableSuccess e
                | 304 -> ConditionNotMet e
                | 409 -> EntityAlreadyExists e
                | 412 -> UpdateConditionNotSatisfied e
                | i   -> Other e
            | _ -> Other e