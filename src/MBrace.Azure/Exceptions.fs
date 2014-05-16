namespace Nessos.MBrace.Azure.Common

    open System
    open System.IO
    open Microsoft.WindowsAzure
    open Microsoft.WindowsAzure.Storage
    open Microsoft.WindowsAzure.Storage.Table

    [<AutoOpen>]
    module StorageExceptions =
        // http://msdn.microsoft.com/en-us/library/windowsazure/dd179357.aspx
        // http://msdn.microsoft.com/en-us/library/windowsazure/dd179438.aspx
        // http://msdn.microsoft.com/en-us/library/windowsazure/dd179439.aspx

        let (|ConditionNotMet|Other|TableNewSuccess|TableSuccess|EntityAlreadyExists|UpdateConditionNotSatisfied|) 
          (e : StorageException) =
            let msg = e.RequestInformation.HttpStatusMessage
            match e.RequestInformation.HttpStatusCode with
            | 201 -> TableNewSuccess e
            | 204 -> TableSuccess e
            | 304 -> ConditionNotMet e
            | 409 -> EntityAlreadyExists e
            | 412 -> UpdateConditionNotSatisfied e
            | i   -> Other e