namespace Nessos.MBrace.Runtime

    open Nessos.Thespian.ConcurrencyTools

    open Nessos.MBrace

    module internal StoreUtils =
        
        let inline private onError<'T> (message : string) (block : Async<'T>) =
            async {
                try 
                    return! block
                with 
                | :? NonExistentObjectStoreException
                | :? MBraceException as e ->
                    return! Async.Raise e
                | exn ->
                    return! Async.Raise <| StoreException(message, exn)
            }

        let inline onDeleteError (obj : 'U)  = 
            onError (sprintf "Error deleting %A" obj)
        let inline onDereferenceError (obj : 'U) = 
            onError (sprintf "Error reading %A" obj) 
        let inline onListError (obj : 'U)  = 
            onError (sprintf "Error listing container %A" obj) 
        let inline onCreateError (container : string) (id : string)  = 
            onError (sprintf "Error creating primitive with container : %s, id : %s" container id) 
        let inline onGetError (container : string) (id : string)  = 
            onError (sprintf "Error finding primitive with container : %s, id : %s" container id) 
        let inline onLengthError (obj : 'U) =
            onError (sprintf "Error getting length for %A" obj) 
        let inline onUpdateError (obj : 'U) =
            onError (sprintf "Error updating %A" obj) 

