namespace Nessos.MBrace.Runtime.Store


    open Nessos.MBrace

    module internal Utils =
        
        let inline private onError<'T> (message : string) (block : Async<'T>) =
            async {
                try 
                    return! block
                with 
                | :? NonExistentObjectStoreException as e ->
                    return! Async.FromContinuations(fun (_,ec,_) -> ec e) 
                | :? MBraceException as e ->
                    return! Async.FromContinuations(fun (_,ec,_) -> ec e) 
                | exn ->
                    return! Async.FromContinuations(fun (_,ec,_) -> ec <| StoreException(message, exn)) 
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

