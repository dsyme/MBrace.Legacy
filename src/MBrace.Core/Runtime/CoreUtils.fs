namespace Nessos.MBrace.Core
    
    open Nessos.Thespian

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Store.Registry

    [<AutoOpen>]
    module Utils =

        let nodeUsesCompatibleStore (node : NodeRef) =
            try (node <!= GetStoreId) = StoreRegistry.DefaultStore.Id
            with _ -> false

        let runtimeUsesCompatibleStore (runtime : ActorRef<ClientRuntimeProxy>) =
            try runtime <!= (RemoteMsg << GetStoreId) = StoreRegistry.DefaultStore.Id
            with _ -> false
