namespace Nessos.MBrace.Client

    open System
    open System.IO

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Store
    open Nessos.Thespian.ConcurrencyTools

    /// Provides methods for interacting with the store and the primitives without the need for a runtime.
    [<Sealed>]
    type StoreClient internal (config : PrimitiveConfiguration, store : StoreInfo) =

        let newId () = Guid.NewGuid().ToString()

        static let registry = Atom.atom Map.empty<StoreId, StoreClient>

        static member Default 
            with get () = 
                let config = MBraceSettings.DefaultPrimitiveConfiguration
                let store  = MBraceSettings.StoreInfo
                match registry.Value.TryFind store.Id with
                | Some sc -> sc
                | None ->
                    registry.Swap(fun m -> m.Add(store.Id, new StoreClient(config, store)))
                    registry.Value.Item store.Id

        //---------------------------------------------------------------------------------
        // CloudRef

        member this.CreateCloudRefAsync(container : string, value : 'T) =
            Error.handleAsync <| config.CloudRefProvider.Create(container, newId(), value)
            
        member this.GetCloudRefsAsync(container : string) =
            Error.handleAsync <| config.CloudRefProvider.GetContainedRefs(container)

        member this.GetCloudRefAsync(container : string, id : string) =
            Error.handleAsync <| config.CloudRefProvider.GetExisting(container, id)


        member this.CreateCloudRef(container : string,  value : 'T) =
            Async.RunSynchronously <| config.CloudRefProvider.Create(container, newId(), value)

        member this.GetCloudRefs(container : string) =
            Async.RunSynchronously <| config.CloudRefProvider.GetContainedRefs(container)

        member this.GetCloudRef(container : string, id : string) =
            Async.RunSynchronously <|config.CloudRefProvider.GetExisting(container, id)
            

        //---------------------------------------------------------------------------------
        // CloudSeq

        member this.CreateCloudSeqAsync(container : string,  values : 'T seq) =
            Error.handleAsync <| config.CloudSeqProvider.Create(container, newId(), values)

        member this.GetCloudSeqsAsync(container : string) =
            Error.handleAsync <| config.CloudSeqProvider.GetContainedSeqs(container)

        member this.GetCloudSeqAsync(container : string, id : string) =
            Error.handleAsync <| config.CloudSeqProvider.GetExisting(container, id)


        member this.CreateCloudSeq(container : string,  values : 'T seq) =
            Async.RunSynchronously <| config.CloudSeqProvider.Create(container, newId(), values)

        member this.GetCloudSeqs(container : string) =
            Async.RunSynchronously <| config.CloudSeqProvider.GetContainedSeqs(container)

        member this.GetCloudSeq(container : string, id : string) =
            Async.RunSynchronously <| config.CloudSeqProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // CloudFile

        member this.CreateCloudFileAsync(container : string, writer : Stream -> Async<unit>) =
            Error.handleAsync <| config.CloudFileProvider.Create(container, newId(), writer)

        member this.GetCloudFilesAsync(container : string) =
            Error.handleAsync <| config.CloudFileProvider.GetContainedFiles(container)

        member this.GetCloudFileAsync(container : string, id : string) =
            Error.handleAsync <| config.CloudFileProvider.GetExisting(container, id)
            

        member this.CreateCloudFile(container : string, writer : Stream -> Async<unit>) =
            Async.RunSynchronously <| config.CloudFileProvider.Create(container, newId(), writer)

        member this.GetCloudFiles(container : string) =
            Async.RunSynchronously <| config.CloudFileProvider.GetContainedFiles(container)

        member this.GetCloudFile(container : string, id : string) =
            Async.RunSynchronously <| config.CloudFileProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // MutableCloudRef

        member this.CreateMutableCloudRefAsync(container : string, id : string,  value : 'T) =
            Error.handleAsync <| config.MutableCloudRefProvider.Create(container, id, value)

        member this.CreateMutableCloudRefAsync(container : string,  value : 'T) =
            this.CreateMutableCloudRefAsync(container, newId(), value)
            
        member this.GetMutableCloudRefsAsync(container : string) =
            Error.handleAsync <| config.MutableCloudRefProvider.GetContainedRefs(container)

        member this.GetMutableCloudRefAsync(container : string, id : string) =
            Error.handleAsync <| config.MutableCloudRefProvider.GetExisting(container, id)
            

        member this.CreateMutableCloudRef(container : string, id : string, value : 'T) =
            Async.RunSynchronously <| config.MutableCloudRefProvider.Create(container, id, value)

        member this.CreateMutableCloudRef(container : string,  value : 'T) =
            this.CreateMutableCloudRef(container, newId(), value)

        member this.GetMutableCloudRefs(container : string) =
            Async.RunSynchronously <| config.MutableCloudRefProvider.GetContainedRefs(container)

        member this.GetMutableCloudRef(container : string, id : string) =
            Async.RunSynchronously <| config.MutableCloudRefProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // Misc

        member this.DeleteContainerAsync(container : string) =
            Error.handleAsync <| store.Store.DeleteContainer(container)

        member this.DeleteContainer(container : string) =
            Async.RunSynchronously <| this.DeleteContainerAsync(container)
            