namespace Nessos.MBrace.Client

    open System
    open System.IO

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Store

    /// Provides methods for interacting with the store and the primitives without the need for a runtime.
    type StoreClient internal (config : PrimitiveConfiguration, store : StoreInfo) =

        let newId () = Guid.NewGuid().ToString()

        static member Default 
            with get () = 
                let config = MBraceSettings.DefaultPrimitiveConfiguration
                let store  = MBraceSettings.StoreInfo
                new StoreClient(config, store)

        //---------------------------------------------------------------------------------
        // CloudRef

        member this.CreateCloudRefAsync(container : Container, value : 'T) =
            Error.handleAsync <| config.CloudRefProvider.Create(container, newId(), value)
            
        member this.GetCloudRefsAsync(container : Container) =
            Error.handleAsync <| config.CloudRefProvider.GetContainedRefs(container)

        member this.GetCloudRefAsync(container : Container, id : Id) =
            Error.handleAsync <| config.CloudRefProvider.GetExisting(container, id)


        member this.CreateCloudRef(container : Container,  value : 'T) =
            Async.RunSynchronously <| config.CloudRefProvider.Create(container, newId(), value)

        member this.GetCloudRefs(container : Container) =
            Async.RunSynchronously <| config.CloudRefProvider.GetContainedRefs(container)

        member this.GetCloudRef(container : Container, id : Id) =
            Async.RunSynchronously <|config.CloudRefProvider.GetExisting(container, id)
            

        //---------------------------------------------------------------------------------
        // CloudSeq

        member this.CreateCloudSeqAsync(container : Container,  values : 'T seq) =
            Error.handleAsync <| config.CloudSeqProvider.Create(container, newId(), values)

        member this.GetCloudSeqsAsync(container : Container) =
            Error.handleAsync <| config.CloudSeqProvider.GetContainedSeqs(container)

        member this.GetCloudSeqAsync(container : Container, id : Id) =
            Error.handleAsync <| config.CloudSeqProvider.GetExisting(container, id)


        member this.CreateCloudSeq(container : Container,  values : 'T seq) =
            Async.RunSynchronously <| config.CloudSeqProvider.Create(container, newId(), values)

        member this.GetCloudSeqs(container : Container) =
            Async.RunSynchronously <| config.CloudSeqProvider.GetContainedSeqs(container)

        member this.GetCloudSeq(container : Container, id : Id) =
            Async.RunSynchronously <| config.CloudSeqProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // CloudFile

        member this.CreateCloudFileAsync(container : Container, writer : Stream -> Async<unit>) =
            Error.handleAsync <| config.CloudFileProvider.Create(container, newId(), writer)

        member this.GetCloudFilesAsync(container : Container) =
            Error.handleAsync <| config.CloudFileProvider.GetContainedFiles(container)

        member this.GetCloudFileAsync(container : Container, id : Id) =
            Error.handleAsync <| config.CloudFileProvider.GetExisting(container, id)
            

        member this.CreateCloudFile(container : Container, writer : Stream -> Async<unit>) =
            Async.RunSynchronously <| config.CloudFileProvider.Create(container, newId(), writer)

        member this.GetCloudFiles(container : Container) =
            Async.RunSynchronously <| config.CloudFileProvider.GetContainedFiles(container)

        member this.GetCloudFile(container : Container, id : Id) =
            Async.RunSynchronously <| config.CloudFileProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // MutableCloudRef

        member this.CreateMutableCloudRefAsync(container : Container,  value : 'T) =
            Error.handleAsync <| config.MutableCloudRefProvider.Create(container, newId(), value)

        member this.GetMutableCloudRefsAsync(container : Container) =
            Error.handleAsync <| config.MutableCloudRefProvider.GetContainedRefs(container)

        member this.GetMutableCloudRefAsync(container : Container, id : Id) =
            Error.handleAsync <| config.MutableCloudRefProvider.GetExisting(container, id)
            

        member this.CreateMutableCloudRef(container : Container,  value : 'T) =
            Async.RunSynchronously <| config.MutableCloudRefProvider.Create(container, newId(), value)

        member this.GetMutableCloudRefs(container : Container) =
            Async.RunSynchronously <| config.MutableCloudRefProvider.GetContainedRefs(container)

        member this.GetMutableCloudRef(container : Container, id : Id) =
            Async.RunSynchronously <| config.MutableCloudRefProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // Misc

        member this.DeleteContainerAsync(container : string) =
            Error.handleAsync <| store.Store.DeleteContainer(container)

        member this.DeleteContainer(container : string) =
            Async.RunSynchronously <| this.DeleteContainerAsync(container)
            