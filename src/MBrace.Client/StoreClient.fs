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
    [<AutoSerializable(false)>]
    type StoreClient internal (info : StoreInfo) =

        let config = info.Primitives

        let newId () = Guid.NewGuid().ToString()

        static let registry = Atom.atom Map.empty<StoreId, StoreClient>

        static member Default 
            with get () = 
                let info = MBraceSettings.DefaultStoreInfo
                match registry.Value.TryFind info.Id with
                | Some sc -> sc
                | None ->
                    registry.Swap(fun m -> m.Add(info.Id, new StoreClient(info)))
                    registry.Value.Item info.Id

        //---------------------------------------------------------------------------------
        // CloudRef

        member this.CreateCloudRefAsync(container : string, value : 'T) : Async<ICloudRef<'T>> =
            config.CloudRefProvider.Create(container, newId(), value)
            
        member this.GetCloudRefsAsync(container : string) : Async<ICloudRef []> =
            config.CloudRefProvider.GetContainedRefs(container)

        member this.GetCloudRefAsync(container : string, id : string) : Async<ICloudRef> =
            config.CloudRefProvider.GetExisting(container, id)


        member this.CreateCloudRef(container : string,  value : 'T) : ICloudRef<'T> =
            Async.RunSynchronously <| config.CloudRefProvider.Create(container, newId(), value)

        member this.GetCloudRefs(container : string) : ICloudRef [] =
            Async.RunSynchronously <|config.CloudRefProvider.GetContainedRefs(container)

        member this.GetCloudRef(container : string, id : string) : ICloudRef =
            Async.RunSynchronously <| config.CloudRefProvider.GetExisting(container, id)
            

        //---------------------------------------------------------------------------------
        // CloudSeq

        member this.CreateCloudSeqAsync(container : string,  values : 'T seq) : Async<ICloudSeq<'T>> =
            config.CloudSeqProvider.Create(container, newId(), values)

        member this.GetCloudSeqsAsync(container : string) : Async<ICloudSeq []> =
            config.CloudSeqProvider.GetContainedSeqs(container)

        member this.GetCloudSeqAsync(container : string, id : string) : Async<ICloudSeq> =
            config.CloudSeqProvider.GetExisting(container, id)


        member this.CreateCloudSeq(container : string,  values : 'T seq) : ICloudSeq<'T> =
            Async.RunSynchronously <| config.CloudSeqProvider.Create(container, newId(), values)

        member this.GetCloudSeqs(container : string) : ICloudSeq [] =
            Async.RunSynchronously <| config.CloudSeqProvider.GetContainedSeqs(container)

        member this.GetCloudSeq(container : string, id : string) : ICloudSeq =
            Async.RunSynchronously <| config.CloudSeqProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // CloudFile

        member this.CreateCloudFileAsync(container : string, writer : Stream -> Async<unit>) : Async<ICloudFile> =
            config.CloudFileProvider.Create(container, newId(), writer)

        member this.GetCloudFilesAsync(container : string) : Async<ICloudFile []> =
            config.CloudFileProvider.GetContainedFiles(container)

        member this.GetCloudFileAsync(container : string, id : string) : Async<ICloudFile> =
            config.CloudFileProvider.GetExisting(container, id)
            

        member this.CreateCloudFile(container : string, writer : Stream -> Async<unit>) : ICloudFile =
            Async.RunSynchronously <|config.CloudFileProvider.Create(container, newId(), writer)

        member this.GetCloudFiles(container : string) : ICloudFile [] =
            Async.RunSynchronously <|config.CloudFileProvider.GetContainedFiles(container)

        member this.GetCloudFile(container : string, id : string) : ICloudFile =
            Async.RunSynchronously <|config.CloudFileProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // MutableCloudRef

        member this.CreateMutableCloudRefAsync(container : string, id : string,  value : 'T) : Async<IMutableCloudRef<'T>> =
            config.MutableCloudRefProvider.Create(container, id, value)

        member this.CreateMutableCloudRefAsync(container : string,  value : 'T) : Async<IMutableCloudRef<'T>> =
            this.CreateMutableCloudRefAsync(container, newId(), value)
            
        member this.GetMutableCloudRefsAsync(container : string) : Async<IMutableCloudRef []> =
            config.MutableCloudRefProvider.GetContainedRefs(container)

        member this.GetMutableCloudRefAsync(container : string, id : string) : Async<IMutableCloudRef> =
            config.MutableCloudRefProvider.GetExisting(container, id)
            

        member this.CreateMutableCloudRef(container : string, id : string, value : 'T) : IMutableCloudRef<'T> =
            Async.RunSynchronously <| config.MutableCloudRefProvider.Create(container, id, value)

        member this.CreateMutableCloudRef(container : string,  value : 'T) : IMutableCloudRef<'T> =
            this.CreateMutableCloudRef(container, newId(), value)

        member this.GetMutableCloudRefs(container : string) : IMutableCloudRef [] =
            Async.RunSynchronously <| config.MutableCloudRefProvider.GetContainedRefs(container)

        member this.GetMutableCloudRef(container : string, id : string) : IMutableCloudRef =
            Async.RunSynchronously <| config.MutableCloudRefProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // Misc

        member this.DeleteContainerAsync(container : string) : Async<unit> =
            info.Store.DeleteContainer(container)

        member this.DeleteContainer(container : string) : unit =
            Async.RunSynchronously <| this.DeleteContainerAsync(container)