namespace Nessos.MBrace.Client

    open System
    open System.IO

    open Nessos.MBrace
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime
    open Nessos.Thespian.ConcurrencyTools

    /// Provides methods for interacting with the store and the primitives without the need for a runtime.
    [<Sealed; AutoSerializable(false)>]
    type StoreClient internal (info : StoreInfo) =

        let newId () = Guid.NewGuid().ToString()

        static let registry = Atom.atom Map.empty<StoreId, StoreClient>

        member __.StoreDefinition = info.Definition

        /// Gets the default StoreClient that corresponds to the current StoreProvider.
        static member Default 
            with get () = 
                let info = StoreRegistry.DefaultStoreInfo
                match registry.Value.TryFind info.ActivationInfo.Id with
                | Some sc -> sc
                | None ->
                    registry.Swap(fun m -> m.Add(info.ActivationInfo.Id, new StoreClient(info)))
                    registry.Value.Item info.ActivationInfo.Id

        //---------------------------------------------------------------------------------
        // CloudRef

        /// <summary>
        /// Creates a new CloudRef in the specified container.
        /// </summary>
        /// <param name="container">The folder's name.</param>
        /// <param name="value">The CloudRef's value.</param>
        member this.CreateCloudRefAsync(container : string, value : 'T) : Async<ICloudRef<'T>> =
            info.CloudRefProvider.Create(container, newId(), value)
            
        /// <summary>
        /// Returns an array of the CloudRefs contained in the specified folder.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        member this.GetCloudRefsAsync(container : string) : Async<ICloudRef []> =
            info.CloudRefProvider.GetContainedRefs(container)

        /// <summary>
        /// Returns a CloudRef that already exists in the specified contained with the given identifier.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        /// <param name="id">The CloudRef's id.</param>
        member this.GetCloudRefAsync(container : string, id : string) : Async<ICloudRef> =
            info.CloudRefProvider.GetExisting(container, id)

        /// <summary>
        /// Creates a new CloudRef in the specified container.
        /// </summary>
        /// <param name="container">The folder's name.</param>
        /// <param name="value">The CloudRef's value.</param>
        member this.CreateCloudRef(container : string,  value : 'T) : ICloudRef<'T> =
            Async.RunSynchronously <| info.CloudRefProvider.Create(container, newId(), value)
        
        /// <summary>
        /// Returns an array of the CloudRefs contained in the specified folder.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        member this.GetCloudRefs(container : string) : ICloudRef [] =
            Async.RunSynchronously <|info.CloudRefProvider.GetContainedRefs(container)

        /// <summary>
        /// Returns a CloudRef that already exists in the specified contained with the given identifier.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        /// <param name="id">The CloudRef's id.</param>
        member this.GetCloudRef(container : string, id : string) : ICloudRef =
            Async.RunSynchronously <| info.CloudRefProvider.GetExisting(container, id)
            

        //---------------------------------------------------------------------------------
        // CloudSeq

        /// <summary>
        /// Creates a new CloudSeq in the specified container.
        /// </summary>
        /// <param name="container">The folder's name.</param>
        /// <param name="values">The source sequence.</param>
        member this.CreateCloudSeqAsync(container : string,  values : 'T seq) : Async<ICloudSeq<'T>> =
            info.CloudSeqProvider.Create(container, newId(), values)

        /// <summary>
        /// Returns an array of the CloudSeqs contained in the specified folder.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        member this.GetCloudSeqsAsync(container : string) : Async<ICloudSeq []> =
            info.CloudSeqProvider.GetContainedSeqs(container)

        /// <summary>
        /// Returns a CloudSeq that already exists in the specified contained with the given identifier.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        /// <param name="id">The CloudSeq's id.</param>
        member this.GetCloudSeqAsync(container : string, id : string) : Async<ICloudSeq> =
            info.CloudSeqProvider.GetExisting(container, id)

        /// <summary>
        /// Creates a new CloudSeq in the specified container.
        /// </summary>
        /// <param name="container">The folder's name.</param>
        /// <param name="values">The source sequence.</param>
        member this.CreateCloudSeq(container : string,  values : 'T seq) : ICloudSeq<'T> =
            Async.RunSynchronously <| info.CloudSeqProvider.Create(container, newId(), values)

        /// <summary>
        /// Returns an array of the CloudSeqs contained in the specified folder.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        member this.GetCloudSeqs(container : string) : ICloudSeq [] =
            Async.RunSynchronously <| info.CloudSeqProvider.GetContainedSeqs(container)

        /// <summary>
        /// Returns a CloudSeq that already exists in the specified contained with the given identifier.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        /// <param name="id">The CloudSeq's id.</param>
        member this.GetCloudSeq(container : string, id : string) : ICloudSeq =
            Async.RunSynchronously <| info.CloudSeqProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // CloudFile

        /// <summary> Create a new file in the storage with the specified folder and name.
        /// Use the serialize function to write to the underlying stream.</summary>
        /// <param name="container">The container (folder) of the CloudFile.</param>
        /// <param name="name">The (file)name of the CloudFile.</param>
        /// <param name="writer">The function that will write data on the underlying stream.</param>
        member this.CreateCloudFileAsync(container : string, name : string, writer : Stream -> Async<unit>) : Async<ICloudFile> =
            info.CloudFileProvider.Create(container, name, writer)

        /// <summary> Return all the files (as CloudFiles) in a folder.</summary>
        /// <param name="container">The container (folder) to search.</param>
        member this.GetCloudFilesAsync(container : string) : Async<ICloudFile []> =
            info.CloudFileProvider.GetContainedFiles(container)

        /// <summary> Create a CloudFile from an existing file.</summary>
        /// <param name="container">The container (folder) of the file.</param>
        /// <param name="name">The filename.</param>
        member this.GetCloudFileAsync(container : string, name : string) : Async<ICloudFile> =
            info.CloudFileProvider.GetExisting(container, name)
            
        /// <summary> Create a new file in the storage with the specified folder and name.
        /// Use the serialize function to write to the underlying stream.</summary>
        /// <param name="container">The container (folder) of the CloudFile.</param>
        /// <param name="name">The (file)name of the CloudFile.</param>
        /// <param name="writer">The function that will write data on the underlying stream.</param>
        member this.CreateCloudFile(container : string, name : string, writer : Stream -> Async<unit>) : ICloudFile =
            Async.RunSynchronously <| info.CloudFileProvider.Create(container, name, writer)

        /// <summary> Return all the files (as CloudFiles) in a folder.</summary>
        /// <param name="container">The container (folder) to search.</param>
        member this.GetCloudFiles(container : string) : ICloudFile [] =
            Async.RunSynchronously <| info.CloudFileProvider.GetContainedFiles(container)

        /// <summary> Create a CloudFile from an existing file.</summary>
        /// <param name="container">The container (folder) of the file.</param>
        /// <param name="name">The filename.</param>
        member this.GetCloudFile(container : string, id : string) : ICloudFile =
            Async.RunSynchronously <| info.CloudFileProvider.GetExisting(container, id)
            
        //---------------------------------------------------------------------------------
        // MutableCloudRef

        /// <summary>Creates a new MutableCloudRef in the specified folder with the specified identifier.</summary>
        /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
        /// <param name="id">The identifier of the MutableCloudRef in the underlying store.</param>
        /// <param name="value">The value to be stored.</param>
        member this.CreateMutableCloudRefAsync(container : string, id : string,  value : 'T) : Async<IMutableCloudRef<'T>> =
            info.MutableCloudRefProvider.Create(container, id, value)
           
        /// <summary>Returns an array of all the MutableCloudRefs within the container.</summary>
        /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
        member this.GetMutableCloudRefsAsync(container : string) : Async<IMutableCloudRef []> =
            info.MutableCloudRefProvider.GetContainedRefs(container)

        /// <summary>Returns the MutableCloudRef with the specified container and id combination.</summary>
        /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
        /// <param name="id">The identifier of the MutableCloudRef in the underlying store.</param>
        member this.GetMutableCloudRefAsync(container : string, id : string) : Async<IMutableCloudRef> =
            info.MutableCloudRefProvider.GetExisting(container, id)
            

        /// <summary>Creates a new MutableCloudRef in the specified folder with the specified identifier.</summary>
        /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
        /// <param name="id">The identifier of the MutableCloudRef in the underlying store.</param>
        /// <param name="value">The value to be stored.</param>
        member this.CreateMutableCloudRef(container : string, id : string, value : 'T) : IMutableCloudRef<'T> =
            Async.RunSynchronously <| info.MutableCloudRefProvider.Create(container, id, value)

        /// <summary>Returns an array of all the MutableCloudRefs within the container.</summary>
        /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
        member this.GetMutableCloudRefs(container : string) : IMutableCloudRef [] =
            Async.RunSynchronously <| info.MutableCloudRefProvider.GetContainedRefs(container)

        /// <summary>Returns the MutableCloudRef with the specified container and id combination.</summary>
        /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
        /// <param name="id">The identifier of the MutableCloudRef in the underlying store.</param>
        member this.GetMutableCloudRef(container : string, id : string) : IMutableCloudRef =
            Async.RunSynchronously <| info.MutableCloudRefProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // Misc

        /// <summary>Deletes the specified container from store.</summary>
        /// <param name="container">The folder to delete.</param>
        member this.DeleteContainerAsync(container : string) : Async<unit> =
            info.Store.DeleteContainer(container)

        /// <summary>Deletes the specified container from store.</summary>
        /// <param name="container">The folder to delete.</param>
        member this.DeleteContainer(container : string) : unit =
            Async.RunSynchronously <| this.DeleteContainerAsync(container)

        ///<summary>Retrieves the name of all containers from store.</summary>
        ///<remarks>This method lists all existing containers whether they were created from mbrace or not.</remarks>
        member this.GetContainersAsync() : Async<string []> =
            info.Store.GetAllContainers()

        ///<summary>Retrieves the name of all containers from store.</summary>
        ///<remarks>This method lists all existing containers whether they were created from mbrace or not.</remarks>
        member this.GetContainers() : string [] =
            Async.RunSynchronously <| this.GetContainersAsync()
            
        /// <summary>Checks if container exists in the given path.</summary>
        /// <param name="container">The container to search for.</param>
        member this.ContainerExistsAsync(container : string) : Async<bool> =
            info.Store.ContainerExists(container)

        /// <summary>Checks if container exists in the given path.</summary>
        /// <param name="container">The container to search for.</param>
        member this.ContainerExists(container : string) : bool =
            Async.RunSynchronously <| this.ContainerExistsAsync(container)
        