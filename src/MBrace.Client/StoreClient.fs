namespace Nessos.MBrace.Client

    open System
    open System.IO

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime
    open Nessos.Thespian.ConcurrencyTools

    /// Provides methods for interacting with cloud storage.
    [<Sealed; AutoSerializable(false)>]
    type StoreClient internal (info : StoreInfo) =

        static do MBraceSettings.Init()

        let newId () = Guid.NewGuid().ToString()
        let defaultContainer = MBraceSettings.DefaultContainer

        static let registry = Atom.atom Map.empty<StoreId, StoreClient>

        /// Gets the Store name for given client instance.
        member __.Name = info.Store.Name

        /// Initializes a StoreClient for given store definition
        static member Create(store : ICloudStore) =
            let info = StoreRegistry.Register(store, makeDefault = false)
            new StoreClient(info)

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
        member this.EnumerateCloudRefsAsync(container : string) : Async<ICloudRef []> =
            info.CloudRefProvider.GetContainedRefs(container)

        /// <summary>
        /// Returns a CloudRef stored in the specified container with the given identifier.
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
        member this.EnumerateCloudRefs(container : string) : ICloudRef [] =
            Async.RunSynchronously <|info.CloudRefProvider.GetContainedRefs(container)

        /// <summary>
        /// Returns a CloudRef stored in the specified container with the given identifier.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        /// <param name="id">The CloudRef's id.</param>
        member this.GetCloudRef(container : string, id : string) : ICloudRef =
            Async.RunSynchronously <| info.CloudRefProvider.GetExisting(container, id)
            

        // Default container methods.

        /// <summary>
        /// Creates a new CloudRef in default container.
        /// </summary>
        /// <param name="value">The CloudRef's value.</param>
        member this.CreateCloudRefAsync(value : 'T) : Async<ICloudRef<'T>> =
            info.CloudRefProvider.Create(defaultContainer, newId(), value)
            
        /// <summary>
        /// Returns an array of the CloudRefs contained in default container.
        /// </summary>
        member this.EnumerateCloudRefsAsync() : Async<ICloudRef []> =
            info.CloudRefProvider.GetContainedRefs(defaultContainer)

        /// <summary>
        /// Returns a CloudRef stored in default container with the given identifier.
        /// </summary>
        /// <param name="id">The CloudRef's id.</param>
        member this.GetCloudRefAsync(id : string) : Async<ICloudRef> =
            info.CloudRefProvider.GetExisting(defaultContainer, id)

        /// <summary>
        /// Creates a new CloudRef in default container.
        /// </summary>
        /// <param name="value">The CloudRef's value.</param>
        member this.CreateCloudRef(value : 'T) : ICloudRef<'T> =
            Async.RunSynchronously <| info.CloudRefProvider.Create(defaultContainer, newId(), value)
        
        /// <summary>
        /// Returns an array of the CloudRefs contained in default container.
        /// </summary>
        member this.EnumerateCloudRefs() : ICloudRef [] =
            Async.RunSynchronously <|info.CloudRefProvider.GetContainedRefs(defaultContainer)

        /// <summary>
        /// Returns a CloudRef stored in default container with the given identifier.
        /// </summary>
        /// <param name="id">The CloudRef's id.</param>
        member this.GetCloudRef(id : string) : ICloudRef =
            Async.RunSynchronously <| info.CloudRefProvider.GetExisting(defaultContainer, id)




        //---------------------------------------------------------------------------------
        // CloudSeq

        /// <summary>
        ///     Creates a new CloudSeq in the specified container.
        /// </summary>
        /// <param name="container">The folder's name.</param>
        /// <param name="values">The source sequence.</param>
        member this.CreateCloudSeqAsync(container : string,  values : 'T seq) : Async<ICloudSeq<'T>> =
            info.CloudSeqProvider.Create(container, newId(), values)

        /// <summary>
        ///     Returns an array of the CloudSeqs contained in the specified folder.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        member this.EnumerateCloudSeqsAsync(container : string) : Async<ICloudSeq []> =
            info.CloudSeqProvider.GetContainedSeqs(container)

        /// <summary>
        /// Returns a CloudSeq stored in the specified container with the given identifier.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        /// <param name="id">The CloudSeq's id.</param>
        member this.GetCloudSeqAsync(container : string, id : string) : Async<ICloudSeq> =
            info.CloudSeqProvider.GetExisting(container, id)

        /// <summary>
        ///     Creates a new CloudSeq in the specified container.
        /// </summary>
        /// <param name="container">The folder's name.</param>
        /// <param name="values">The source sequence.</param>
        member this.CreateCloudSeq(container : string,  values : 'T seq) : ICloudSeq<'T> =
            Async.RunSynchronously <| info.CloudSeqProvider.Create(container, newId(), values)

        /// <summary>
        ///     Returns an array of the CloudSeqs contained in the specified folder.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        member this.EnumerateCloudSeqs(container : string) : ICloudSeq [] =
            Async.RunSynchronously <| info.CloudSeqProvider.GetContainedSeqs(container)

        /// <summary>
        ///     Returns a CloudSeq stored in the specified container with the given identifier.
        /// </summary>
        /// <param name="container">The folder to search.</param>
        /// <param name="id">The CloudSeq's id.</param>
        member this.GetCloudSeq(container : string, id : string) : ICloudSeq =
            Async.RunSynchronously <| info.CloudSeqProvider.GetExisting(container, id)



        // Default container methods.

        /// <summary>
        ///     Creates a new CloudSeq in default container.
        /// </summary>
        /// <param name="values">The source sequence.</param>
        member this.CreateCloudSeqAsync(values : 'T seq) : Async<ICloudSeq<'T>> =
            info.CloudSeqProvider.Create(defaultContainer, newId(), values)

        /// <summary>
        ///     Returns an array of the CloudSeqs contained default container.
        /// </summary>
        member this.EnumerateCloudSeqsAsync() : Async<ICloudSeq []> =
            info.CloudSeqProvider.GetContainedSeqs(defaultContainer)

        /// <summary>
        /// Returns a CloudSeq stored in default container with the given identifier.
        /// </summary>
        /// <param name="id">The CloudSeq's id.</param>
        member this.GetCloudSeqAsync(id : string) : Async<ICloudSeq> =
            info.CloudSeqProvider.GetExisting(defaultContainer, id)

        /// <summary>
        ///     Creates a new CloudSeq in default container.
        /// </summary>
        /// <param name="values">The source sequence.</param>
        member this.CreateCloudSeq(values : 'T seq) : ICloudSeq<'T> =
            Async.RunSynchronously <| info.CloudSeqProvider.Create(defaultContainer, newId(), values)

        /// <summary>
        ///     Returns an array of the CloudSeqs contained in default container.
        /// </summary>
        member this.EnumerateCloudSeqs() : ICloudSeq [] =
            Async.RunSynchronously <| info.CloudSeqProvider.GetContainedSeqs(defaultContainer)

        /// <summary>
        ///     Returns a CloudSeq stored in default container with the given identifier.
        /// </summary>
        /// <param name="id">The CloudSeq's id.</param>
        member this.GetCloudSeq(id : string) : ICloudSeq =
            Async.RunSynchronously <| info.CloudSeqProvider.GetExisting(defaultContainer, id)


        //---------------------------------------------------------------------------------
        // CloudFile

        /// <summary>
        ///     Create a new file in the storage with the specified folder and name.
        ///     Use the serialize function to write to the underlying stream.
        /// </summary>
        /// <param name="container">The container (folder) of the CloudFile.</param>
        /// <param name="name">The (file)name of the CloudFile.</param>
        /// <param name="writer">The function that will write data on the underlying stream.</param>
        member this.CreateCloudFileAsync(container : string, name : string, writer : Stream -> Async<unit>) : Async<ICloudFile> =
            info.CloudFileProvider.Create(container, name, writer)

        /// <summary> Return all the files (as CloudFiles) in a folder.</summary>
        /// <param name="container">The container (folder) to search.</param>
        member this.EnumerateCloudFilesAsync(container : string) : Async<ICloudFile []> =
            info.CloudFileProvider.GetContainedFiles(container)

        /// <summary> Create a CloudFile from an existing file.</summary>
        /// <param name="container">The container (folder) of the file.</param>
        /// <param name="name">The filename.</param>
        member this.GetCloudFileAsync(container : string, name : string) : Async<ICloudFile> =
            info.CloudFileProvider.GetExisting(container, name)
            
        /// <summary> 
        ///     Create a new file in the storage with the specified folder and name.
        ///     Use the serialize function to write to the underlying stream.
        /// </summary>
        /// <param name="container">The container (folder) of the CloudFile.</param>
        /// <param name="name">The (file)name of the CloudFile.</param>
        /// <param name="writer">The function that will write data on the underlying stream.</param>
        member this.CreateCloudFile(container : string, name : string, writer : Stream -> Async<unit>) : ICloudFile =
            Async.RunSynchronously <| info.CloudFileProvider.Create(container, name, writer)

        /// <summary> Return all the files (as CloudFiles) in a folder.</summary>
        /// <param name="container">The container (folder) to search.</param>
        member this.EnumerateCloudFiles(container : string) : ICloudFile [] =
            Async.RunSynchronously <| info.CloudFileProvider.GetContainedFiles(container)

        /// <summary> Create a CloudFile from an existing file.</summary>
        /// <param name="container">The container (folder) of the file.</param>
        /// <param name="name">The filename.</param>
        member this.GetCloudFile(container : string, id : string) : ICloudFile =
            Async.RunSynchronously <| info.CloudFileProvider.GetExisting(container, id)


        /// <summary>
        ///     Uploads given collection of local files to the runtime store.
        ///     Returns an array of CloudFiles that point to the uploaded resources.
        /// </summary>
        /// <param name="paths">Array of paths to local files.</param>
        /// <param name="container">Container to place CloudFiles. Defaults to auto-generated container.</param>
        /// <returns>an array of CloudFiles.</returns>
        member sc.UploadFilesAsync(paths : string [], ?container) =
            let ufng = new UniqueFileNameGenerator()
            let container =
                match container with
                | None -> let g = Guid.NewGuid().ToString("N") in sprintf "folder%s" g
                | Some container -> container

            let upload (path : string) = async {
                let writer (target : Stream) = async {
                    use source = File.OpenRead path
                    return! Stream.AsyncCopy(source, target)
                }

                let fileName = ufng.GetFileName <| Path.GetFileName path
                return! sc.CreateCloudFileAsync(container, fileName, writer)
            }

            paths |> Array.map upload |> Async.Parallel

        /// <summary>
        ///     Uploads given collection of local files to the runtime store.
        ///     Returns an array of CloudFiles that point to the uploaded resources.
        /// </summary>
        /// <param name="paths">Array of paths to local files.</param>
        /// <param name="container">Container to place CloudFiles. Defaults to auto-generated container.</param>
        /// <returns>an array of CloudFiles.</returns>
        member sc.UploadFiles(paths : string [], ?container) = 
            sc.UploadFilesAsync(paths, ?container = container) |> Async.RunSynchronously
        
        // Default container methods.

        /// <summary>
        ///     Create a new file in the storage with the specified name in default container.
        ///     Use the serialize function to write to the underlying stream.
        /// </summary>
        /// <param name="name">The (file)name of the CloudFile.</param>
        /// <param name="writer">The function that will write data on the underlying stream.</param>
        member this.CreateCloudFileAsync(name : string, writer : Stream -> Async<unit>) : Async<ICloudFile> =
            info.CloudFileProvider.Create(defaultContainer, name, writer)

        /// <summary> Returns CloudFiles to all contents in the default container.</summary>
        member this.EnumerateCloudFilesAsync() : Async<ICloudFile []> =
            info.CloudFileProvider.GetContainedFiles(defaultContainer)

        /// <summary> Create a CloudFile from an existing file in default container.</summary>
        /// <param name="name">The filename.</param>
        member this.GetCloudFileAsync(name : string) : Async<ICloudFile> =
            info.CloudFileProvider.GetExisting(defaultContainer, name)
            
        /// <summary>
        ///     Create a new file in the storage with the specified name in default container.
        ///     Use the serialize function to write to the underlying stream.
        /// </summary>
        /// <param name="name">The (file)name of the CloudFile.</param>
        /// <param name="writer">The function that will write data on the underlying stream.</param>
        member this.CreateCloudFile(name : string, writer : Stream -> Async<unit>) : ICloudFile =
            Async.RunSynchronously <| info.CloudFileProvider.Create(defaultContainer, name, writer)

        /// <summary> Returns CloudFiles to all contents in the default container.</summary>
        member this.EnumerateCloudFiles() : ICloudFile [] =
            Async.RunSynchronously <| info.CloudFileProvider.GetContainedFiles(defaultContainer)

        /// <summary> Create a CloudFile from an existing file in default container.</summary>
        /// <param name="name">The filename.</param>
        member this.GetCloudFile(id : string) : ICloudFile =
            Async.RunSynchronously <| info.CloudFileProvider.GetExisting(defaultContainer, id)




            
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



        // Default container methods.

        /// <summary>Creates a new MutableCloudRef in default container with the specified identifier.</summary>
        /// <param name="id">The identifier of the MutableCloudRef in the underlying store.</param>
        /// <param name="value">The value to be stored.</param>
        member this.CreateMutableCloudRefAsync(id : string,  value : 'T) : Async<IMutableCloudRef<'T>> =
            info.MutableCloudRefProvider.Create(defaultContainer, id, value)
           
        /// <summary>Returns an array of all the MutableCloudRefs in default container.</summary>
        member this.GetMutableCloudRefsAsync() : Async<IMutableCloudRef []> =
            info.MutableCloudRefProvider.GetContainedRefs(defaultContainer)

        /// <summary>Returns the MutableCloudRef in default container with the given id.</summary>
        /// <param name="id">The identifier of the MutableCloudRef in the underlying store.</param>
        member this.GetMutableCloudRefAsync(id : string) : Async<IMutableCloudRef> =
            info.MutableCloudRefProvider.GetExisting(defaultContainer, id)
            

        /// <summary>Creates a new MutableCloudRef in default container with the specified identifier.</summary>
        /// <param name="id">The identifier of the MutableCloudRef in the underlying store.</param>
        /// <param name="value">The value to be stored.</param>
        member this.CreateMutableCloudRef(id : string, value : 'T) : IMutableCloudRef<'T> =
            Async.RunSynchronously <| info.MutableCloudRefProvider.Create(defaultContainer, id, value)

        /// <summary>Returns an array of all the MutableCloudRefs in default container.</summary>
        member this.GetMutableCloudRefs() : IMutableCloudRef [] =
            Async.RunSynchronously <| info.MutableCloudRefProvider.GetContainedRefs(defaultContainer)

        /// <summary>Returns the MutableCloudRef in default container with the given id.</summary>
        /// <param name="id">The identifier of the MutableCloudRef in the underlying store.</param>
        member this.GetMutableCloudRef(id : string) : IMutableCloudRef =
            Async.RunSynchronously <| info.MutableCloudRefProvider.GetExisting(defaultContainer, id)


        // Default id methods.

        /// <summary>Creates a new MutableCloudRef in default container.</summary>
        /// <param name="value">The value to be stored.</param>
        member this.CreateMutableCloudRefAsync(value : 'T) : Async<IMutableCloudRef<'T>> =
            info.MutableCloudRefProvider.Create(defaultContainer, newId(), value)
   
        /// <summary>Creates a new MutableCloudRef in default container.</summary>
        /// <param name="value">The value to be stored.</param>
        member this.CreateMutableCloudRef(value : 'T) : IMutableCloudRef<'T> =
            Async.RunSynchronously <| info.MutableCloudRefProvider.Create(defaultContainer, newId(), value)


        //---------------------------------------------------------------------------------
        // CloudArray
        // TODO : Revise/populate
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="container"></param>
        /// <param name="values"></param>
        member this.CreateCloudArrayAsync(container : string, values : seq<'T>) : Async<ICloudArray<'T>> =
            async { let! ca = info.CloudArrayProvider.Create(container, values, typeof<'T>) in return ca :?> ICloudArray<'T> }

        member this.CreateCloudArray(container : string, values : seq<'T>) : ICloudArray<'T> =
            Async.RunSynchronously <| this.CreateCloudArrayAsync(container, values)


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
        member this.EnumerateContainersAsync() : Async<string []> =
            info.Store.EnumerateContainers()

        ///<summary>Retrieves the name of all containers from store.</summary>
        ///<remarks>This method lists all existing containers whether they were created from mbrace or not.</remarks>
        member this.EnumerateContainers() : string [] =
            Async.RunSynchronously <| this.EnumerateContainersAsync()
            
        /// <summary>Checks if container exists in the given path.</summary>
        /// <param name="container">The container to search for.</param>
        member this.ContainerExistsAsync(container : string) : Async<bool> =
            info.Store.ContainerExists(container)

        /// <summary>Checks if container exists in the given path.</summary>
        /// <param name="container">The container to search for.</param>
        member this.ContainerExists(container : string) : bool =
            Async.RunSynchronously <| this.ContainerExistsAsync(container)

        // Default container methods.

        /// <summary>Deletes the default container from store.</summary>
        member this.DeleteContainerAsync() : Async<unit> =
            info.Store.DeleteContainer(defaultContainer)

        /// <summary>Deletes the default container from store.</summary>
        member this.DeleteContainer() : unit =
            Async.RunSynchronously <| this.DeleteContainerAsync(defaultContainer)
        