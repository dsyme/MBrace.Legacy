namespace Nessos.MBrace.Core

    open System
    open System.IO

    type Tag = string

    /// Cloud filesystem abstraction
    type ICloudFileSystem =

        /// A description of the implementation
        abstract Name : string

        /// Unique store endpoint identifier
        abstract UUID : string

        // General-purpose methods

        /// Resolves the container of given path
        abstract GetContainer       : path:string -> string
        /// Gets all files that exist in given container
        abstract GetAllFiles        : container:string -> Async<string []>
        /// Get all container paths that exist in file system
        abstract GetAllContainer    : unit -> Async<string []>
        /// Checks if file exists in given path
        abstract FileExists         : path:string -> Async<bool>
        /// Checks if container exists in given path
        abstract ContainerExists    : path:string -> Async<bool>
        /// Deletes container in given path
        abstract DeleteContainer    : path:string -> Async<unit>
        /// Deletes file in given path
        abstract DeleteFile         : path:string -> Async<unit>

        //
        // Immutable file section
        //

        /// <summary>
        ///     Creates a new immutable file in store.
        /// </summary>
        /// <param name="path">file path.</param>
        /// <param name="source">local stream to copy data from.</param>
        /// <param name="asFile">treat as binary object.</param>
        abstract CreateImmutable    : path:string * source:Stream * asFile:bool -> Async<unit>

        /// <summary>
        ///     Reads an existing immutable file from store.
        /// </summary>
        /// <param name="path">file path.</param>
        /// <param name="target">local stream to copy data to.</param>
        abstract ReadImmutable      : path:string * target:Stream -> Async<Tag>

        /// <summary>
        ///     Creates a new immutable file in store.
        /// </summary>
        /// <param name="path">file path.</param>
        /// <param name="writer">writer function; asynchronously write to the target stream.</param>
        /// <param name="asFile">treat as binary object.</param>
        abstract CreateImmutable    : path:string * writer:(Stream -> Async<unit>) * asFile:bool -> Async<unit>

        /// <summary>
        ///     Reads an existing immutable file from store.
        /// </summary>
        /// <param name="path">file path.</param>
        /// <param name="reader">reader function; asynchronously read from the source stream.</param>
        abstract ReadImmutable      : path:string * reader:(Stream -> Async<unit>) -> Async<unit>

        //
        //  Mutable file section
        //

        /// <summary>
        ///     Creates a new mutable file in store.
        /// </summary>
        /// <param name="path">file path.</param>
        /// <param name="writer">writer function; asynchronously write to the target stream.</param>
        /// <returns>if succesful, returns a tag identifier.</returns>
        abstract CreateMutable  : path:string * writer:(Stream -> Async<unit>) -> Async<Tag>

        /// <summary>
        ///     Reads a mutable file from store
        /// </summary>
        /// <param name="path">file path.</param>
        /// <param name="reader">reader function; asynchronously read from the source stream.</param>
        /// <returns>if succesful, returns a tag identifier.</returns>
        abstract ReadMutable    : path:string * reader:(Stream -> Async<unit>) -> Async<Tag>

        /// <summary>
        ///     Attempt to update mutable file in store.
        /// </summary>
        /// <param name="path">file path.</param>
        /// <param name="writer">writer function; asynchronously write to the target stream.</param>
        /// <param name="tag">tag used to update the file.</param>
        /// <returns>a boolean signifying that the update was succesful and an updated tag identifier.</returns>
        abstract TryUpdate      : path:string * writer:(Stream -> Async<unit>) * tag:Tag -> Async<bool * Tag>

        /// <summary>
        ///     Force update a mutable file in store.
        /// </summary>
        /// <param name="path">file path.</param>
        /// <param name="writer">writer function; asynchronously write to the target stream.</param>
        abstract ForceUpdate    : path:string * writer:(Stream -> Async<unit>) -> Async<Tag>


    and ICloudFileSystemFactory =
        /// Constructs a file system instance from given connection string
        abstract CreateStoreFromConnectionString: connectionString : string -> ICloudFileSystem


//    and [<StructuralEquality ; StructuralComparison>] StoreId = 
//        internal {
//            AssemblyQualifiedName : string
//            UUID                  : byte []
//        }
//    with 
//        override id.ToString () = id.AssemblyQualifiedName
