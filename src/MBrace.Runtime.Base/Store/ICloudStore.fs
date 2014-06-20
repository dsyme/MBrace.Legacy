namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO

    type Tag = string

    /// Cloud filesystem abstraction
    type ICloudStore =

        /// A description of the implementation
        abstract Name : string

        /// Unique store endpoint identifier
        abstract UUID : string

        // General-purpose methods

        /// <summary>
        ///     Checks if file exists in given path
        /// </summary>
        /// <param name="container">file container.</param>
        /// <param name="name">file name.</param>
        abstract Exists         : container:string * name:string -> Async<bool>

        /// <summary>
        ///     Deletes file in given path
        /// </summary>
        /// <param name="container">file container.</param>
        /// <param name="name">file name.</param>
        abstract Delete         : container:string * name:string -> Async<unit>

        /// <summary>
        ///     Gets all files that exist in given container
        /// </summary>
        /// <param name="container">file container.</param>
        abstract GetAllFiles        : container:string -> Async<string []>

        /// <summary>
        ///     Checks if container exists in given path
        /// </summary>
        /// <param name="container">file container.</param>
        abstract ContainerExists    : container:string -> Async<bool>
        
        /// <summary>
        ///     Deletes container in given path
        /// </summary>
        /// <param name="container">file container.</param>
        abstract DeleteContainer    : container:string -> Async<unit>

        /// Get all container paths that exist in file system
        abstract GetAllContainers   : unit -> Async<string []>
        
        //
        // Immutable file section
        //

        /// <summary>
        ///     Creates a new immutable file in from stream.
        /// </summary>
        /// <param name="container">file container.</param>
        /// <param name="name">file name.</param>
        /// <param name="source">local stream to copy data from.</param>
        /// <param name="asFile">treat as binary object.</param>
        abstract CopyFrom    : container:string * name:string * source:Stream * asFile:bool -> Async<unit>

        /// <summary>
        ///     Reads an existing immutable file to target stream.
        /// </summary>
        /// <param name="container">file container.</param>
        /// <param name="name">file name.</param>
        /// <param name="target">local stream to copy data to.</param>
        abstract CopyTo      : container:string * name:string * target:Stream -> Async<unit>

        /// <summary>
        ///     Creates a new immutable file in store.
        /// </summary>
        /// <param name="container">file container.</param>
        /// <param name="name">file name.</param>
        /// <param name="writer">writer function; asynchronously write to the target stream.</param>
        /// <param name="asFile">treat as binary object.</param>
        abstract CreateImmutable    : container:string * name:string * writer:(Stream -> Async<unit>) * asFile:bool -> Async<unit>

        /// <summary>
        ///     Reads an existing immutable file from store.
        /// </summary>
        /// <param name="container">file container.</param>
        /// <param name="name">file name.</param>
        /// <param name="reader">reader function; asynchronously read from the source stream.</param>
        abstract ReadImmutable      : container:string * name:string -> Async<Stream>

        //
        //  Mutable file section
        //

        /// <summary>
        ///     Creates a new mutable file in store.
        /// </summary>
        /// <param name="container">file container.</param>
        /// <param name="name">file name.</param>
        /// <param name="writer">writer function; asynchronously write to the target stream.</param>
        /// <returns>if successful, returns a tag identifier.</returns>
        abstract CreateMutable      : container:string * name:string * writer:(Stream -> Async<unit>) -> Async<Tag>

        /// <summary>
        ///     Reads a mutable file from store
        /// </summary>
        /// <param name="container">file container.</param>
        /// <param name="name">file name.</param>
        /// <returns>if succesful, returns a tag identifier.</returns>
        abstract ReadMutable        : container:string * name:string -> Async<Stream * Tag>

        /// <summary>
        ///     Attempt to update mutable file in store.
        /// </summary>
        /// <param name="container">file container.</param>
        /// <param name="name">file name.</param>
        /// <param name="writer">writer function; asynchronously write to the target stream.</param>
        /// <param name="tag">tag used to update the file.</param>
        /// <returns>a boolean signifying that the update was succesful and an updated tag identifier.</returns>
        abstract TryUpdateMutable   : container:string * name:string * writer:(Stream -> Async<unit>) * tag:Tag -> Async<bool * Tag>

        /// <summary>
        ///     Force update a mutable file in store.
        /// </summary>
        /// <param name="container">file container.</param>
        /// <param name="name">file name.</param>
        /// <param name="writer">writer function; asynchronously write to the target stream.</param>
        abstract ForceUpdateMutable : container:string * name:string * writer:(Stream -> Async<unit>) -> Async<Tag>

    type ICloudStoreFactory =
        abstract CreateStoreFromConnectionString: connectionString : string -> ICloudStore