namespace Nessos.MBrace
    
    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.MBrace.CloudExpr

    /// The type responsible for the ICloudFile management. Offers methods for
    /// file creation, listing and reading.
    type CloudFile = 

        /// <summary> 
        ///     Create a new file in the storage with the specified folder and name.
        ///     Use the serialize function to write to the underlying stream.
        /// </summary>
        /// <param name="container">The container (folder) of the CloudFile.</param>
        /// <param name="name">The (file)name of the CloudFile.</param>
        /// <param name="serializer">The function that will write data on the underlying stream.</param>
        static member New(container : string, name : string, serializer : (Stream -> Async<unit>)) : Cloud<ICloudFile> =
            CloudExpr.wrap <| NewCloudFile(container, name, serializer)

        /// <summary>
        ///     Create a new file in the storage in the specified folder.
        ///     Use the serialize function to write to the underlying stream.
        /// </summary>
        /// <param name="container">The container (folder) of the CloudFile.</param>
        /// <param name="serializer">The function that will write data on the underlying stream.</param>
        static member New(container : string, serializer : (Stream -> Async<unit>)) : Cloud<ICloudFile> =
            // TODO : container name should be specified by runtime,
            // not the Core library
            CloudFile.New(container, Guid.NewGuid().ToString(), serializer)

        /// <summary> 
        ///     Create a new file in the storage provider.
        ///     Use the serialize function to write to the underlying stream.
        /// </summary>
        /// <param name="serializer">The function that writes data to the underlying stream.</param>
        static member New(serializer : (Stream -> Async<unit>)) : Cloud<ICloudFile> =
            cloud {
                // TODO : container name should be specified by runtime,
                // not the Core library
                let! pid = Cloud.GetProcessId()
                return! CloudFile.New(sprintf "process%d" pid, serializer)
            }

        /// <summary> 
        ///     Read the contents of a CloudFile using the given deserialize/reader function.
        /// </summary>
        /// <param name="cloudFile">The CloudFile to read.</param>
        /// <param name="deserialize">The function that reads data from the underlying stream.</param>
        static member Read(cloudFile : ICloudFile, deserialize : (Stream -> Async<'Result>)) : Cloud<'Result> =
            let deserialize stream = async { let! o = deserialize stream in return o :> obj }
            CloudExpr.wrap <| ReadCloudFile(cloudFile, deserialize, typeof<'Result>)

        /// <summary> 
        ///     Returns an existing CloudFile of given container and name.
        /// </summary>
        /// <param name="container">The container (folder) of the file.</param>
        /// <param name="name">The filename.</param>
        static member Get(container : string, name : string) : Cloud<ICloudFile> =
            CloudExpr.wrap <| GetCloudFile(container, name)

        /// <summary> 
        ///     Returns all CloudFiles in given container.
        /// </summary>
        /// <param name="container">The container (folder) to search.</param>
        static member Enumerate(container : string) : Cloud<ICloudFile []> =
            CloudExpr.wrap <| GetCloudFiles(container)

        /// <summary>
        ///     Try to create a CloudFile from an existing file.
        /// </summary>
        /// <param name="container">The container (folder) of the file.</param>
        /// <param name="name">The filename.</param>
        static member TryGet(container : string, name : string) : Cloud<ICloudFile option> =
            mkTry<StoreException,_> <| CloudFile.Get(container,name)