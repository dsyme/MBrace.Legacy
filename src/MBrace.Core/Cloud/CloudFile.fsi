namespace Nessos.MBrace

    open System.IO

    /// The type responsible for the ICloudFile management. Offers methods for
    /// file creation, listing and reading.
    type CloudFile =
        class
            /// <summary> Create a new file in the storage with the specified folder and name.
            /// Use the serialize function to write to the underlying stream.</summary>
            /// <param name="container">The container (folder) of the CloudFile.</param>
            /// <param name="name">The (file)name of the CloudFile.</param>
            /// <param name="serialize">The function that will write data on the underlying stream.</param>
            static member Create : container:string * name:string * serialize:(Stream -> Async<unit>) -> Cloud<ICloudFile>

            /// <summary> Create a new file in the storage in the specified folder.
            /// Use the serialize function to write to the underlying stream.</summary>
            /// <param name="container">The container (folder) of the CloudFile.</param>
            /// <param name="serialize">The function that will write data on the underlying stream.</param>
            static member Create : container:string * serialize:(Stream -> Async<unit>) -> Cloud<ICloudFile>
            
            /// <summary> Create a new file in the storage .
            /// Use the serialize function to write to the underlying stream.</summary>
            /// <param name="serialize">The function that writes data to the underlying stream.</param>
            static member Create : serialize:(Stream -> Async<unit>) -> Cloud<ICloudFile>
            
            /// <summary> Read the contents of a CloudFile using the given deserialize/reader function.</summary>
            /// <param name="cloudFile">The CloudFile to read.</param>
            /// <param name="deserialize">The function that reads data from the underlying stream.</param>
            static member Read : cloudFile:ICloudFile * deserialize :(Stream -> Async<'Result>) -> Cloud<'Result>
            
            /// <summary> Read the contents of a CloudFile as a sequence of objects using the given deserialize/reader function.</summary>
            /// <param name="cloudFile">The CloudFile to read.</param>
            /// <param name="deserialize">The function that reads data from the underlying stream as a sequence.</param>
            static member ReadAsSeq : cloudFile:ICloudFile * deserialize :(Stream -> Async<seq<'T>>) -> Cloud<ICloudSeq<'T>>

            /// <summary> Return all the files (as CloudFiles) in a folder.</summary>
            /// <param name="container">The container (folder) to search.</param>
            static member Get : container:string -> Cloud<ICloudFile []>

            /// <summary> Create a CloudFile from an existing file.</summary>
            /// <param name="container">The container (folder) of the file.</param>
            /// <param name="name">The filename.</param>
            static member Get : container:string * name:string -> Cloud<ICloudFile>

            /// <summary> Try to create a new file in the storage with the specified folder and name.
            /// Use the serialize function to  write to the underlying stream.</summary>
            /// <param name="container">The container (folder) of the CloudFile.</param>
            /// <param name="name">The (file)name of the CloudFile.</param>
            /// <param name="serialize">The function that will write data on the underlying stream.</param>
            static member TryCreate : container:string * name:string * serialize:(Stream -> Async<unit>) -> Cloud<ICloudFile option>
            
            /// <summary> Try to list all the files (as CloudFiles) in a folder.</summary>
            /// <param name="container">The container (folder) to search.</param>
            static member TryGet : container:string -> Cloud<ICloudFile [] option>
            
            /// <summary> Try to create a CloudFile from an existing file.</summary>
            /// <param name="container">The container (folder) of the file.</param>
            /// <param name="name">The filename.</param>
            static member TryGet : container:string * name:string -> Cloud<ICloudFile option>
        end