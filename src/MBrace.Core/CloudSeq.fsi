namespace Nessos.MBrace

    /// Provides methods to create and read a CloudSeq.
    type CloudSeq =
        class
            /// Returns an array of the CloudSeqs contained in the specified folder.
            static member Get : container:string -> Cloud<ICloudSeq []>
            
            /// Returns a CloudSeq that already exists in the specified contained with the given name.
            static member Get : container:string * name:string -> Cloud<ICloudSeq<'T>>
            
            /// <summary>Creates a new CloudRef in the process's container (folder).</summary>
            /// <param name="value">The value to be stored.</param>
            static member New : values:seq<'T> -> Cloud<ICloudSeq<'T>>

            /// <summary>Creates a new CloudSeq in the specified container.</summary>
            /// <param name="container">The container (folder) of the CloudSeq in the underlying store.</param>
            /// <param name="value">The values to be stored.</param>
            static member New : container:string * values:seq<'T> -> Cloud<ICloudSeq<'T>>
            
            /// <summary>Gets the values stored in a CloudRef.</summary>
            /// <param name="sequence">The CloudSeq to read.</param>
            static member Read : sequence:ICloudSeq<'T> -> Cloud<seq<'T>>

            /// Returns an array of the CloudSeqs contained in the specified folder.            
            static member TryGet : container:string -> Cloud<ICloudSeq [] option>

            /// Returns a CloudSeq that already exists in the specified contained with the given name.
            static member TryGet : container:string * name:string -> Cloud<ICloudSeq<'T> option>

            /// <summary>Creates a new CloudSeq in the specified container.</summary>
            /// <param name="container">The container (folder) of the CloudSeq in the underlying store.</param>
            /// <param name="value">The values to be stored.</param>            
            static member TryNew : container:string * values:seq<'T> -> Cloud<ICloudSeq<'T> option>
        end