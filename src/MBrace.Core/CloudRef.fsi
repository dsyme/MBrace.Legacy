namespace Nessos.MBrace


    /// Provides methods to create and read CloudRefs.
    type CloudRef =
        class
            /// Returns an array of the CloudRefs contained in the specified folder.
            static member Get : container:string -> Cloud<ICloudRef []>

            /// Returns an array of the CloudRefs contained in the specified folder.
            static member TryGet : container:string -> Cloud<ICloudRef [] option>

            /// Returns a CloudRef that already exists in the specified contained with the given identifier.
            static member Get : container:string * id:string -> Cloud<ICloudRef<'T>>

            /// <summary>Creates a new CloudRef in the process defined container.</summary>
            /// <param name="value">The value to be stored.</param>
            static member New : value:'T -> Cloud<ICloudRef<'T>>
                
            /// <summary>Creates a new CloudRef in the specified container.</summary>
            /// <param name="container">The container (folder) of the CloudRef in the underlying store.</param>
            /// <param name="value">The value to be stored.</param>
            static member New : container:string * value:'T -> Cloud<ICloudRef<'T>>

            /// <summary>Dereferences a CloudRef.</summary>
            /// <param name="cref">The CloudRef to read.</param>
            static member Read : cref:ICloudRef<'T> -> Cloud<'T>

            /// Returns a CloudRef that already exists in the specified contained with the given identifier.
            static member TryGet : container:string * id:string -> Cloud<ICloudRef<'T> option>

            /// <summary>Creates a new CloudRef in the specified container.</summary>
            /// <param name="container">The container (folder) of the CloudRef in the underlying store.</param>
            /// <param name="value">The value to be stored.</param>
            static member TryNew : container:string * value:'T -> Cloud<ICloudRef<'T> option>
        end

