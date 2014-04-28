namespace Nessos.MBrace

    
    [<AutoOpen>]
    module CloudRefModule =

        /// Provides methods to create and read CloudRefs
        type CloudRef =
            class
                /// Returns an array of the CloudRefs contained in the specified folder.
                static member Get : container:string -> ICloud<ICloudRef []>

                /// Returns an array of the CloudRefs contained in the specified folder.
                static member TryGet : container:string -> ICloud<ICloudRef [] option>

                /// Returns a CloudRef that already exists in the specified contained with the given identifier.
                static member Get : container:string * id:string -> ICloud<ICloudRef<'T>>

                /// <summary>Creates a new CloudRef in the process's container (folder).</summary>
                /// <param name="value">The value to be stored.</param>
                static member New : value:'T -> ICloud<ICloudRef<'T>>
                
                /// <summary>Creates a new CloudRef in the specified container.</summary>
                /// <param name="container">The container (folder) of the CloudRef in the underlying store.</param>
                /// <param name="value">The value to be stored.</param>
                static member New : container:string * value:'T -> ICloud<ICloudRef<'T>>

                /// <summary>Gets the value stored in a CloudRef.</summary>
                /// <param name="cref">The CloudRef to read.</param>
                static member Read : cref:ICloudRef<'T> -> ICloud<'T>

                /// Returns a CloudRef that already exists in the specified contained with the given identifier.
                static member TryGet : container:string * id:string -> ICloud<ICloudRef<'T> option>

                /// <summary>Creates a new CloudRef in the specified container.</summary>
                /// <param name="container">The container (folder) of the CloudRef in the underlying store.</param>
                /// <param name="value">The value to be stored.</param>
                static member TryNew : container:string * value:'T -> ICloud<ICloudRef<'T> option>
            end

        /// Creates a new CloudRef. This is a shorthand for the 'CloudRef.New' method.
        val newRef : value:'a -> ICloud<ICloudRef<'a>>
        /// An active pattern that matches an ICloudRef and returns its value.
        val ( |CloudRef| ) : cloudRef:ICloudRef<'T> -> 'T