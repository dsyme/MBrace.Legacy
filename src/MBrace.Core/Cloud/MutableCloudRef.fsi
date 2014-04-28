namespace Nessos.MBrace

    /// Provides methods to create, read, update and delete MutableCloudRefs.
    type MutableCloudRef =
        class
            /// <summary>Try to update the value of a MutableCloudRef. This method does
            /// not check if the MutableCloudRef was updated by someone else.</summary>
            /// <param name="mref">The MutableCloudRef.</param>
            /// <param name="value">The value to stored.</param>
            static member Force : mref:IMutableCloudRef<'T> * value:'T -> ICloud<unit>
            
            ///<summary>Deletes the MutableCloudRef from the underlying store.</summary>
            static member Free : mref:IMutableCloudRef<'T> -> ICloud<unit>
            
            /// <summary>Returns an array of all the MutableCloudRefs within the container.</summary>
            /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
            static member Get : container:string -> ICloud<IMutableCloudRef []>
            
            /// <summary>Returns the MutableCloudRef with the specified container and id combination.</summary>
            /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
            /// <param name="name">The identifier of the MutableCloudRef in the underlying store.</param>
            static member Get : container:string * name:string -> ICloud<IMutableCloudRef<'T>>
            
            /// <summary>Creates a new MutableCloudRef in the process's container (folder).</summary>
            /// <param name="value">The value to be stored.</param>
            static member New : value:'T -> ICloud<IMutableCloudRef<'T>>
            
            /// <summary>Creates a new MutableCloudRef in the specified container.</summary>
            /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
            /// <param name="value">The value to be stored.</param>
            static member New : container:string * value:'T -> ICloud<IMutableCloudRef<'T>>
            
            /// <summary>Creates a new MutableCloudRef in the specified folder with the specified identifier.</summary>
            /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
            /// <param name="name">The identifier of the MutableCloudRef in the underlying store.</param>
            /// <param name="value">The value to stored.</param>
            static member New : container:string * name:string * value:'T -> ICloud<IMutableCloudRef<'T>>
            
            /// <summary>Gets the current value of a MutableCloudRef.</summary>
            /// <param name="mref">The MutableCloudRef.</param>
            static member Read : mref:IMutableCloudRef<'T> -> ICloud<'T>
            
            /// <summary>Try to update the value of a MutableCloudRef. Returns true
            /// if the operation succeeded, or false if the MutableCloudRef was modified (by someone else) since the last time
            /// the it was read.</summary>
            /// <param name="mref">The MutableCloudRef.</param>
            /// <param name="value">The value to stored.</param>
            static member Set : mref:IMutableCloudRef<'T> * value:'T -> ICloud<bool>
            
            /// <summary>Updates the MutableCloudRef using the update function.
            /// This method will return when the update is successful.</summary>
            /// <param name="mref">The MutableCloudRef to be updated.</param>
            /// <param name="update">A function that takes the current value of the MutableCloudRef and
            /// returns the new value to be stored.</param>
            /// <param name="interval">The interval, in milliseconds, to sleep between the spin calls.</param>
            /// <returns>Unit</returns>
            static member SpinSet : mref:IMutableCloudRef<'T> * update:('T -> 'T) * ?interval:int -> ICloud<unit>
            
            /// <summary>Try to update the value of a MutableCloudRef. This method does
            /// not check if the MutableCloudRef was updated by someone else.</summary>
            /// <param name="mref">The MutableCloudRef.</param>
            /// <param name="value">The value to stored.</param>
            static member TryForce : mref:IMutableCloudRef<'T> * value:'T -> ICloud<unit option>
            
            ///<summary>Deletes the MutableCloudRef from the underlying store.</summary>
            static member TryFree : mref:IMutableCloudRef<'T> -> ICloud<unit option>
            
            /// <summary>Returns an array of all the MutableCloudRefs within the container.</summary>
            /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
            static member TryGet : container:string -> ICloud<IMutableCloudRef [] option>
            
            /// <summary>Returns the MutableCloudRef with the specified container and id combination.</summary>
            /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
            /// <param name="name">The identifier of the MutableCloudRef in the underlying store.</param>
            static member TryGet : container:string * name:string -> ICloud<IMutableCloudRef<'T> option>
            
            /// <summary>Creates a new MutableCloudRef in the process's container (folder).</summary>
            /// <param name="value">The value to be stored.</param>
            static member TryNew : value:'T -> ICloud<IMutableCloudRef<'T> option>
            
            /// <summary>Creates a new MutableCloudRef in the specified container.</summary>
            /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
            /// <param name="value">The value to be stored.</param>
            static member TryNew : container:string * value:'T -> ICloud<IMutableCloudRef<'T> option>
            
            /// <summary>Creates a new MutableCloudRef in the specified folder with the specified identifier.</summary>
            /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
            /// <param name="name">The identifier of the MutableCloudRef in the underlying store.</param>
            /// <param name="value">The value to stored.</param>
            static member TryNew : container:string * name:string * value:'T -> ICloud<IMutableCloudRef<'T> option>
            
            /// Gets the current value of a MutableCloudRef.
            static member TryRead : mref:IMutableCloudRef<'T> -> ICloud<'T option>
        
            /// <summary>Try to update the value of a MutableCloudRef. Returns true
            /// if the operation succeeded, or false if the MutableCloudRef was updated (by someone else) since the last time
            /// the it was read.</summary>
            /// <param name="mref">The MutableCloudRef.</param>
            /// <param name="value">The value to stored.</param>
            static member TrySet : mref:IMutableCloudRef<'T> * value:'T -> ICloud<bool option>
        end