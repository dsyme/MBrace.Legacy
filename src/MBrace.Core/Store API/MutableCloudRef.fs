namespace Nessos.MBrace
    
    open System

    open Nessos.MBrace.CloudExpr

    /// Provides methods to create, read, update and delete MutableCloudRefs.
    type MutableCloudRef =

        /// <summary>
        ///     Creates a new MutableCloudRef in the specified container.
        /// </summary>
        /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
        /// <param name="value">The value to be stored.</param>
        static member New<'T>(container : string, id : string, value : 'T) : Cloud<IMutableCloudRef<'T>> = 
            CloudExpr.wrap <| NewMutableRefByNameExpr (container, id, value, typeof<'T>)

        /// <summary>
        ///     Creates a new MutableCloudRef in the default process container.
        /// </summary>
        /// <param name="value">The value to be stored.</param>
        static member New<'T>(value : 'T) : Cloud<IMutableCloudRef<'T>> = 
            cloud {
                let! pid = Cloud.GetProcessId()
                let id = Guid.NewGuid()
                // TODO : defaut process container should be specified by runtime,
                // not core library
                return! MutableCloudRef.New(sprintf "process%d" pid, string id, value)
            }

        /// <summary>
        ///     Creates a new MutableCloudRef in the specified folder with the specified identifier.
        /// </summary>
        /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
        /// <param name="name">The identifier of the MutableCloudRef in the underlying store.</param>
        /// <param name="value">The value to stored.</param>
        static member New<'T>(container : string, value : 'T) : Cloud<IMutableCloudRef<'T>> = 
            cloud {
                let id = Guid.NewGuid()
                return! MutableCloudRef.New(container, string id, value)
            }

        /// <summary>Gets the current value of a MutableCloudRef.</summary>
        /// <param name="mref">The MutableCloudRef.</param>
        static member Read<'T>(mref : IMutableCloudRef<'T>) : Cloud<'T> =
            CloudExpr.wrap <| ReadMutableRefExpr(mref, typeof<'T>)

        /// <summary>
        ///     Try to update the value of a MutableCloudRef. Returns true
        ///     if the operation succeeded, or false if the MutableCloudRef 
        ///     was modified (by someone else) since the last time it was read.
        /// </summary>
        /// <param name="mref">The MutableCloudRef.</param>
        /// <param name="value">The value to stored.</param>
        /// <returnType>Operation success</returnType>
        static member Set<'T>(mref : IMutableCloudRef<'T>, value : 'T) : Cloud<bool> =
            CloudExpr.wrap <| SetMutableRefExpr(mref, value)

        /// <summary>
        ///     Force an update to the value of a MutableCloudRef. 
        /// </summary>
        /// <param name="mref">The MutableCloudRef.</param>
        /// <param name="value">The value to stored.</param>
        static member Force<'T>(mref : IMutableCloudRef<'T>, value : 'T) : Cloud<unit> =
            CloudExpr.wrap <| ForceSetMutableRefExpr(mref, value)

        /// <summary>
        ///     Returns an array of all the MutableCloudRefs within the container.
        /// </summary>
        /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
        static member Get(container : string) : Cloud<IMutableCloudRef []> =
            CloudExpr.wrap <| GetMutableRefsByNameExpr(container)

        /// <summary>
        ///     Returns the MutableCloudRef with the specified container and id combination.
        /// </summary>
        /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
        /// <param name="name">The identifier of the MutableCloudRef in the underlying store.</param>
        static member Get<'T>(container : string, id : string) : Cloud<IMutableCloudRef<'T>> =
            CloudExpr.wrap <| GetMutableRefByNameExpr(container, id, typeof<'T>)

        /// <summary>
        ///     Deletes the MutableCloudRef from the underlying store.
        /// </summary>
        /// <param name="mref">Mutable CloudRef to be deleted.</param>
        static member Free(mref : IMutableCloudRef<'T>) : Cloud<unit> =
            CloudExpr.wrap <| FreeMutableRefExpr(mref)

        /// <summary>
        ///     Try returning the MutableCloudRef with the specified container and id combination, if it exists.
        /// </summary>
        /// <param name="container">The container (folder) of the MutableCloudRef in the underlying store.</param>
        /// <param name="name">The identifier of the MutableCloudRef in the underlying store.</param>
        static member TryGet<'T>(container : string, id : string) : Cloud<IMutableCloudRef<'T> option> =
            mkTry<StoreException, _> <| MutableCloudRef.Get(container, id)

        /// <summary>
        ///     Updates the MutableCloudRef using the update function.
        ///     This method will return when the update is successful.
        /// </summary>
        /// <param name="mref">The MutableCloudRef to be updated.</param>
        /// <param name="update">
        ///     A function that takes the current value of the MutableCloudRef and
        ///     returns the new value to be stored.
        /// </param>
        /// <param name="interval">The interval, in milliseconds, to sleep between the spin calls.</param>
        static member SpinSet<'T>(mref : IMutableCloudRef<'T>, update : 'T -> 'T, ?interval : int) : Cloud<unit> = 
            let rec spin retries interval = cloud {
                let! current = MutableCloudRef.Read mref
                let! isSuccess = MutableCloudRef.Set(mref, update current)
                if isSuccess then return ()
                else
                    do! Cloud.Sleep interval
                    return! spin (retries + 1) interval
            }

            spin 0 (defaultArg interval 0)