namespace Nessos.MBrace

    open Nessos.MBrace.CloudExpr

    /// Provides methods to create and read CloudRefs.
    type CloudRef = 

        /// <summary>
        ///     Creates a new CloudRef in the process defined container.
        /// </summary>
        /// <param name="value">The value to be stored.</param>
        static member New<'T>(value : 'T) : Cloud<ICloudRef<'T>> = 
            cloud {
                let! pid = Cloud.GetProcessId()
                return! CloudRef.New<'T>(sprintf "process%d" pid, value)
            }

        /// <summary>
        ///     Creates a new CloudRef in the specified container.
        /// </summary>
        /// <param name="container">Containing folder of the CloudRef in the underlying store.</param>
        /// <param name="value">The value to be stored.</param>
        static member New<'T>(container : string, value : 'T) : Cloud<ICloudRef<'T>> = 
            CloudExpr.wrap <| NewRefByNameExpr (container, value, typeof<'T>)

        /// <summary>
        ///     Returns a collection of all CloudRefs contained in the specified folder.
        /// </summary>
        /// <param name="container">Containing folder of the CloudRef in the underlying store.</param>
        static member Enumerate(container : string) : Cloud<ICloudRef []> = 
            CloudExpr.wrap <| GetRefsByNameExpr (container)

        /// <summary>
        ///     Returns a CloudRef that already exists in the specified container with the given identifier.
        /// </summary>
        /// <param name="container">Containing folder of the CloudRef in the underlying store.</param>
        /// <param name="id">CloudRef Id.</param>
        static member Get<'T>(container : string, id : string) : Cloud<ICloudRef<'T>> = 
            CloudExpr.wrap <| GetRefByNameExpr (container, id, typeof<'T>)

        /// <summary>
        ///     Try returning a CloudRef that already using given identifiers.
        /// </summary>
        /// <param name="container">Containing folder of the CloudRef in the underlying store.</param>
        /// <param name="id">CloudRef id.</param>
        static member TryGet<'T>(container : string, id : string) : Cloud<ICloudRef<'T> option> =
            mkTry<StoreException, _> <| CloudRef.Get<'T>(container, id)

        /// <summary>
        ///     Dereferences a CloudRef
        /// </summary>
        /// <param name="cref">Input CloudRef.</param>
        static member Read<'T>(cref : ICloudRef<'T>) : Cloud<'T> =
            cloud { return cref.Value }