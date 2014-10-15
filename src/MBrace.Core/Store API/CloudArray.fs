namespace Nessos.MBrace
    
    open System.Collections
    
    open Nessos.MBrace.CloudExpr

    /// Provides methods to create and CloudArrays.
    type CloudArray =

        /// <summary>
        /// Creates a new CloudArray in the specified container.
        /// </summary>
        /// <param name="container">Containing folder in the underlying store.</param>
        /// <param name="values">Values to be stored.</param>
        static member New<'T>(container : string, values : seq<'T>) : Cloud<ICloudArray<'T>> =
            CloudExpr.wrap <| NewCloudArray (container, values :> IEnumerable, typeof<'T>)

        /// <summary>
        /// Creates a new CloudArray in the specified container.
        /// </summary>
        /// <param name="container">Containing folder in the underlying store.</param>
        /// <param name="values">Values to be stored.</param>
        static member New<'T>(values : seq<'T>) : Cloud<ICloudArray<'T>> = 
            cloud {
                let! pid = Cloud.GetProcessId()
                return! CloudArray.New<'T>(sprintf "process%d" pid, values)
            }

        /// <summary>
        ///     Returns a collection of all CloudArrays contained in the specified container.
        /// </summary>
        /// <param name="container">underlying store container.</param>
        static member Enumerate(container : string) : Cloud<ICloudArray []> =
            CloudExpr.wrap <| GetCloudArrays (container)

        /// <summary>
        ///     Returns a CloudArray that already exists in the specified container with given name.
        /// </summary>
        /// <param name="container">Containing folder of the CloudArray in the underlying store.</param>
        /// <param name="id">ClouArray id.</param>
        static member Get<'T>(container : string, id : string) : Cloud<ICloudArray<'T>> =
            CloudExpr.wrap <| GetCloudArray (container, id, typeof<'T>)

        /// <summary>
        ///     Try returning a CloudArray that already exists in the specified container with given name.
        /// </summary>
        /// <param name="container">Containing folder of the CloudArray in the underlying store.</param>
        /// <param name="id">CloudArray id.</param>
        static member TryGet<'T>(container : string, id : string) : Cloud<ICloudArray<'T> option> =
            mkTry<StoreException, _> <| CloudArray.Get<'T>(container, id)