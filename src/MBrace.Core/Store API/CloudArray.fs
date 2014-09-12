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