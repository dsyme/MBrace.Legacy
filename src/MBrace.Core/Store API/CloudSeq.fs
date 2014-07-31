namespace Nessos.MBrace
    
    open System
    open System.Collections
    
    open Nessos.MBrace.CloudExpr

    /// Provides methods to create and read CloudRefs.
    type CloudSeq =

        /// <summary>
        ///     Creates a new CloudSeq in the specified container.
        /// </summary>
        /// <param name="container">The container (folder) of the CloudSeq in the underlying store.</param>
        /// <param name="values">The values to be stored.</param>
        static member New<'T>(container : string, values : seq<'T>) : Cloud<ICloudSeq<'T>> =
            CloudExpr.wrap <| NewCloudSeqByNameExpr (container, values :> IEnumerable, typeof<'T>)

        /// <summary>
        ///     Creates a new CloudRef in the default container.
        /// </summary>
        /// <param name="values">The values to be stored.</param>
        static member New<'T>(values : seq<'T>) : Cloud<ICloudSeq<'T>> = 
            cloud {
                // TODO : container name should be specified by runtime,
                // not the Core library
                let! pid = Cloud.GetProcessId()
                return! CloudSeq.New<'T>(sprintf "process%d" pid, values)
            }

        /// <summary>
        ///     Enumerates the values stored in given CloudSeq.
        /// </summary>
        /// <param name="sequence">The CloudSeq to dereference.</param>
        static member Read<'T>(sequence : ICloudSeq<'T>) : Cloud<seq<'T>> =
            cloud { return sequence :> _ }

        /// Returns an array of all CloudSeqs contained in the specified folder.
        static member Get(container : string) : Cloud<ICloudSeq []> =
            CloudExpr.wrap <| GetCloudSeqsByNameExpr (container)

        /// Returns a CloudSeq that already exists in the specified container with given name.
        static member Get<'T>(container : string, id : string) : Cloud<ICloudSeq<'T>> =
            CloudExpr.wrap <| GetCloudSeqByNameExpr (container, id, typeof<'T>)

        /// Try returning a CloudSeq that already exists in the specified container with given name.
        static member TryGet<'T>(container : string, id : string) : Cloud<ICloudSeq<'T> option> =
            mkTry<StoreException, _> <| CloudSeq.Get<'T>(container, id)