namespace Nessos.MBrace.Core
    
    open System
    open System.IO

    open Nessos.MBrace

    /// Defines a provider abstraction for cloud refs
    type ICloudRefProvider =
        
        /// Defines a new cloud ref instance 
        abstract CreateNew : container:string * id:string * value:'T -> Async<ICloudRef<'T>>
        
        /// Defines a new cloud ref instance
        abstract CreateNewUntyped : container:string * id:string * value:obj * ty:Type -> Async<ICloudRef>

        /// Defines an already existing cloud ref
        abstract CreateExisting : container:string * id:string -> Async<ICloudRef>

        /// Receives the value of given cloud ref
        abstract Dereference : ICloudRef -> Async<obj>

        /// Deletes a cloud ref
        abstract Delete : ICloudRef -> Async<unit>

        /// Receive all cloud ref's defined within the given container
        abstract GetContainedRefs : container:string -> Async<ICloudRef []>

    /// Defines a provider abstraction for mutable cloud refs
    type IMutableCloudRefProvider =
        
        /// Defines a new mutable cloud ref instance
        abstract CreateNew : container:string * id:string * value:'T -> Async<IMutableCloudRef<'T>>

        // Defines a new mutable cloud ref instance
        abstract CreateNewUntyped : container:string * id:string * value:obj * ty:Type -> Async<IMutableCloudRef>

        /// Defines an existing mutable cloud ref instance
        abstract CreateExisting : container:string * id:string -> Async<IMutableCloudRef>

        /// Receives the value of given cloud ref
        abstract Dereference : IMutableCloudRef -> Async<obj>
        
        /// Force update a mutable cloud ref
        abstract ForceUpdate : IMutableCloudRef * value:obj -> Async<unit>

        /// Try update a mutable cloud ref
        abstract TryUpdate : IMutableCloudRef * value:obj -> Async<bool>

        /// Deletes a mutable cloud ref
        abstract Delete : IMutableCloudRef -> Async<unit>

        /// Receive all cloud ref's defined within the given container
        abstract GetContainedRefs : container:string -> Async<IMutableCloudRef []>

    type ICloudSeqProvider =
        abstract GetSeq : Container * Id  -> Async<ICloudSeq>
        abstract Create : System.Collections.IEnumerable * string * string * System.Type -> Async<ICloudSeq>
        abstract Delete : ICloudSeq -> Async<unit>
        abstract GetSeqs : Container -> Async<ICloudSeq []>

    type ICloudFileProvider =
        abstract Create   : Container * Id * (Stream -> Async<unit>)    -> Async<ICloudFile   >
        abstract Read     : ICloudFile * (Stream -> Async<obj>)         -> Async<obj          >
        abstract ReadAsSeq: ICloudFile * (Stream -> Async<obj>) * Type  -> Async<obj          > // TODO : Change return type to IEnumerator
        abstract GetFiles : Container                                   -> Async<ICloudFile []>
        abstract GetFile  : Container  * Id                             -> Async<ICloudFile   >
        abstract Delete   : ICloudFile                                  -> Async<unit         >

    type IObjectCloner =
        abstract Clone : 'T -> 'T


    type CoreConfiguration =
        {
            CloudSeqProvider        : ICloudSeqProvider
            CloudRefProvider        : ICloudRefProvider
            CloudFileProvider       : ICloudFileProvider
            MutableCloudRefProvider : IMutableCloudRefProvider
            CloudLogger             : ICloudLogger
            Cloner                  : IObjectCloner
        }