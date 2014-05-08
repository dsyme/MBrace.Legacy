namespace Nessos.MBrace.Core
    
    open System
    open System.Collections
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

    /// Defines a provider abstraction for cloud sequences
    type ICloudSeqProvider =
        
        /// Defines a new cloud seq instance
        abstract CreateNew : container:string * id:string * values:seq<'T> -> Async<ICloudSeq<'T>>

        /// Defines a new untyped cloud seq instance
        abstract CreateNewUntyped : container:string * id:string * values:IEnumerable * ty:Type -> Async<ICloudSeq>

        /// Defines an existing cloud seq instance
        abstract CreateExisting : container:string * id:string  -> Async<ICloudSeq>

        /// Receive all cloud seq's defined within the given container
        abstract GetContainedSeqs : container:string -> Async<ICloudSeq []>
        
        /// Deletes a cloud sequence
        abstract Delete : ICloudSeq -> Async<unit>

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