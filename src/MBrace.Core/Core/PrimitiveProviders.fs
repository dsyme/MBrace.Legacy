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

    /// Defines a provider abstraction for cloud files
    type ICloudFileProvider =
        
        /// Defines a new cloud file
        abstract CreateNew : container:string * id:string * writer:(Stream -> Async<unit>) -> Async<ICloudFile>

        /// Defines an existing cloud file
        abstract CreateExisting : container:string * id:string -> Async<ICloudFile>

        /// Reads from an existing cloud file
        abstract Read : file:ICloudFile * reader:(Stream -> Async<'T>) -> Async<'T>
        
        /// Deserialize a sequence from a given cloud file
        abstract ReadAsSequence: file:ICloudFile * elementReader:(Stream -> Async<obj>) * seqType:Type  -> Async<IEnumerable>

        /// Delete a cloud file
        abstract Delete: file:ICloudFile -> Async<unit>

        /// Get all cloud files that exist in specified container
        abstract GetContainedFiles : container:string -> Async<ICloudFile []>

    /// Defines an object cloning abstraction
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