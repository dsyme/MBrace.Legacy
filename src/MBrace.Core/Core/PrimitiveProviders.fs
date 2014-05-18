namespace Nessos.MBrace.Core
    
    open System
    open System.Collections
    open System.IO

    open Nessos.MBrace

    /// Defines a provider abstraction for cloud refs
    type ICloudRefProvider =
        
        /// Defines a new cloud ref instance 
        abstract Create : container:string * id:string * value:'T -> Async<ICloudRef<'T>>
        
        /// Defines a new cloud ref instance
        abstract Create : container:string * id:string * valueType:Type * value:obj -> Async<ICloudRef>

        /// Gets an already existing cloud ref
        abstract GetExisting : container:string * id:string -> Async<ICloudRef>

        /// Receive all cloud ref's defined within the given container
        abstract GetContainedRefs : container:string -> Async<ICloudRef []>

    /// Defines a provider abstraction for mutable cloud refs
    type IMutableCloudRefProvider =
        
        /// Defines a new mutable cloud ref instance
        abstract Create : container:string * id:string * value:'T -> Async<IMutableCloudRef<'T>>

        // Defines a new mutable cloud ref instance
        abstract Create : container:string * id:string * valueType:Type * value:obj  -> Async<IMutableCloudRef>

        /// Defines an existing mutable cloud ref instance
        abstract GetExisting : container:string * id:string -> Async<IMutableCloudRef>

        /// Receive all cloud ref's defined within the given container
        abstract GetContainedRefs : container:string -> Async<IMutableCloudRef []>

    /// Defines a provider abstraction for cloud sequences
    type ICloudSeqProvider =
        
        /// Defines a new cloud seq instance
        abstract Create : container:string * id:string * values:seq<'T> -> Async<ICloudSeq<'T>>

        /// Defines a new untyped cloud seq instance
        abstract Create : container:string * id:string * seqType:Type * values:IEnumerable -> Async<ICloudSeq>

        /// Defines an existing cloud seq instance
        abstract GetExisting : container:string * id:string  -> Async<ICloudSeq>

        /// Receive all cloud seq's defined within the given container
        abstract GetContainedSeqs : container:string -> Async<ICloudSeq []>

    /// Defines a provider abstraction for cloud files
    type ICloudFileProvider =
        
        /// Defines a new cloud file
        abstract Create : container:string * id:string * writer:(Stream -> Async<unit>) -> Async<ICloudFile>

        /// Defines an existing cloud file
        abstract GetExisting : container:string * id:string -> Async<ICloudFile>

        /// Get all cloud files that exist in specified container
        abstract GetContainedFiles : container:string -> Async<ICloudFile []>

    /// Defines an object cloning abstraction
    type IObjectCloner =
        abstract Clone : 'T -> 'T

    /// configuration used by the interpreter
    type CoreConfiguration =
        {
            CloudSeqProvider        : ICloudSeqProvider
            CloudRefProvider        : ICloudRefProvider
            CloudFileProvider       : ICloudFileProvider
            MutableCloudRefProvider : IMutableCloudRefProvider
            CloudLogger             : ICloudLogger
            Cloner                  : IObjectCloner
        }