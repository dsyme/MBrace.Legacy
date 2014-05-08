namespace Nessos.MBrace.Core
    
    open System
    open System.IO

    open Nessos.MBrace

    /// Defines a provider abstraction for Cloud refs
    type ICloudRefProvider =
        
        /// Defines a new cloud ref instance 
        abstract CreateNew : container:string * id:string * value:'T -> Async<ICloudRef<'T>>
        
        /// Defines a new cloud ref instance
        abstract CreateNewUntyped : container:string * id:string * value:obj * ty:Type -> Async<ICloudRef>

        /// Defines an already existing cloud ref
        abstract CreateExisting : Container * Id -> Async<ICloudRef>

        /// Deletes a cloud ref in the given location
        abstract Delete : ICloudRef -> Async<unit>

        /// Receive all cloud ref's defined within the given container
        abstract GetContainedRefs : container:string -> Async<ICloudRef []>

    type IMutableCloudRefProvider =
        abstract Create : Container * Id * obj * System.Type -> Async<IMutableCloudRef>
        abstract Delete : IMutableCloudRef -> Async<unit>
        abstract Read : IMutableCloudRef -> Async<obj>
        abstract GetRef : Container * Id -> Async<IMutableCloudRef>
        abstract ForceUpdate : IMutableCloudRef * obj -> Async<unit>
        abstract Update : IMutableCloudRef * obj -> Async<bool>
        abstract GetRefs : Container -> Async<IMutableCloudRef []>

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