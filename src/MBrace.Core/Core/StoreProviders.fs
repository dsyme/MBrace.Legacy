namespace Nessos.MBrace.Core
    
    open System
    open System.IO

    open Nessos.MBrace

    // TODO : clean up & document

    type ICloudFileProvider =
        abstract Create   : Container * Id * (Stream -> Async<unit>)    -> Async<ICloudFile   >
        abstract Read     : ICloudFile * (Stream -> Async<obj>)         -> Async<obj          >
        abstract ReadAsSeq: ICloudFile * (Stream -> Async<obj>) * Type  -> Async<obj          > // TODO : Change return type to IEnumerator
        abstract GetFiles : Container                                   -> Async<ICloudFile []>
        abstract GetFile  : Container  * Id                             -> Async<ICloudFile   >
        abstract Delete   : ICloudFile                                  -> Async<unit         >

    type ICloudRefProvider =
        abstract Create : Container * Id * obj * System.Type -> Async<ICloudRef>
        abstract Delete : Container * Id -> Async<unit>
        abstract GetRefs : Container -> Async<ICloudRef []>
        abstract GetRef : Container * Id ->Async<ICloudRef>
        abstract Read : ICloudRef -> Async<obj>

    type ICloudSeqProvider =
        abstract GetSeq : Container * Id  -> Async<ICloudSeq>
        abstract Create : System.Collections.IEnumerable * string * string * System.Type -> Async<ICloudSeq>
        abstract Delete : ICloudSeq -> Async<unit>
        abstract GetSeqs : Container -> Async<ICloudSeq []>

    type IMutableCloudRefProvider =
        abstract Create : Container * Id * obj * System.Type -> Async<IMutableCloudRef>
        abstract Delete : IMutableCloudRef -> Async<unit>
        abstract Read : IMutableCloudRef -> Async<obj>
        abstract GetRef : Container * Id -> Async<IMutableCloudRef>
        abstract ForceUpdate : IMutableCloudRef * obj -> Async<unit>
        abstract Update : IMutableCloudRef * obj -> Async<bool>
        abstract GetRefs : Container -> Async<IMutableCloudRef []>


    type IObjectCloner =
        abstract Clone : 'T -> 'T


    type CoreConfiguration =
        {
            CloudSeqStore         : ICloudSeqProvider
            CloudRefStore         : ICloudRefProvider
            CloudFileStore        : ICloudFileProvider
            MutableCloudRefStore  : IMutableCloudRefProvider
            CloudLogger           : ICloudLogger
            Cloner                : IObjectCloner
        }