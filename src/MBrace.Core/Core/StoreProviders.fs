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
        abstract GetRefs : Container ->Async<ICloudRef []>
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
            LogStore              : ILogStore
            Cloner                : IObjectCloner
        }



// do we need this?
//    /////////////////////////////////////////////////////////////////////////////////////////////////////////////
//    // Special PersistableCloudRef - special treatment - access to store 
//    and IPersistableCloudRef =
//        inherit ICloudRef
//        abstract Container : string
//    and IPersistableCloudRef<'T> =
//        inherit IPersistableCloudRef
//        inherit ICloudRef<'T>


// dead code, commented out

//    type IMemoryCache = 
//        abstract TryFind        : string -> obj option
//        abstract Get            : string -> obj
//        abstract ContainsKey    : string -> bool
//        abstract Set            : string * obj -> unit
//        abstract Delete         : string -> unit
//    type ICacheStore =
//        abstract Create : Folder * File * (Stream -> Async<unit>) -> Async<unit>
//        abstract Commit : Folder * File * ?asFile:bool -> Async<unit> 
//        abstract Read   : Folder * File -> Async<Stream>