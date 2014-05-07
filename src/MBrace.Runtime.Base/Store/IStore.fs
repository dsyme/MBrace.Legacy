namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO

    // TODO : move to Runtime.Base

    type Folder = string
    type File = string
    type Tag = string

    type StoreId = StoreId of byte []

    type IStore =

        abstract Name : string
        // Universal resource identifier
        abstract UUID : string
        
        // Immutable access
        abstract Create             : Folder * File * (Stream -> Async<unit>) * ?asFile:bool -> Async<unit>
        abstract Read               : Folder * File                         -> Async<Stream>
        abstract CopyTo             : Folder * File * Stream                -> Async<unit>
        abstract CopyFrom           : Folder * File * Stream * ?asFile:bool -> Async<unit>

        // General purpose methods
        abstract GetFiles           : Folder        -> Async<File []>
        abstract GetFolders         : unit          -> Async<Folder []>
        abstract Exists             : Folder        -> Async<bool>
        abstract Exists             : Folder * File -> Async<bool>
        abstract Delete             : Folder        -> Async<unit>
        abstract Delete             : Folder * File -> Async<unit>

        // Mutable access
        abstract CreateMutable      : Folder * File * (Stream -> Async<unit>) -> Async<Tag>
        abstract ReadMutable        : Folder * File                           -> Async<Stream * Tag>
        abstract UpdateMutable      : Folder * File * (Stream -> Async<unit>) * Tag -> Async<bool * Tag>
        abstract ForceUpdateMutable : Folder * File * (Stream -> Async<unit>) -> Async<Tag>

    type IStoreFactory =
        abstract CreateStoreFromConnectionString: connectionString : string -> IStore
