namespace Nessos.MBrace.Caching
    
    open System
    open System.IO

    open Nessos.MBrace.Store

    type ICacheStore =
        abstract Create : Folder * File * (Stream -> Async<unit>) -> Async<unit>
        abstract Commit : Folder * File * ?asFile:bool -> Async<unit> 
        abstract Read   : Folder * File -> Async<Stream>
