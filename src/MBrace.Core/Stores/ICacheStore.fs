namespace Nessos.MBrace.Caching
    open System
    open System.IO
    open System.Text
    open System.Threading
    open System.Collections.Concurrent
    open System.Runtime.Serialization
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Store
    //open Nessos.MBrace.Store.Registry

    type ICacheStore =
        abstract Create : Folder * File * (Stream -> Async<unit>) -> Async<unit>
        abstract Commit : Folder * File * ?asFile:bool -> Async<unit> 
        abstract Read   : Folder * File -> Async<Stream>
