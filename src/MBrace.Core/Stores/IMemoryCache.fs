namespace Nessos.MBrace.Caching

    open System
    open System.IO
    open System.Text
    open System.Runtime.Caching
    open System.Runtime.Serialization

    open Nessos.FsPickler

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Store

    type IMemoryCache = 
        abstract TryFind        : string -> obj option
        abstract Get            : string -> obj
        abstract ContainsKey    : string -> bool
        abstract Set            : string * obj -> unit
        abstract Delete         : string -> unit
