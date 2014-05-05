namespace Nessos.MBrace.Caching

    open System
    open System.IO
    open System.Text

    type IMemoryCache = 
        abstract TryFind        : string -> obj option
        abstract Get            : string -> obj
        abstract ContainsKey    : string -> bool
        abstract Set            : string * obj -> unit
        abstract Delete         : string -> unit
