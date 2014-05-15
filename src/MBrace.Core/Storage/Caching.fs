namespace Nessos.MBrace.Core

    open System.IO
    open System.Collections.Generic

    type ILocalObjectCache = IDictionary<string, obj>
    
    type ILocalStoreCache =
        abstract Copy : origin:ICloudFileSystem * id:string -> Async<unit>
        abstract Read : origin:ICloudFileSystem * id:string * reader:(Stream -> Async<'T>) -> Async<'T>