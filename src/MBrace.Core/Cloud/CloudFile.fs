namespace Nessos.MBrace
    
    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.MBrace.Core

    type CloudFile = 

        static member Create(container : string, name : string, serialize : (Stream -> Async<unit>)) : Cloud<ICloudFile> =
            Cloud.wrapExpr <| NewCloudFile(container, name, serialize)

        static member Create(container : string, serialize : (Stream -> Async<unit>)) : Cloud<ICloudFile> =
            cloud {
                return! CloudFile.Create(container, Guid.NewGuid().ToString(), serialize)
            }

        static member Create(serialize : (Stream -> Async<unit>)) : Cloud<ICloudFile> =
            cloud {
                let! pid = Cloud.GetProcessId()
                return! CloudFile.Create(sprintf "process%d" pid, serialize)
            }

        static member Read(cloudFile : ICloudFile, deserialize : (Stream -> Async<'Result>)) : Cloud<'Result> =
            let deserialize stream = async { let! o = deserialize stream in return o :> obj }
            Cloud.wrapExpr <| ReadCloudFile(cloudFile, deserialize, typeof<'Result>)

        static member Get(container : string, name : string) : Cloud<ICloudFile> =
            Cloud.wrapExpr <| GetCloudFile(container, name)

        static member Get(container : string) : Cloud<ICloudFile []> =
            Cloud.wrapExpr <| GetCloudFiles(container)

        static member TryCreate(container : string, name : string, serialize : (Stream -> Async<unit>)) : Cloud<ICloudFile option> =
            mkTry<StoreException,_> <| CloudFile.Create(container, name, serialize)

        static member TryGet(container : string, name : string) : Cloud<ICloudFile option> =
            mkTry<StoreException,_> <| CloudFile.Get(container,name)

        static member TryGet(container : string) : Cloud<ICloudFile [] option> =
            mkTry<StoreException,_> <| CloudFile.Get(container)

        static member TryRead(cloudFile : ICloudFile, deserialize : (Stream -> Async<'Result>)) : Cloud<'Result option> =
            mkTry<StoreException,_> <| CloudFile.Read(cloudFile, deserialize)