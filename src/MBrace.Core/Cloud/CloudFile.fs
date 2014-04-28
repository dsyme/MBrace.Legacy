namespace Nessos.MBrace
    
    open System
    open System.IO

    type CloudFile = 

        static member Create(container : string, name : string, serialize : (Stream -> Async<unit>)) : ICloud<ICloudFile> =
            wrapCloudExpr <| NewCloudFile(container, name, serialize)

        static member Create(container : string, serialize : (Stream -> Async<unit>)) : ICloud<ICloudFile> =
            cloud {
                return! CloudFile.Create(container, Guid.NewGuid().ToString(), serialize)
            }

        static member Create(serialize : (Stream -> Async<unit>)) : ICloud<ICloudFile> =
            cloud {
                let! pid = Cloud.GetProcessId()
                return! CloudFile.Create(sprintf "process%d" pid, serialize)
            }

        static member Read(cloudFile : ICloudFile, deserialize : (Stream -> Async<'Result>)) : ICloud<'Result> =
            let deserialize stream = async { let! o = deserialize stream in return o :> obj }
            wrapCloudExpr <| ReadCloudFile(cloudFile, deserialize, typeof<'Result>)

        static member ReadSeq(cloudFile:ICloudFile, deserialize :(Stream -> Async<seq<'T>>)) : ICloud<seq<'T>> =
            let deserialize stream = async { let! o = deserialize stream in return o :> obj }
            wrapCloudExpr <| ReadCloudFileAsSeq(cloudFile, deserialize, typeof<'T>)

        static member Get(container : string, name : string) : ICloud<ICloudFile> =
            wrapCloudExpr <| GetCloudFile(container, name)

        static member Get(container : string) : ICloud<ICloudFile []> =
            wrapCloudExpr <| GetCloudFiles(container)

        static member TryCreate(container : string, name : string, serialize : (Stream -> Async<unit>)) : ICloud<ICloudFile option> =
            mkTry<StoreException,_> <| CloudFile.Create(container, name, serialize)

        static member TryGet(container : string, name : string) : ICloud<ICloudFile option> =
            mkTry<StoreException,_> <| CloudFile.Get(container,name)

        static member TryGet(container : string) : ICloud<ICloudFile [] option> =
            mkTry<StoreException,_> <| CloudFile.Get(container)

        static member TryRead(cloudFile : ICloudFile, deserialize : (Stream -> Async<'Result>)) : ICloud<'Result option> =
            mkTry<StoreException,_> <| CloudFile.Read(cloudFile, deserialize)

        static member TryReadSeq(cloudFile:ICloudFile, deserialize :(Stream -> Async<seq<'T>>)) : ICloud<seq<'T> option> =
            mkTry<StoreException,_> <| CloudFile.ReadSeq(cloudFile, deserialize)
