namespace Nessos.MBrace

    open Nessos.MBrace.CloudExpr

    type CloudRef = 
        static member New<'T>(container : string, value : 'T) : Cloud<ICloudRef<'T>> = 
            Cloud.wrapExpr <| NewRefByNameExpr (container, value, typeof<'T>)

        static member Get(container : string) : Cloud<ICloudRef []> = 
            Cloud.wrapExpr <| GetRefsByNameExpr (container)

        static member Get<'T>(container : string, id : string) : Cloud<ICloudRef<'T>> = 
            Cloud.wrapExpr <| GetRefByNameExpr (container, id, typeof<'T>)

        static member New<'T>( value : 'T) : Cloud<ICloudRef<'T>> = 
            cloud {
                let! pid = Cloud.GetProcessId()
                return! CloudRef.New<'T>(sprintf "process%d" pid, value)
            }

        static member Read<'T>(cref : ICloudRef<'T>) : Cloud<'T> =
            cloud { return cref.Value }

        static member TryGet(container : string) : Cloud<ICloudRef [] option> = 
            mkTry<StoreException,_> <| CloudRef.Get(container)

        static member TryNew<'T>(container : string, value : 'T) : Cloud<ICloudRef<'T> option> =
            mkTry<StoreException, _> <| CloudRef.New<'T>(container, value)

        static member TryGet<'T>(container : string, id : string) : Cloud<ICloudRef<'T> option> =
            mkTry<StoreException, _> <| CloudRef.Get<'T>(container, id)

