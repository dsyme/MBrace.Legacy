namespace Nessos.MBrace

    [<AutoOpen>]
    module CloudRefModule =

        type CloudRef = 
            static member New<'T>(container : string, value : 'T) : ICloud<ICloudRef<'T>> = 
                wrapCloudExpr <| NewRefByNameExpr (container, value, typeof<'T>)

            static member Get(container : string) : ICloud<ICloudRef []> = 
                wrapCloudExpr <| GetRefsByNameExpr (container)

            static member Get<'T>(container : string, id : string) : ICloud<ICloudRef<'T>> = 
                wrapCloudExpr <| GetRefByNameExpr (container, id, typeof<'T>)

            static member New<'T>( value : 'T) : ICloud<ICloudRef<'T>> = 
                cloud {
                    let! pid = Cloud.GetProcessId()
                    return! CloudRef.New<'T>(sprintf "process%d" pid, value)
                }

            static member Read<'T>(cref : ICloudRef<'T>) : ICloud<'T> =
                cloud { return cref.Value }

            static member TryGet(container : string) : ICloud<ICloudRef [] option> = 
                mkTry<StoreException,_> <| CloudRef.Get(container)

            static member TryNew<'T>(container : string, value : 'T) : ICloud<ICloudRef<'T> option> =
                mkTry<StoreException, _> <| CloudRef.New<'T>(container, value)

            static member TryGet<'T>(container : string, id : string) : ICloud<ICloudRef<'T> option> =
                mkTry<StoreException, _> <| CloudRef.Get<'T>(container, id)

        [<Cloud>]
        let newRef value = CloudRef.New value 

        let (|CloudRef|) (cloudRef : ICloudRef<'T>) = cloudRef.Value
