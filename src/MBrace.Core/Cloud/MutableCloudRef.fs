namespace Nessos.MBrace
    
    open System

    open Nessos.MBrace.Core

    type MutableCloudRef = 
        // loop (possibly forever) 
        static member private spin (expr : Cloud<'T option>, ?interval : int) : Cloud<'T> =
            cloud {
                let! v = expr
                match v with
                | Some v -> return v
                | None -> 
                    match interval with
                    | None -> ()
                    | Some t -> do! Cloud.OfAsync <| Async.Sleep t
                    return! MutableCloudRef.spin expr
            }

        static member New<'T>(container : string, id : string, value : 'T) : Cloud<IMutableCloudRef<'T>> = 
            Cloud.wrapExpr <| NewMutableRefByNameExpr (container, id, value, typeof<'T>)

        static member New<'T>(value : 'T) : Cloud<IMutableCloudRef<'T>> = 
            cloud {
                let! pid = Cloud.GetProcessId()
                let id = Guid.NewGuid()
                return! MutableCloudRef.New(sprintf "process%d" pid, string id, value)
            }

        static member New<'T>(container : string, value : 'T) : Cloud<IMutableCloudRef<'T>> = 
            cloud {
                let id = Guid.NewGuid()
                return! MutableCloudRef.New(container, string id, value)
            }

        static member Read<'T>(mref : IMutableCloudRef<'T>) : Cloud<'T> =
            Cloud.wrapExpr <| ReadMutableRefExpr(mref, typeof<'T>)

        static member Set<'T>(mref : IMutableCloudRef<'T>, value : 'T) : Cloud<bool> =
            Cloud.wrapExpr <| SetMutableRefExpr(mref, value)

        static member Force<'T>(mref : IMutableCloudRef<'T>, value : 'T) : Cloud<unit> =
            Cloud.wrapExpr <| ForceSetMutableRefExpr(mref, value)

        static member Get(container : string) : Cloud<IMutableCloudRef []> =
            Cloud.wrapExpr <| GetMutableRefsByNameExpr(container)

        static member Get<'T>(container : string, id : string) : Cloud<IMutableCloudRef<'T>> =
            Cloud.wrapExpr <| GetMutableRefByNameExpr(container, id, typeof<'T>)

        static member Free(mref : IMutableCloudRef<'T>) : Cloud<unit> =
            Cloud.wrapExpr <| FreeMutableRefExpr(mref)

        static member SpinSet<'T>(mref : IMutableCloudRef<'T>, update : 'T -> 'T, ?interval : int) : Cloud<unit> =
            cloud {
                let ok = ref false
                while not !ok do
                    let! old = MutableCloudRef.spin(MutableCloudRef.TryRead<'T>(mref), ?interval = interval)
                    let! isOk = MutableCloudRef.spin(MutableCloudRef.TrySet(mref, update old), ?interval = interval)
                    ok := isOk
            }

        static member TryNew<'T>(container : string, id : string, value : 'T) : Cloud<IMutableCloudRef<'T> option> = 
            mkTry<StoreException, _> <| MutableCloudRef.New(container, id, value)
        
        static member TryNew<'T>(value : 'T) : Cloud<IMutableCloudRef<'T> option> = 
            mkTry<StoreException, _> <| MutableCloudRef.New(value)

        static member TryNew<'T>(container : string, value : 'T) : Cloud<IMutableCloudRef<'T> option> = 
            mkTry<StoreException, _> <| MutableCloudRef.New(container, value)

        static member TryRead<'T>(mref : IMutableCloudRef<'T>) : Cloud<'T option> =
            mkTry<StoreException, _> <| MutableCloudRef.Read(mref)

        static member TrySet<'T>(mref : IMutableCloudRef<'T>, value : 'T) : Cloud<bool option> =
            mkTry<StoreException, _> <| MutableCloudRef.Set(mref, value)

        static member TryForce<'T>(mref : IMutableCloudRef<'T>, value : 'T) : Cloud<unit option> =
            mkTry<StoreException, _> <| MutableCloudRef.Force(mref, value)

        static member TryGet(container : string) : Cloud<(IMutableCloudRef []) option> =
            mkTry<StoreException, _> <| MutableCloudRef.Get(container)

        static member TryGet<'T>(container : string, id : string) : Cloud<IMutableCloudRef<'T> option> =
            mkTry<StoreException, _> <| MutableCloudRef.Get(container, id)

        static member TryFree(mref : IMutableCloudRef<'T>) : Cloud<unit option> =
            mkTry<StoreException, _> <| MutableCloudRef.Free(mref)