namespace Nessos.MBrace
    
    open System
    
    open Nessos.MBrace.CloudExpr

    type CloudSeq =
        static member New<'T>(container : string, values : seq<'T>) : Cloud<ICloudSeq<'T>> =
            Cloud.wrapExpr <| NewCloudSeqByNameExpr (container, values :> System.Collections.IEnumerable, typeof<'T>)

        static member New<'T>(values : seq<'T>) : Cloud<ICloudSeq<'T>> = 
            cloud {
                let! pid = Cloud.GetProcessId()
                return! CloudSeq.New<'T>(sprintf "process%d" pid, values)
            }

        static member Read<'T>(sequence : ICloudSeq<'T>) : Cloud<seq<'T>> =
            cloud { return sequence :> _ }

        static member Get(container : string) : Cloud<ICloudSeq []> =
            Cloud.wrapExpr <| GetCloudSeqsByNameExpr (container)

        static member Get<'T>(container : string, id : string) : Cloud<ICloudSeq<'T>> =
            Cloud.wrapExpr <| GetCloudSeqByNameExpr (container, id, typeof<'T>)

        static member TryNew<'T>(container : string, values : seq<'T>) : Cloud<ICloudSeq<'T> option> =
            mkTry<StoreException, _> <| CloudSeq.New(container, values)

        static member TryGet(container : string) : Cloud<ICloudSeq [] option> =
            mkTry<StoreException, _> <| CloudSeq.Get(container)

        static member TryGet<'T>(container : string, id : string) : Cloud<ICloudSeq<'T> option> =
            mkTry<StoreException, _> <| CloudSeq.Get<'T>(container, id)

        static member TryRead<'T>(sequence : ICloudSeq<'T>) : Cloud<seq<'T> option> =
            mkTry<StoreException, _> <| CloudSeq.Read(sequence)

