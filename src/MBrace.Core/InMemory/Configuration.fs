namespace Nessos.MBrace.Core.InMemory

    open System
    open System.Collections.Concurrent

    open Nessos.MBrace
    open Nessos.MBrace.Core

    type InMemoryCloudRef<'T> internal (id : string, value : 'T) =
        interface ICloudRef<'T> with
            member __.Name = id
            member __.Container = ""
            member __.Type = typeof<'T>
            member __.Value = value
            member __.Value = value :> obj
            member __.TryValue = Some value
            member __.TryValue = Some (value :> obj)
            member __.Dispose () = async.Zero()
            member __.GetObjectData (_,_) = raise <| new NotSupportedException()

//    type InMemoryCloudRefStore () =
//        static let store = new ConcurrentDictionary<string, ICloudRef> ()
//
//        interface ICloudRefStore with
//            member __.Create(container, id, value, t) =

//
//    type ICloudRefStore =
//        abstract Create : Container * Id * obj * System.Type -> Async<ICloudRef>
//        abstract Create : Container * Id * 'T -> Async<ICloudRef<'T>>
//        abstract Delete : Container * Id -> Async<unit>
//        abstract Exists : Container -> Async<bool>
//        abstract Exists : Container * Id -> Async<bool>
//        abstract GetRefType : Container * Id -> Async<System.Type>
//        abstract GetRefs : Container ->Async<ICloudRef []>
//        abstract Read : Container * Id * System.Type -> Async<obj>
//        abstract Read : ICloudRef -> Async<obj>
//        abstract Read : ICloudRef<'T> -> Async<'T>
//
//    type CloudSeqInfo = { Size : int64; Count : int; Type : Type }
//    
//    type ICloudSeqStore =
//        // added just now : probably needed
//        abstract Get : Container * Id (* * Type *) -> Async<ICloudSeq>
//
//
//        abstract Create : System.Collections.IEnumerable * string * string * System.Type -> Async<ICloudSeq>
//        abstract Delete : Container * Id -> Async<unit>
//        abstract Exists : Container -> Async<bool>
//        abstract Exists : Container * Id -> Async<bool>
//        abstract GetCloudSeqInfo : ICloudSeq<'T> -> Async<CloudSeqInfo>
//        abstract GetEnumerator : ICloudSeq<'T> -> Async<System.Collections.Generic.IEnumerator<'T>>
//        abstract GetIds : Container -> Async<string []>
//        abstract GetSeqs : Container -> Async<ICloudSeq []>
//
//    type IMutableCloudRefStore =
//        abstract member Create : Container * Id * obj * System.Type -> Async<IMutableCloudRef>
//        abstract member Create : Container * Id * 'T -> Async<IMutableCloudRef<'T>>
//        abstract member Delete : Container * Id -> Async<unit>
//        abstract member Exists : Container -> Async<bool>
//        abstract member Exists : Container * Id -> Async<bool>
//        abstract member ForceUpdate : IMutableCloudRef * obj -> Async<unit>
//        abstract member GetRefType : Container * Id -> Async<System.Type>
//        abstract member GetRefs : Container -> Async<IMutableCloudRef []>
//        abstract member Read : IMutableCloudRef -> Async<obj>
//        abstract member Read : IMutableCloudRef<'T> -> Async<'T>
//        abstract member Update : IMutableCloudRef * obj -> Async<bool>
//        abstract member Update : IMutableCloudRef<'T> * 'T -> Async<bool>
//
//
//    type IObjectCloner =
//        abstract Clone : 'T -> 'T

    type TrivialObjectCloner() =
        interface IObjectCloner with
            member __.Clone t = t