namespace Nessos.MBrace

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    /// Denotes handle to a distributable resource that can be disposed of.
    type ICloudDisposable =
        inherit ISerializable
        abstract Dispose : unit -> Async<unit>

    type ICloudRef =
        inherit ICloudDisposable

        abstract Name : string 
        abstract Container : string
        abstract Type : Type
        
        abstract Value : obj

    type ICloudRef<'T> = 
        inherit ICloudRef

        abstract Value : 'T
        abstract TryValue : 'T option

    type ICloudSeq =
        inherit ICloudDisposable
        inherit IEnumerable

        abstract Name : string
        abstract Container : string
        abstract Type : Type
        abstract Size : int64
        abstract Count : int

    type ICloudSeq<'T> =
        inherit ICloudSeq
        inherit IEnumerable<'T>

    type IMutableCloudRef = 
        inherit ICloudDisposable

        abstract Name : string
        abstract Container : string
        abstract Type : Type

        abstract ReadValue : unit -> Async<obj>
        abstract TryUpdate : obj -> Async<bool>
        abstract ForceUpdate : obj -> Async<unit>

    type IMutableCloudRef<'T> = 
        inherit IMutableCloudRef

        abstract Value : 'T
        abstract ReadValue : unit -> Async<'T>
        abstract TryUpdate : 'T -> Async<bool>
        abstract ForceUpdate : 'T -> Async<unit>

    type ICloudFile =
        inherit ICloudDisposable

        abstract Name : string
        abstract Container : string
        abstract Size : int64

        abstract Read : unit -> Async<Stream>