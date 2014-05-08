namespace Nessos.MBrace

    open System
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    type ICloudDisposable =
        inherit ISerializable
        abstract Dispose : unit -> Async<unit>

    type ICloudRef =
        inherit ICloudDisposable

        abstract Name : string 
        abstract Container : string
        abstract Type : Type
        abstract Value : obj
        abstract TryValue : obj option

    type ICloudRef<'T> = 
        inherit ICloudRef

        abstract Value : 'T
        abstract TryValue : 'T option

    type ICloudSeq =
        inherit ICloudDisposable

        abstract Name : string
        abstract Container : string
        abstract Type : Type
        abstract Size : int64
        abstract Count : int

    type ICloudSeq<'T> =
        inherit IEnumerable<'T>
        inherit ICloudSeq

    type IMutableCloudRef = 
        inherit ICloudDisposable

        abstract Name : string
        abstract Container : string
        abstract Type : Type

    type IMutableCloudRef<'T> = 
        inherit IMutableCloudRef

    type ICloudFile =
        inherit ICloudDisposable

        abstract Name : string
        abstract Container : string