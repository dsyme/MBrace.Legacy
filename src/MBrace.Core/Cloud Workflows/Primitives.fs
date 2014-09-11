namespace Nessos.MBrace

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    /// Denotes handle to a distributable resource that can be disposed of.
    type ICloudDisposable =
        inherit ISerializable
        /// Releases any storage resources used by this object.
        abstract Dispose : unit -> Async<unit>

    /// Represents an immutable reference to an
    /// object that is persisted in the underlying CloudStore.
    type ICloudRef =
        inherit ICloudDisposable

        /// CloudRef identifier.
        abstract Name : string 
        /// CloudRef containing folder in underlying store.
        abstract Container : string
        /// Type of value referenced by CloudRef.
        abstract Type : Type
        /// Retrieve value referenced by CloudRef.
        abstract Value : obj

    /// Represents an immutable reference to an
    /// object that is stored in the underlying CloudStore.
    type ICloudRef<'T> = 
        inherit ICloudRef

        /// Retrieve value referenced by CloudRef.
        abstract Value : 'T
        /// Try Retrieving value referenced by CloudRef.
        abstract TryValue : 'T option

    /// Represents a finite and immutable sequence of
    /// elements that is stored in the underlying CloudStore
    /// and will be enumerated on demand.
    type ICloudSeq =
        inherit ICloudDisposable
        inherit IEnumerable

        /// CloudSeq identifier.
        abstract Name : string
        /// CloudSeq containing folder in underlying store.
        abstract Container : string
        /// Element Type in referenced CloudSeq.
        abstract Type : Type
        /// Approximate size (in bytes) of the referenced CloudSeq.
        abstract Size : int64
        /// CloudSeq element count.
        abstract Count : int

    /// Represents a finite and immutable sequence of
    /// elements that is stored in the underlying CloudStore
    /// and will be enumerated on demand.
    type ICloudSeq<'T> =
        inherit ICloudSeq
        inherit IEnumerable<'T>

    /// Represents a mutable reference to an
    /// object that is stored in the underlying CloudStore.
    type IMutableCloudRef = 
        inherit ICloudDisposable

        /// MutableCloudRef identifier.
        abstract Name : string
        /// MutableCloudSeq containing folder in underlying store.
        abstract Container : string
        /// Type of value referenced by MutableCloudRef.
        abstract Type : Type
        /// Asynchronously dereferences the MutableCloudRef.
        abstract ReadValue : unit -> Async<obj>
        /// Asynchronously attempts to update the MutableCloudRef; returns true if successful.
        abstract TryUpdate : obj -> Async<bool>
        /// Asynchronously forces update to MutableCloudRef regardless of state.
        abstract ForceUpdate : obj -> Async<unit>

    /// Represents a mutable reference to an
    /// object that is stored in the underlying CloudStore.
    type IMutableCloudRef<'T> = 
        inherit IMutableCloudRef

        /// Dereferences current value of MutableCloudRef.
        abstract Value : 'T
        /// Asynchronously dereferences current value of MutableCloudRef.
        abstract ReadValue : unit -> Async<'T>
        /// Asynchronously attempts to update the MutableCloudRef; returns true if successful.
        abstract TryUpdate : 'T -> Async<bool>
        /// Asynchronously forces update to MutableCloudRef regardless of state.
        abstract ForceUpdate : 'T -> Async<unit>

    /// Represents a binary object stored in the underlying CloudStore.
    type ICloudFile =
        inherit ICloudDisposable

        /// CloudFile Identifier.
        abstract Name : string
        /// CloudFile containing folder in underlying store.
        abstract Container : string
        /// CloudFile size in bytes.
        abstract Size : int64
        /// Asynchronously returns a read-only stream with access to CloudFile data.
        abstract Read : unit -> Async<Stream>

    type ICloudArray =
        inherit IEnumerable

        abstract Name : string
        abstract Container : string
        abstract Length : int64

        //abstract Cache : unit -> ICachedCloudArray<'T>
        //abstract Append : ICloudArray<'T> -> ICloudArray<'T>
        //abstract Item : int64 -> 'T with get

    type ICloudArray<'T> =
        inherit ICloudArray
        inherit IEnumerable<'T>

        abstract Cache : unit -> ICachedCloudArray<'T>
        abstract Append : ICloudArray<'T> -> ICloudArray<'T>
        abstract Item : int64 -> 'T with get
        abstract Range : int64 * int -> 'T []

    and ICachedCloudArray<'T> =
        inherit ICloudArray<'T>

