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
    /// object that is stored in the underlying CloudStore.
    type ICloudRef =
        inherit ICloudDisposable

        /// The CloudRef's name.
        abstract Name : string 
        /// The CloudRef's container (folder).
        abstract Container : string
        /// The type of the object stored in the CloudRef.
        abstract Type : Type
        /// The object stored in the CloudRef.
        abstract Value : obj

    /// Represents an immutable reference to an
    /// object that is stored in the underlying CloudStore.
    type ICloudRef<'T> = 
        inherit ICloudRef

        /// The object stored in the CloudRef.
        abstract Value : 'T
        /// The object stored in the CloudRef.
        abstract TryValue : 'T option

    /// Represents a finite and immutable sequence of
    /// elements that is stored in the underlying CloudStore
    /// and will be enumerated on demand.
    type ICloudSeq =
        inherit ICloudDisposable
        inherit IEnumerable

        /// The CloudSeq's name.
        abstract Name : string
        /// The CloudSeq's container (folder).
        abstract Container : string
        /// The type of the elements stored in the CloudSeq.
        abstract Type : Type
        /// The size of the CloudSeq in the underlying store.
        /// The value is in bytes and might be an approximation.
        abstract Size : int64
        /// The number of elements contained in the CloudSeq.
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

        /// The MutableCloudRef's name.
        abstract Name : string
        /// The MutableCloudRef's container (folder).
        abstract Container : string
        /// The type of the object stored in the MutableCloudRef.
        abstract Type : Type
        /// Returns an asynchronous computation that gets the current value of the MutableCloudRef.
        abstract ReadValue : unit -> Async<obj>
        /// Executes an update operation on the MutableCloudRef and returns if the operation succeeded.
        abstract TryUpdate : obj -> Async<bool>
        /// Updates the current value of the MutableCloudRef with the given value.
        abstract ForceUpdate : obj -> Async<unit>

    /// Represents a mutable reference to an
    /// object that is stored in the underlying CloudStore.
    type IMutableCloudRef<'T> = 
        inherit IMutableCloudRef

        /// Gets the current value of the MutableCloudRef.
        abstract Value : 'T
        /// Returns an asynchronous computation that gets the current value of the MutableCloudRef.
        abstract ReadValue : unit -> Async<'T>
        /// Executes an update operation on the MutableCloudRef and returns if the operation succeeded.
        abstract TryUpdate : 'T -> Async<bool>
        /// Updates the current value of the MutableCloudRef with the given value.
        abstract ForceUpdate : 'T -> Async<unit>

    /// Represents a binary object stored in the underlying CloudStore.
    type ICloudFile =
        inherit ICloudDisposable

        /// The CloudFile's name.
        abstract Name : string
        /// The CloudFile's container (folder).
        abstract Container : string
        /// The CloudFile's size in bytes.
        abstract Size : int64
        /// A asynchronous computations that returns the content of the CloudFile as a Stream.
        /// Note that this Stream is not guaranteed to be a direct Stream to the underlying store
        /// and it should be used only for read operations.
        abstract Read : unit -> Async<Stream>