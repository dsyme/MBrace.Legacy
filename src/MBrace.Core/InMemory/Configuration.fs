namespace Nessos.MBrace.Core.InMemory

    open System
    open System.Reflection
    open System.Collections.Concurrent

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Core.Utils

    type private InMemoryTag = interface end

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

        interface InMemoryTag

    type InMemoryCloudRefStore () =
        static let store = new ConcurrentDictionary<string, ICloudRef> ()

        static let checkType(cref : ICloudRef) =
            match cref with
            | :? InMemoryTag -> ()
            | _ -> invalidArg "cref" "not an in-memory cloud ref."

        static let createUntyped(id : string, value : obj, ty : Type) =
            let crefType = typedefof<InMemoryCloudRef<_>>.MakeGenericType [|ty|]
            let cref = Activator.CreateInstance(crefType, [|id :> obj ; value |])
            cref :?> ICloudRef

        interface ICloudRefProvider with
            member __.CreateExisting(_,id) = async {
                return
                    let ok,v = store.TryGetValue id in
                    if ok then v
                    else
                        failwithf "Cloud ref '%s' not found" id
            }

            member __.CreateNew(_,id,value) = async {
                return
                    let cref = new InMemoryCloudRef<'T>(id, value) in
                    if store.TryAdd(id, cref) then cref :> ICloudRef<'T>
                    else
                        failwith "Cloud ref '%s' could not be created." id
            }

            member __.CreateNewUntyped(_,id,value,ty) = async {
                return
                    let cref = createUntyped(id, value, ty) in
                    if store.TryAdd(id, cref) then cref
                    else
                        failwith "Cloud ref '%s' could not be created." id
            }

            member __.Dereference(cref : ICloudRef) = async {
                checkType cref
                return cref.Value
            } 

            member __.Delete(cref : ICloudRef) = async {
                do checkType cref
                return
                    let mutable x = cref in
                    if store.TryRemove(cref.Name, &x) then ()
                    else
                        failwith "Cloud ref '%s' could not be deleted." cref.Name
            }

            member __.GetContainedRefs _ = async {
                return
                    store |> Seq.map (function (KeyValue(_,v)) -> v) |> Seq.toArray
            }

            

//   /// Defines a provider abstraction for cloud refs
//    type ICloudRefProvider =
//        
//        /// Defines a new cloud ref instance 
//        abstract CreateNew : container:string * id:string * value:'T -> Async<ICloudRef<'T>>
//        
//        /// Defines a new cloud ref instance
//        abstract CreateNewUntyped : container:string * id:string * value:obj * ty:Type -> Async<ICloudRef>
//
//        /// Defines an already existing cloud ref
//        abstract CreateExisting : container:string * id:string -> Async<ICloudRef>
//
//        /// Receives the value of given cloud ref
//        abstract Dereference : ICloudRef -> Async<obj>
//
//        /// Deletes a cloud ref
//        abstract Delete : ICloudRef -> Async<unit>
//
//        /// Receive all cloud ref's defined within the given container
//        abstract GetContainedRefs : container:string -> Async<ICloudRef []>
//
//    /// Defines a provider abstraction for mutable cloud refs
//    type IMutableCloudRefProvider =
//        
//        /// Defines a new mutable cloud ref instance
//        abstract CreateNew : container:string * id:string * value:'T -> Async<IMutableCloudRef<'T>>
//
//        // Defines a new mutable cloud ref instance
//        abstract CreateNewUntyped : container:string * id:string * value:obj * ty:Type -> Async<IMutableCloudRef>
//
//        /// Defines an existing mutable cloud ref instance
//        abstract CreateExisting : container:string * id:string -> Async<IMutableCloudRef>
//
//        /// Receives the value of given cloud ref
//        abstract Dereference : IMutableCloudRef -> Async<obj>
//        
//        /// Force update a mutable cloud ref
//        abstract ForceUpdate : IMutableCloudRef * value:obj -> Async<unit>
//
//        /// Try update a mutable cloud ref
//        abstract TryUpdate : IMutableCloudRef * value:obj -> Async<bool>
//
//        /// Deletes a mutable cloud ref
//        abstract Delete : IMutableCloudRef -> Async<unit>
//
//        /// Receive all cloud ref's defined within the given container
//        abstract GetContainedRefs : container:string -> Async<IMutableCloudRef []>
//
//    /// Defines a provider abstraction for cloud sequences
//    type ICloudSeqProvider =
//        
//        /// Defines a new cloud seq instance
//        abstract CreateNew : container:string * id:string * values:seq<'T> -> Async<ICloudSeq<'T>>
//
//        /// Defines a new untyped cloud seq instance
//        abstract CreateNewUntyped : container:string * id:string * values:IEnumerable * ty:Type -> Async<ICloudSeq>
//
//        /// Defines an existing cloud seq instance
//        abstract CreateExisting : container:string * id:string  -> Async<ICloudSeq>
//
//        /// Receive all cloud seq's defined within the given container
//        abstract GetContainedSeqs : container:string -> Async<ICloudSeq []>
//        
//        /// Deletes a cloud sequence
//        abstract Delete : ICloudSeq -> Async<unit>
//
//    /// Defines a provider abstraction for cloud files
//    type ICloudFileProvider =
//        
//        /// Defines a new cloud file
//        abstract CreateNew : container:string * id:string * writer:(Stream -> Async<unit>) -> Async<ICloudFile>
//
//        /// Defines an existing cloud file
//        abstract CreateExisting : container:string * id:string -> Async<ICloudFile>
//
//        /// Reads from an existing cloud file
//        abstract Read : file:ICloudFile * reader:(Stream -> Async<'T>) -> Async<'T>
//        
//        /// Deserialize a sequence from a given cloud file
//        abstract ReadAsSequence: file:ICloudFile * elementReader:(Stream -> Async<obj>) * seqType:Type  -> Async<IEnumerable>
//
//        /// Delete a cloud file
//        abstract Delete: file:ICloudFile -> Async<unit>
//
//        /// Get all cloud files that exist in specified container
//        abstract GetContainedFiles : container:string -> Async<ICloudFile []>
//
//    /// Defines an object cloning abstraction
//    type IObjectCloner =
//        abstract Clone : 'T -> 'T

    type TrivialObjectCloner() =
        interface IObjectCloner with
            member __.Clone t = t