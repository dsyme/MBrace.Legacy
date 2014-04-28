namespace Nessos.MBrace.Runtime
    
    open System
    open System.Runtime.Serialization
    open System.Collections
    open System.Collections.Generic

    open Nessos.MBrace
    open Nessos.MBrace.Utils

    type CloudSeqInfo = { Size : int64; Count : int; Type : Type }
    
    type ICloudSeqStore =
        abstract Create : System.Collections.IEnumerable * string * string * System.Type -> Async<ICloudSeq>
        abstract Delete : Container * Id -> Async<unit>
        abstract Exists : Container -> Async<bool>
        abstract Exists : Container * Id -> Async<bool>
        abstract GetCloudSeqInfo : ICloudSeq<'T> -> Async<CloudSeqInfo>
        abstract GetEnumerator : ICloudSeq<'T> -> Async<System.Collections.Generic.IEnumerator<'T>>
        abstract GetIds : Container -> Async<string []>
        abstract GetSeqs : Container -> Async<ICloudSeq []>

    [<Serializable>]
    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>] 
    type CloudSeq<'T> (id : string, container : string ) as this =
        let factoryLazy = lazy IoC.Resolve<ICloudSeqStore>()

        let info = lazy (Async.RunSynchronously <| factoryLazy.Value.GetCloudSeqInfo(this))

        interface ICloudSeq with
            member this.Name = id
            member this.Container = container
            member this.Type = info.Value.Type
            member this.Count = info.Value.Count
            member this.Size = info.Value.Size
            member this.Dispose () =
                let this = this :> ICloudSeq
                factoryLazy.Value.Delete(this.Container, this.Name)

        interface ICloudSeq<'T>

        override this.ToString () = sprintf "%s - %s" container id

        member private this.StructuredFormatDisplay = this.ToString()

        interface IEnumerable with
            member this.GetEnumerator () = 
                factoryLazy.Value.GetEnumerator(this)
                |> Async.RunSynchronously :> IEnumerator
        
        interface IEnumerable<'T> with
            member this.GetEnumerator () = 
                factoryLazy.Value.GetEnumerator(this)  
                |> Async.RunSynchronously
            
        interface ISerializable with
            member this.GetObjectData (info : SerializationInfo , context : StreamingContext) =
                info.AddValue ("id", (this :> ICloudSeq<'T>).Name)
                info.AddValue ("container", (this :> ICloudSeq<'T>).Container)

        new (info : SerializationInfo , context : StreamingContext) =
            CloudSeq(info.GetString "id", 
                     info.GetString "container")