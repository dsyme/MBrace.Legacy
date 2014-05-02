namespace Nessos.MBrace.Core

    open System
    open System.Collections
    open System.Collections.Generic
    open System.IO
    open System.Runtime.Serialization
    
    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Caching
    open Nessos.MBrace.Store
    open Nessos.MBrace.Utils

    [<Serializable>]
    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>] 
    type internal CloudFileSeq<'T> (file : ICloudFile, reader:(Stream -> Async<obj>)) =
        let factoryLazy = lazy IoC.Resolve<ICloudFileStore>()

        override this.ToString () = sprintf' "%s - %s" file.Container file.Name

        member private this.StructuredFormatDisplay = this.ToString()

        interface IEnumerable with
            member this.GetEnumerator () = 
                let s = factoryLazy.Value.Read(file, reader) |> Async.RunSynchronously :?> IEnumerable
                s.GetEnumerator()

        interface IEnumerable<'T> with
            member this.GetEnumerator () = 
                let s = factoryLazy.Value.Read(file, reader) |> Async.RunSynchronously :?> IEnumerable<'T> 
                s.GetEnumerator()
            
        interface ISerializable with
            member this.GetObjectData (info : SerializationInfo , context : StreamingContext) =
                info.AddValue ("file", file)
                info.AddValue ("reader", reader)

        new (info : SerializationInfo , context : StreamingContext) =
            CloudFileSeq(info.GetValue("file", typeof<ICloudFile> ) :?> ICloudFile, 
                         info.GetValue ("reader", typeof<Stream -> Async<obj>>) :?> Stream -> Async<obj>)

//    [<Serializable>]
//    [<StructuredFormatDisplay("{StructuredFormatDisplay}")>] 
//    type internal CloudSeqLight<'T> (id : string, container : string, interpretation : CloudFileInterpretation ) =
//        let factoryLazy = lazy IoC.Resolve<ICloudFileStore>()
//
//        override this.ToString () = sprintf' "%s - %s" container id
//
//        member private this.StructuredFormatDisplay = this.ToString()
//
//        interface IEnumerable with
//            member this.GetEnumerator () = factoryLazy.Value.Read(this) |> Async.RunSynchronously :?> IEnumerator
//        
//        interface IEnumerable<'T> with
//            member this.GetEnumerator () = factoryLazy.Value.Read(this) |> Async.RunSynchronously :?> IEnumerator<'T> 
//            
//        interface ISerializable with
//            member this.GetObjectData (info : SerializationInfo , context : StreamingContext) =
//                info.AddValue ("name", (this :> ICloudFile).Name)
//                info.AddValue ("container", (this :> ICloudFile).Container)
//                info.AddValue ("interpretation", interpretation, typeof<CloudFileInterpretation>)
//
////                let i = match (this :> ICloudFileConcrete).Interpretation with
////                        | LineSeq -> 0
////                        | ByteSeq -> 1
////                info.AddValue ("interpretation", i)
//
//        new (info : SerializationInfo , context : StreamingContext) =
//            CloudSeqLight(info.GetString "name", 
//                          info.GetString "container",
//                          info.GetValue ("interpretation", typeof<CloudFileInterpretation>) :?> CloudFileInterpretation)
//
//                          match info.GetInt32 "interpretation" with
//                          | 0 -> LineSeq
//                          | 1 -> ByteSeq
//                          | _ -> failwith "Invalid interpretation")

//        interface ICloudFileConcrete with
//            member this.Name = id
//            member this.Container = container
//            member this.Interpretation = interpretation
//
//            member this.AllLines () = invalidOp "Invalid interpretation on concrete file"
//            member this.AllText  () = invalidOp "Invalid interpretation on concrete file"
//            member this.ToArray  () = invalidOp "Invalid interpretation on concrete file"