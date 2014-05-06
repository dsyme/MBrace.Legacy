namespace Nessos.MBrace.Runtime

    open System
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils

    type CloudFile(id : string, container : string) =

        let fileStoreLazy = lazy IoC.Resolve<ICloudFileStore>()

        interface ICloudFile with
            member self.Name = id
            member self.Container = container
            member self.Dispose () =
                fileStoreLazy.Value.Delete(container, id)

        override self.ToString() = sprintf' "%s - %s" container id

        new (info : SerializationInfo, context : StreamingContext) = 
                CloudFile(info.GetValue("id", typeof<string>) :?> string,
                            info.GetValue("container", typeof<string>) :?> string)
        
        interface ISerializable with 
            member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)



    type PersistableCloudRef<'T>(id : string, container : string, ty : Type) as this = 
        let cloudRefReaderLazy = 
            lazy ( 
                    if IoC.IsRegistered<ICloudRefStore>() then 
                        Some <| IoC.Resolve<ICloudRefStore>() 
                    else
                        None
                    )
        let valueLazy () = 
            match cloudRefReaderLazy.Value with
            | Some reader -> 
                async {
                    let! value = containAsync <| reader.Read(this) 
                    match value with
                    | Choice1Of2 value -> return value
                    | Choice2Of2 exc ->
                        let! exists = containAsync <| reader.Exists(container, id)
                        match exists with
                        | Choice1Of2 false -> 
                            return raise <| NonExistentObjectStoreException(container, id)
                        | _ -> 
                            return raise <| StoreException(sprintf' "Cannot locate Container: %s, Name: %s" container id, exc)
                } |> Async.RunSynchronously
            | None -> 
                raise <| new InvalidOperationException(sprintf' "CloudRef %s cannot be dereferenced; no reader is initialized." id)

        new (info : SerializationInfo, context : StreamingContext) = 
            PersistableCloudRef<'T>(info.GetValue("id", typeof<string>) :?> string,
                                    info.GetValue("container", typeof<string>) :?> string,
                                    info.GetValue("type", typeof<Type>) :?> Type)

        override self.ToString() = 
            sprintf' "%s - %s" container id

        interface ICloudRef with
            member self.Name = id
            member self.Container = container
            member self.Type = ty
            member self.Value = valueLazy() :> obj
            member self.TryValue = 
                try 
                    Some (valueLazy () :> obj)
                with _ -> None

        interface ICloudDisposable with
            member self.Dispose () = 
                let self = self :> ICloudRef
                cloudRefReaderLazy.Value.Value.Delete(self.Container, self.Name)

        interface ICloudRef<'T> with
            member self.Value = valueLazy ()
            member self.TryValue = 
                try 
                    Some (self :> ICloudRef<'T>).Value
                with _ -> None
                    

//        interface IPersistableCloudRef with
//            member self.Container = container
//
//        interface IPersistableCloudRef<'T> 
                

        interface ISerializable with
            member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
//                    // collect ids
//                    match context.Context with
//                    | :? List<string> as ids -> ids.Add(id)
//                    | _ -> ()
                // Add
                info.AddValue("id", id)
                info.AddValue("container", container)
                info.AddValue("type", ty)



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


    type MutableCloudRef<'T>(id : string, container : string, tag : Tag, ty : Type) =
        
        let mutablecloudrefstorelazy = lazy IoC.Resolve<IMutableCloudRefStore>()

        interface IMutableCloudRef with
            member self.Name = id
            member self.Container = container
            member self.Type = ty
            member self.Dispose () =
                let self = self :> IMutableCloudRef
                mutablecloudrefstorelazy.Value.Delete(self.Container, self.Name)

        interface IMutableCloudRef<'T>

        interface IMutableCloudRefTagged with
            member val Tag = tag with get, set

        interface IMutableCloudRefTagged<'T>

        override self.ToString() =  sprintf' "%s - %s" container id

        new (info : SerializationInfo, context : StreamingContext) = 
                MutableCloudRef<'T>(info.GetValue("id", typeof<string>) :?> string,
                                    info.GetValue("container", typeof<string>) :?> string,
                                    info.GetValue("tag", typeof<string>) :?> string,
                                    info.GetValue("type", typeof<Type>) :?> Type)
        
        interface ISerializable with 
            member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)
                info.AddValue("tag", (self :> IMutableCloudRefTagged).Tag)
                info.AddValue("type", ty)