namespace Nessos.MBrace.Runtime
    
    open System
    open System.Runtime.Serialization
    open System.Collections.Generic

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry

    type ICloudRefStore =
        abstract Create : Container * Id * obj * System.Type -> Async<IPersistableCloudRef>
        abstract Create : Container * Id * 'T -> Async<IPersistableCloudRef<'T>>
        abstract Delete : Container * Id -> Async<unit>
        abstract Exists : Container -> Async<bool>
        abstract Exists : Container * Id -> Async<bool>
        abstract GetRefType : Container * Id -> Async<System.Type>
        abstract GetRefs : Container ->Async<ICloudRef []>
        abstract Read : Container * Id * System.Type -> Async<obj>
        abstract Read : IPersistableCloudRef -> Async<obj>
        abstract Read : IPersistableCloudRef<'T> -> Async<'T>
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Special PersistableCloudRef - special treatment - access to store 
    and IPersistableCloudRef =
        inherit ICloudRef
        abstract Container : string
    and IPersistableCloudRef<'T> =
        inherit IPersistableCloudRef
        inherit ICloudRef<'T>

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
                    

        interface IPersistableCloudRef with
            member self.Container = container

        interface IPersistableCloudRef<'T> 
                

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