namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Reflection
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Core

    [<AbstractClass>]
    type CloudRef internal (id : string, container : string, provider : CloudRefProvider) as this =

        let valueLazy () = 
            async {
                let! value = Async.Catch <| (provider :> ICloudRefProvider).Dereference this
                match value with
                | Choice1Of2 value -> return value
                | Choice2Of2 exc ->
                    let! exists = Async.Catch <| provider.Exists(container, id)
                    match exists with
                    | Choice1Of2 false -> 
                        return raise <| NonExistentObjectStoreException(container, id)
                    | _ -> 
                        return raise <| StoreException(sprintf' "Cannot locate Container: %s, Name: %s" container id, exc)
            } |> Async.RunSynchronously

        abstract Type : Type

        member internal __.Value = valueLazy ()

        interface ICloudRef with
            member self.Name = id
            member self.Container = container
            member self.Type = self.Type
            member self.Value = self.Value
            member self.TryValue = 
                try 
                    Some (valueLazy ())
                with _ -> None
    
        interface ISerializable with
            member self.GetObjectData(_,_) = raise <| new NotSupportedException("implemented by the inheriting class")

        interface ICloudDisposable with
            member self.Dispose () = (provider :> ICloudRefProvider).Delete(self)

    and CloudRef<'T> internal (id : string, container : string, provider : CloudRefProvider) =
        inherit CloudRef(id, container, provider)

        override __.Type = typeof<'T>

        new (info : SerializationInfo, _ : StreamingContext) = 
            let id = info.GetString("id")
            let container = info.GetString("container")
            let storeId = info.GetValue("storeId", typeof<StoreId>) :?> StoreId
            let provider =
                match StoreRegistry.TryGetCoreConfiguration storeId with
                | None -> raise <| new MBraceException(sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName)
                | Some config -> config.CloudRefProvider :?> CloudRefProvider
            
            new CloudRef<'T>(id, container, provider)

        override self.ToString() = sprintf' "cloudref:%s/%s" container id

        interface ICloudRef<'T> with
            member self.Value = base.Value :?> 'T
            member self.TryValue = 
                try Some (base.Value :?> 'T)
                with _ -> None
                    
        interface ISerializable with
            override self.GetObjectData(info : SerializationInfo, _ : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)
                info.AddValue("storeId", provider.StoreId, typeof<StoreId>)
    

    and internal CloudRefProvider(storeInfo : StoreInfo, cache : LocalObjectCache) as self =
        let store = storeInfo.Store
        let storeId = storeInfo.Id

        let extension = "ref"
        let postfix = fun s -> sprintf' "%s.%s" s extension

        let read container id : Async<Type * obj> = async {
                use! stream = store.ReadImmutable(container, id)
                let t = Serialization.DefaultPickler.Deserialize<Type> stream
                let o = Serialization.DefaultPickler.Deserialize<obj> stream
                return t, o
            }

        let readType container id  = async {
                use! stream = store.ReadImmutable(container, id)
                let t = Serialization.DefaultPickler.Deserialize<Type> stream
                return t
            }

        let getIds (container : string) : Async<string []> = async {
                let! files = store.GetAllFiles(container)
                return files
                    |> Seq.filter (fun w -> w.EndsWith <| sprintf' ".%s" extension)
                    |> Seq.map (fun w -> w.Substring(0, w.Length - extension.Length - 1))
                    |> Seq.toArray
            }

        let defineUntyped(ty : Type, container : string, id : string) =
            typeof<CloudRefProvider>
                .GetMethod("CreateCloudRef", BindingFlags.Static ||| BindingFlags.NonPublic)
                .MakeGenericMethod([| ty |])
                .Invoke(null, [| id :> obj ; container :> obj ; self :> obj |])
                :?> ICloudRef

        // WARNING : method called by reflection from 'defineUntyped' function above
        static member CreateCloudRef<'T>(container, id, provider) =
            new CloudRef<'T>(container, id, provider)

        member __.StoreId = storeInfo.Id

        member self.Exists(container : string) : Async<bool> = 
            store.ContainerExists(container)

        member self.Exists(container : string, id : string) : Async<bool> = 
            store.Exists(container, postfix id)

        interface ICloudRefProvider with

            member self.CreateNew (container : string, id : string, value : 'T) : Async<ICloudRef<'T>> = 
                async {
                    do! store.CreateImmutable(container, postfix id, 
                            (fun stream -> async {
                                Serialization.DefaultPickler.Serialize(stream, typeof<'T>)
                                Serialization.DefaultPickler.Serialize<obj>(stream, value) }), false)

                    return new CloudRef<'T>(id, container, self) :> _
            }

            member self.CreateNewUntyped (container : string, id : string, value : obj, t : Type) : Async<ICloudRef> = 
                async {
                    do! store.CreateImmutable(container, postfix id, 
                            (fun stream -> async {
                                Serialization.DefaultPickler.Serialize(stream, t)
                                Serialization.DefaultPickler.Serialize(stream, value) }), false)

                    // construct & return
                    return defineUntyped(t, container, id)
            }

            member self.CreateExisting(container, id) : Async<ICloudRef> =
                async {
                    let! t = readType container (postfix id)
                    return defineUntyped(t, container, id)
                }

            member self.GetContainedRefs(container : string) : Async<ICloudRef []> =
                async {
                    let! ids = getIds container
                    return
                        ids |> Seq.map (fun id -> Async.RunSynchronously(readType container (postfix id)), container, id)
                            |> Seq.map defineUntyped
                            |> Seq.toArray
                }

            member self.Delete(cloudRef : ICloudRef) : Async<unit> = 
                async {
                    let id = postfix cloudRef.Name
                    let! containsKey = cache.ContainsKey id
                    if containsKey then
                        do! cache.Delete(id)

                    return! store.Delete(cloudRef.Container, id)
                }

            member self.Dereference (cref : ICloudRef) = 
                async {
                    let id, container, t = cref.Name, cref.Container, cref.Type
                    let id = postfix id

                    // get value
                    let! cacheResult = cache.TryFind <| sprintf' "%s" id
                    match cacheResult with
                    | Some value -> 
                        return value :?> Type * obj |> snd
                    | None -> 
                        let! ty, value = read container id
                        if t <> ty then 
                            let msg = sprintf' "CloudRef type mismatch. Internal type %s, got %s" ty.AssemblyQualifiedName t.AssemblyQualifiedName
                            raise <| MBraceException(msg)
                        // update cache
                        cache.Set(id, (ty, value))
                        return value
                }