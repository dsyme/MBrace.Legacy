namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Core

    type CloudRef<'T>(id : string, container : string, ty : Type) as this = 
        let cloudRefReaderLazy = lazy IoC.Resolve<CloudRefProvider>() 

        let valueLazy () = 
            async {
                let! value = containAsync <| (cloudRefReaderLazy.Value :> ICloudRefProvider).Read(this) 
                match value with
                | Choice1Of2 value -> return value
                | Choice2Of2 exc ->
                    let! exists = containAsync <| cloudRefReaderLazy.Value.Exists(container, id)
                    match exists with
                    | Choice1Of2 false -> 
                        return raise <| NonExistentObjectStoreException(container, id)
                    | _ -> 
                        return raise <| StoreException(sprintf' "Cannot locate Container: %s, Name: %s" container id, exc)
            } |> Async.RunSynchronously

        new (info : SerializationInfo, context : StreamingContext) = 
            CloudRef<'T>(info.GetValue("id", typeof<string>) :?> string,
                                    info.GetValue("container", typeof<string>) :?> string,
                                    info.GetValue("type", typeof<Type>) :?> Type)

        override self.ToString() = 
            sprintf' "%s - %s" container id

        interface ICloudRef with
            member self.Name = id
            member self.Container = container
            member self.Type = ty
            member self.Value = valueLazy() 
            member self.TryValue = 
                try 
                    Some (valueLazy ())
                with _ -> None

        interface ICloudDisposable with
            member self.Dispose () = 
                let self = self :> ICloudRef
                (cloudRefReaderLazy.Value :> ICloudRefProvider).Delete(self.Container, self.Name)

        interface ICloudRef<'T> with
            member self.Value = valueLazy () :?> 'T
            member self.TryValue = 
                try 
                    Some (self :> ICloudRef<'T>).Value
                with _ -> None
                    
        interface ISerializable with
            member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)
                info.AddValue("type", ty)
    

    and CloudRefProvider(store : IStore, cache : Cache) =
        let pickler = Nessos.MBrace.Runtime.Serializer.Pickler

        let extension = "ref"
        let postfix = fun s -> sprintf' "%s.%s" s extension

        let read container id : Async<Type * obj> = async {
                use! stream = store.Read(container, id)
                let t = pickler.Deserialize<Type> stream
                let o = pickler.Deserialize<obj> stream
                return t, o
            }

        let readType container id  = async {
                use! stream = store.Read(container, id)
                let t = pickler.Deserialize<Type> stream
                return t
            }

        let getIds (container : string) : Async<string []> = async {
                let! files = store.GetFiles(container)
                return files
                    |> Seq.filter (fun w -> w.EndsWith <| sprintf' ".%s" extension)
                    |> Seq.map (fun w -> w.Substring(0, w.Length - extension.Length - 1))
                    |> Seq.toArray
            }

        member self.GetRefType (container : string, id : string) : Async<Type> =
            readType container (postfix id)

        member self.Exists(container : string) : Async<bool> = 
            store.Exists(container)

        member self.Exists(container : string, id : string) : Async<bool> = 
            store.Exists(container, postfix id)

        interface ICloudRefProvider with

            member self.Create (container : string, id : string, value : obj, t : Type) : Async<ICloudRef> = 
                async {
                    do! store.Create(container, postfix id, 
                            fun stream -> async {
                                pickler.Serialize(stream, t)
                                pickler.Serialize(stream, value) })

                    // construct & return

                    let cloudRefType = typedefof<CloudRef<_>>.MakeGenericType [| t |]
                    let cloudRef = Activator.CreateInstance(cloudRefType, [| id :> obj; container :> obj; t :> obj |])
                    return cloudRef :?> _
            }

            member self.Read (cref : ICloudRef) = 
                async {
                    let id, container, t = cref.Name, cref.Container, cref.Type
                    let id = postfix id

                    // get value
                    match cache.TryFind <| sprintf' "%s" id with
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

            member self.GetRef(container, id) : Async<ICloudRef> =
                async {
                    let! t = readType container (postfix id)
                    let cloudRefType = typedefof<CloudRef<_>>.MakeGenericType [| t |]
                    let cloudRef = Activator.CreateInstance(cloudRefType, [| id :> obj; container :> obj; t :> obj |])
                    return cloudRef :?> ICloudRef
                }

            member self.GetRefs(container : string) : Async<ICloudRef []> =
                async {
                    let! ids = getIds container
                    return
                        ids |> Seq.map (fun id -> Async.RunSynchronously(readType container (postfix id)), container, id)
                            |> Seq.map (fun (t,c,i) ->
                                    let cloudRefTy = typedefof<CloudRef<_>>.MakeGenericType [| t |]
                                    let cloudRef = Activator.CreateInstance(cloudRefTy, [| i :> obj; c :> obj; t :> obj |])
                                    cloudRef :?> ICloudRef)
                            |> Seq.toArray
                }

            member self.Delete(container : string, id : string) : Async<unit> = 
                async {
                    let id = postfix id
                    if cache.ContainsKey(id) then
                        cache.Delete(id)
                    return! store.Delete(container, id)
                }