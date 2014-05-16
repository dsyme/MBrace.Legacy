namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Reflection
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime

    [<AbstractClass>]
    type MutableCloudRef internal (id : string, container : string, tag : Tag, provider : MutableCloudRefProvider) =

        member val internal Tag = tag with get, set

        abstract member Type : Type

        member __.StoreId = provider.StoreId

        interface IMutableCloudRef with
            member self.Name = id
            member self.Container = container
            member self.Type = self.Type
            member self.Dispose () = (provider :> IMutableCloudRefProvider).Delete(self)
            member self.GetObjectData(_,_) = raise <| new NotSupportedException("implemented by the inheriting class")

    and MutableCloudRef<'T> internal (id : string, container : string, tag : Tag, provider : MutableCloudRefProvider) =
        inherit MutableCloudRef(id, container, tag, provider)

        override __.Type = typeof<'T>

        new (info : SerializationInfo, context : StreamingContext) = 
            let id          = info.GetString("id")
            let container   = info.GetString("container")
            let tag         = info.GetString("tag")
            let storeId     = info.GetValue("storeId", typeof<StoreId>) :?> StoreId
            let config =
                match StoreRegistry.TryGetCoreConfiguration storeId with
                | None -> raise <| new StoreException(sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName)
                | Some config -> config.MutableCloudRefProvider :?> MutableCloudRefProvider

            new MutableCloudRef<'T>(id, container, tag, config)

        interface IMutableCloudRef<'T>
        
        interface ISerializable with 
            override self.GetObjectData(info : SerializationInfo, _ : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)
                info.AddValue("tag", tag)
                info.AddValue("storeId", provider.StoreId, typeof<StoreId>)


    and internal MutableCloudRefProvider(storeInfo : StoreInfo) as self =

        let extension = "mref"
        let postfix = fun s -> sprintf' "%s.%s" s extension

        let checkIsValid (mref : IMutableCloudRef) =
            match mref with
            | :? MutableCloudRef as m ->
                if m.StoreId = storeInfo.Id then m
                else
                    raise <| new StoreException("Mutable cloud ref belongs to invalid store.")
            | _ -> 
                raise <| new StoreException("Invalid mutable cloud ref.")

        let read container id : Async<Type * obj * Tag> = async {
                let id = postfix id
                let! s, tag = storeInfo.Store.ReadMutable(container, id)
                use stream = s
                let t = Serialization.DefaultPickler.Deserialize<Type> stream
                let o = Serialization.DefaultPickler.Deserialize<obj> stream
                return t, o, tag
            }

        let readType container id = async {
                let! stream, tag = storeInfo.Store.ReadMutable(container, id)
                let t = Serialization.DefaultPickler.Deserialize<Type> stream
                stream.Dispose()
                return t
            }

        let readInfo container id = async {
            let! stream, tag = storeInfo.Store.ReadMutable(container, id)        
            let t = Serialization.DefaultPickler.Deserialize<Type> stream
            stream.Dispose()
            return t, tag
        }

        let getIds (container : string) : Async<string []> = async {
                let! files = storeInfo.Store.GetAllFiles(container)
                return files 
                        |> Seq.filter (fun w -> w.EndsWith extension)
                        |> Seq.map (fun w -> w.Substring(0, w.Length - extension.Length - 1))
                        |> Seq.toArray
            }


        let defineUntyped(ty : Type, container : string, id : string, tag : string) =
            typeof<MutableCloudRefProvider>
                .GetMethod("CreateCloudRef", BindingFlags.Static ||| BindingFlags.NonPublic)
                .MakeGenericMethod([| ty |])
                .Invoke(null, [| id :> obj ; container :> obj ; tag :> obj ; self :> obj |])
                :?> IMutableCloudRef

        // WARNING : method called by reflection from 'defineUntyped' function above
        static member CreateCloudRef<'T>(id, container, tag, provider) =
            new MutableCloudRef<'T>(id , container, tag, provider)

        member self.StoreId = storeInfo.Id

        member self.GetRefType (container : string, id : string) : Async<Type> =
            readType container (postfix id)

        member self.Exists(container : string) : Async<bool> = 
            storeInfo.Store.ContainerExists(container)

        member self.Exists(container : string, id : string) : Async<bool> = 
            storeInfo.Store.Exists(container, postfix id)

        interface IMutableCloudRefProvider with

            member self.CreateNew (container : string, id : string, value : 'T) : Async<IMutableCloudRef<'T>> = 
                async {
                    let! tag = storeInfo.Store.CreateMutable(container, postfix id, 
                                fun stream -> async {
                                    Serialization.DefaultPickler.Serialize(stream, typeof<'T>)
                                    Serialization.DefaultPickler.Serialize(stream, value) })

                    return new MutableCloudRef<'T>(id, container, tag, self) :> _
                }

            member self.CreateNewUntyped (container : string, id : string, value : obj, t : Type) : Async<IMutableCloudRef> = 
                async {
                    let! tag = storeInfo.Store.CreateMutable(container, postfix id, 
                                fun stream -> async {
                                    Serialization.DefaultPickler.Serialize(stream, t)
                                    Serialization.DefaultPickler.Serialize(stream, value) })

                    return defineUntyped(t, container, id, tag)
                }

            member self.CreateExisting(container , id) : Async<IMutableCloudRef> =
                async {
                    let! t, tag = readInfo container (postfix id)
                    return defineUntyped(t, container, id, tag)
                }

            member self.Dereference(cloudRef : IMutableCloudRef) : Async<obj> = 
                async {
                    let cloudRef' = checkIsValid cloudRef
                    let! ty, value, tag = read cloudRef.Container cloudRef.Name
                    cloudRef'.Tag <- tag
                    return value
                }

            member this.TryUpdate(cloudRef : IMutableCloudRef, value : obj) : Async<bool>  =
                async {
                    let cloudRef' = checkIsValid cloudRef
                    let t = cloudRef.Type
                    let! ok, tag = storeInfo.Store.TryUpdateMutable(cloudRef.Container, postfix cloudRef.Name, 
                                            (fun stream -> async {
                                                Serialization.DefaultPickler.Serialize(stream, t)
                                                Serialization.DefaultPickler.Serialize(stream, value) }),
                                            cloudRef'.Tag)
                    if ok then cloudRef'.Tag <- tag
                    return ok
                }

            member this.ForceUpdate(cloudRef : IMutableCloudRef, value : obj) =
                async {
                    let cloudRef' = checkIsValid cloudRef
                    let t = cloudRef.Type
                    let! tag = storeInfo.Store.ForceUpdateMutable(cloudRef.Container, postfix cloudRef.Name,
                                            (fun stream -> async {
                                                Serialization.DefaultPickler.Serialize(stream, t)
                                                Serialization.DefaultPickler.Serialize(stream, value)}))
                    cloudRef'.Tag <- tag
                }

            member self.GetContainedRefs(container : string) : Async<IMutableCloudRef []> =
                async {
                    let! ids = getIds container
                    return 
                        ids |> Seq.map (fun id -> Async.RunSynchronously(readType container (postfix id)), container, id, "")
                            |> Seq.map defineUntyped
                            |> Seq.toArray
                }

            member self.Delete(mref : IMutableCloudRef) : Async<unit> = 
                let _ = checkIsValid mref
                storeInfo.Store.Delete(mref.Container, postfix mref.Name)