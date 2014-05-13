namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime

    type internal IMutableCloudRefTagged =
        inherit IMutableCloudRef

        abstract Tag : string with get, set

    type internal MutableCloudRef<'T> (id : string, container : string, tag : Tag, provider : MutableCloudRefProvider) =

        interface IMutableCloudRef with
            member self.Name = id
            member self.Container = container
            member self.Type = typeof<'T>
            member self.Dispose () = (provider :> IMutableCloudRefProvider).Delete(self)

        interface IMutableCloudRef<'T>

        interface IMutableCloudRefTagged with
            member val Tag = tag with get, set

        override self.ToString() =  sprintf' "mutablecloudref:%s/%s" container id

        new (info : SerializationInfo, context : StreamingContext) = 
            let id          = info.GetString("id")
            let container   = info.GetString("container")
            let tag         = info.GetString("tag")
            let storeId     = info.GetValue("storeId", typeof<StoreId>) :?> StoreId
            let config =
                match StoreRegistry.TryGetCoreConfiguration storeId with
                | None -> raise <| new MBraceException(sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName)
                | Some config -> config.MutableCloudRefProvider :?> MutableCloudRefProvider

            new MutableCloudRef<'T>(id, container, tag, config)
        
        interface ISerializable with 
            member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)
                info.AddValue("tag", (self :> IMutableCloudRefTagged).Tag)
                info.AddValue("storeId", provider.StoreId, typeof<StoreId>)


    and internal MutableCloudRefProvider(storeInfo : StoreInfo) as self =

        let extension = "mref"
        let postfix = fun s -> sprintf' "%s.%s" s extension

        let read container id : Async<Type * obj * Tag> = async {
                let id = postfix id
                let! s, tag = storeInfo.Store.ReadMutable(container, id)
                use stream = s
                let t = Serialization.DefaultPickler.Deserialize<Type> stream
                let o = Serialization.DefaultPickler.Deserialize<obj> stream
                return t, o, tag
            }

        let readType container id = async {
                let! stream, _ = storeInfo.Store.ReadMutable(container, id)
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
                let! files = storeInfo.Store.GetFiles(container)
                return files 
                        |> Seq.filter (fun w -> w.EndsWith extension)
                        |> Seq.map (fun w -> w.Substring(0, w.Length - extension.Length - 1))
                        |> Seq.toArray
            }

        let defineUntyped (ty : Type, container : string, id : string, tag : string) =
            let cloudRefTy = typedefof<MutableCloudRef<_>>.MakeGenericType [| ty |]
            let cloudRef = Activator.CreateInstance(cloudRefTy, [| id :> obj; container :> obj; tag :> obj ; self :> obj |])
            cloudRef :?> IMutableCloudRef

        member self.StoreId = storeInfo.Id

        member self.GetRefType (container : string, id : string) : Async<Type> =
            readType container (postfix id)

        member self.Exists(container : string) : Async<bool> = 
            storeInfo.Store.Exists(container)

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
                    let cloudRef = cloudRef :?> IMutableCloudRefTagged
                    let! ty, value, tag = read cloudRef.Container cloudRef.Name
                    cloudRef.Tag <- tag
                    return value
                }

            member this.TryUpdate(cloudRef : IMutableCloudRef, value : obj) : Async<bool>  =
                async {
                    let cloudRef = cloudRef :?> IMutableCloudRefTagged
                    let t = cloudRef.Type
                    let! ok, tag = storeInfo.Store.UpdateMutable(cloudRef.Container, postfix cloudRef.Name, 
                                            (fun stream -> async {
                                                Serialization.DefaultPickler.Serialize(stream, t)
                                                Serialization.DefaultPickler.Serialize(stream, value) }),
                                            cloudRef.Tag)
                    if ok then cloudRef.Tag <- tag
                    return ok
                }

            member this.ForceUpdate(cloudRef : IMutableCloudRef, value : obj) =
                async {
                    let cloudRef = cloudRef :?> IMutableCloudRefTagged
                    let t = cloudRef.Type
                    let! tag = storeInfo.Store.ForceUpdateMutable(cloudRef.Container, postfix cloudRef.Name,
                                            (fun stream -> async {
                                                Serialization.DefaultPickler.Serialize(stream, t)
                                                Serialization.DefaultPickler.Serialize(stream, value)}))
                    cloudRef.Tag <- tag
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
                storeInfo.Store.Delete(mref.Container, postfix mref.Name)