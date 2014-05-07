namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils

    type MutableCloudRef<'T>(id : string, container : string, tag : Tag, ty : Type) =
        
        let mutablecloudrefstorelazy = lazy IoC.Resolve<MutableCloudRefStore>()

        interface IMutableCloudRef with
            member self.Name = id
            member self.Container = container
            member self.Type = ty
            member self.Dispose () =
                (mutablecloudrefstorelazy.Value :> IMutableCloudRefProvider).Delete(self)

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


    and MutableCloudRefStore(store : IStore) =

        let pickler = Nessos.MBrace.Runtime.Serializer.Pickler

        let extension = "mref"
        let postfix = fun s -> sprintf' "%s.%s" s extension

        let read container id : Async<Type * obj * Tag> = async {
                let id = postfix id
                let! s, tag = store.ReadMutable(container, id)
                use stream = s
                let t = pickler.Deserialize<Type> stream
                let o = pickler.Deserialize<obj> stream
                return t, o, tag
            }

        let readType container id = async {
                let! stream, _ = store.ReadMutable(container, id)
                let t = pickler.Deserialize<Type> stream
                stream.Dispose()
                return t
            }

        let readInfo container id = async {
            let! stream, tag = store.ReadMutable(container, id)        
            let t = pickler.Deserialize<Type> stream
            stream.Dispose()
            return t, tag
        }

        let getIds (container : string) : Async<string []> = async {
                let! files = store.GetFiles(container)
                return files 
                       |> Seq.filter (fun w -> w.EndsWith extension)
                       |> Seq.map (fun w -> w.Substring(0, w.Length - extension.Length - 1))
                       |> Seq.toArray
            }

        member self.GetRefType (container : string, id : string) : Async<Type> =
            readType container (postfix id)

        member self.Exists(container : string) : Async<bool> = 
            store.Exists(container)

        member self.Exists(container : string, id : string) : Async<bool> = 
            store.Exists(container, postfix id)

        interface IMutableCloudRefProvider with

            member self.Create (container : string, id : string, value : obj, t : Type) : Async<IMutableCloudRef> = 
                async {
                    let! tag = store.CreateMutable(container, postfix id, 
                                fun stream -> async {
                                    pickler.Serialize(stream, t)
                                    pickler.Serialize(stream, value) })

                    let cloudRefType = typedefof<MutableCloudRef<_>>.MakeGenericType [| t |]
                    let cloudRef = Activator.CreateInstance(cloudRefType, [| id :> obj; container :> obj; tag :> obj; t :> obj |])
                    return cloudRef :?> _
                }

            member self.Read(cloudRef : IMutableCloudRef) : Async<obj> = 
                async {
                    let cloudRef = cloudRef :?> IMutableCloudRefTagged
                    let! ty, value, tag = read cloudRef.Container cloudRef.Name
                    cloudRef.Tag <- tag
                    return value
                }

            member this.Update(cloudRef : IMutableCloudRef, value : obj) : Async<bool>  =
                async {
                    let cloudRef = cloudRef :?> IMutableCloudRefTagged
                    let t = cloudRef.Type
                    let! ok, tag = store.UpdateMutable(cloudRef.Container, postfix cloudRef.Name, 
                                            (fun stream -> async {
                                                pickler.Serialize(stream, t)
                                                pickler.Serialize(stream, value) }),
                                            cloudRef.Tag)
                    if ok then cloudRef.Tag <- tag
                    return ok
                }

            member this.ForceUpdate(cloudRef : IMutableCloudRef, value : obj) =
                async {
                    let cloudRef = cloudRef :?> IMutableCloudRefTagged
                    let t = cloudRef.Type
                    let! tag = store.ForceUpdateMutable(cloudRef.Container, postfix cloudRef.Name,
                                            (fun stream -> async {
                                                pickler.Serialize(stream, t)
                                                pickler.Serialize(stream, value)}))
                    cloudRef.Tag <- tag
                }

            member self.GetRef(container , id) : Async<IMutableCloudRef> =
                async {
                    let! t, tag = readInfo container (postfix id)
                    let cloudRefType = typedefof<MutableCloudRef<_>>.MakeGenericType [| t |]
                    let cloudRef = Activator.CreateInstance(cloudRefType, [| id :> obj; container :> obj; tag :> obj; t :> obj |])
                    return cloudRef :?> _
                }

            member self.GetRefs(container : string) : Async<IMutableCloudRef []> =
                async {
                    let! ids = getIds container
                    return 
                        ids |> Seq.map (fun id -> Async.RunSynchronously(readType container (postfix id)), container, id)
                            |> Seq.map (fun (t,c,i) ->
                                    let cloudRefTy = typedefof<MutableCloudRef<_>>.MakeGenericType [| t |]
                                    let cloudRef = Activator.CreateInstance(cloudRefTy, [| i :> obj; c :> obj; "" :> obj; t :> obj; |])
                                    cloudRef :?> IMutableCloudRef)
                            |> Seq.toArray
                }

            member self.Delete(mref : IMutableCloudRef) : Async<unit> = 
                store.Delete(mref.Container, postfix mref.Name)