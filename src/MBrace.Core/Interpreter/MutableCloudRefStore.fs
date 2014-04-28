namespace Nessos.MBrace.Core

    open System
    open System.IO
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Store
    open Nessos.MBrace.Caching

    open Nessos.FsPickler

    type MutableCloudRefStore(store : IStore, ?logger : ILogger) =

        let pickler = Nessos.MBrace.Runtime.Serializer.Pickler
        let logger = match logger with Some logger -> logger | None -> IoC.Resolve<ILogger>()

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

        let getIds (container : string) : Async<string []> = async {
                let! files = store.GetFiles(container)
                return files 
                       |> Seq.filter (fun w -> w.EndsWith extension)
                       |> Seq.map (fun w -> w.Substring(0, w.Length - extension.Length - 1))
                       |> Seq.toArray
            }

        interface IMutableCloudRefStore with

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

            member self.Create<'T> (container : string, id : string, value : 'T) : Async<IMutableCloudRef<'T>> = 
                async {
                    let! cloudRef = (self :> IMutableCloudRefStore).Create(container, id, value :> obj, typeof<'T>) 
                    return cloudRef :?> IMutableCloudRef<'T>
                }

            member self.Read(cloudRef : IMutableCloudRef) : Async<obj> = 
                async {
                    let cloudRef = cloudRef :?> IMutableCloudRefTagged
                    let! ty, value, tag = read cloudRef.Container cloudRef.Name
                    cloudRef.Tag <- tag
                    return value
                }
  
            member self.Read<'T> (cloudRef : IMutableCloudRef<'T>) : Async<'T> = 
                async {
                    let! value = (self :> IMutableCloudRefStore).Read(cloudRef :> IMutableCloudRef) 
                    return value :?> 'T
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

            member this.Update<'T>(cloudRef : IMutableCloudRef<'T>, value : 'T) : Async<bool>  =
                (this :> IMutableCloudRefStore).Update(cloudRef :> IMutableCloudRef, value)

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

            member self.GetRefType (container : string, id : string) : Async<Type> =
                readType container (postfix id)

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

            member self.Exists(container : string) : Async<bool> = 
                store.Exists(container)

            member self.Exists(container : string, id : string) : Async<bool> = 
                store.Exists(container, postfix id)

            member self.Delete(container : string, id : string) : Async<unit> = 
                store.Delete(container, postfix id)