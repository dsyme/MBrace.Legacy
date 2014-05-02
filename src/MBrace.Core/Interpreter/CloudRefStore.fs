namespace Nessos.MBrace.Core

    open System
    open System.IO
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Caching
    

    type CloudRefStore(store : IStore, ?cache : Cache, ?logger : ILogger) =
        let pickler = Nessos.MBrace.Runtime.Serializer.Pickler
        let cache = match cache with Some cache -> cache | None -> new Cache()
        let logger = match logger with Some logger -> logger | None -> IoC.Resolve<ILogger>()

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

        interface ICloudRefStore with

            member self.Create (container : string, id : string, value : obj, t : Type) : Async<IPersistableCloudRef> = 
                async {
                    do! store.Create(container, postfix id, 
                            fun stream -> async {
                                pickler.Serialize(stream, t)
                                pickler.Serialize(stream, value) })

                    // construct & return

                    let cloudRefType = typedefof<PersistableCloudRef<_>>.MakeGenericType [| t |]
                    let cloudRef = Activator.CreateInstance(cloudRefType, [| id :> obj; container :> obj; t :> obj |])
                    return cloudRef :?> _
            }

            member self.Create<'T> (container : string, id : string, value : 'T) : Async<IPersistableCloudRef<'T>> = 
                async {                    
                    let! cloudRef = (self :> ICloudRefStore).Create(container, id, value :> obj, typeof<'T>) 
                    return cloudRef :?> IPersistableCloudRef<'T>
                }

            member self.Read (container : string, id : string, t : Type) = 
                async {
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

            member self.GetRefType (container : string, id : string) : Async<Type> =
                readType container (postfix id)

            member self.GetRefs(container : string) : Async<ICloudRef []> =
                async {
                    let! ids = getIds container
                    return
                        ids |> Seq.map (fun id -> Async.RunSynchronously(readType container (postfix id)), container, id)
                            |> Seq.map (fun (t,c,i) ->
                                    let cloudRefTy = typedefof<PersistableCloudRef<_>>.MakeGenericType [| t |]
                                    let cloudRef = Activator.CreateInstance(cloudRefTy, [| i :> obj; c :> obj; t :> obj |])
                                    cloudRef :?> ICloudRef)
                            |> Seq.toArray
                }

            member self.Read(cloudRef : IPersistableCloudRef) : Async<obj> = 
                (self :> ICloudRefStore).Read(cloudRef.Container, cloudRef.Name, cloudRef.Type) 
                
            member self.Read<'T> (cloudRef : IPersistableCloudRef<'T>) : Async<'T> = 
                async {
                    let! value = (self :> ICloudRefStore).Read(cloudRef.Container, cloudRef.Name, cloudRef.Type) 
                    return value :?> 'T
                }

            member self.Exists(container : string) : Async<bool> = 
                store.Exists(container)

            member self.Exists(container : string, id : string) : Async<bool> = 
                store.Exists(container, postfix id)

            member self.Delete(container : string, id : string) : Async<unit> = 
                async {
                    let id = postfix id
                    if cache.ContainsKey(id) then
                        cache.Delete(id)
                    return! store.Delete(container, id)
                }