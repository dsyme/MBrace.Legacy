namespace Nessos.MBrace.Runtime.Store

    open System
    open System.IO
    open System.Reflection
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Core

    type CloudRef<'T> internal (id : string, container : string, provider : CloudRefProvider) as self =

        let mutable value : 'T option = None
        let valueLazy () =
            match value with
            | Some v -> v
            | None ->                
                let v = provider.Dereference self |> Async.RunSynchronously
                value <- Some v
                v

        member __.Name = id
        member __.Container = container

        override self.ToString() = sprintf' "cloudref:%s/%s" container id

        interface ICloudDisposable with
            member self.Dispose () = async {
                do! provider.Delete self
                do value <- None // remove cached value to force exceptions for current ref instance
            }

        interface ICloudRef<'T> with
            member __.Name = id
            member __.Container = container
            member __.Type = typeof<'T>
            member __.Value : obj = valueLazy () :> obj
            member __.Value : 'T = valueLazy ()
            member __.TryValue = try Some (valueLazy ()) with _ -> None

        new (info : SerializationInfo, _ : StreamingContext) = 
            let id = info.GetString("id")
            let container = info.GetString("container")
            let storeId = info.GetValue("storeId", typeof<StoreId>) :?> StoreId
            let provider =
                match StoreRegistry.TryGetCoreConfiguration storeId with
                | None -> raise <| new MBraceException(sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName)
                | Some config -> config.CloudRefProvider :?> CloudRefProvider
            
            new CloudRef<'T>(id, container, provider)
                    
        interface ISerializable with
            override self.GetObjectData(info : SerializationInfo, _ : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)
                info.AddValue("storeId", provider.StoreId, typeof<StoreId>)
    

    and internal CloudRefProvider(storeInfo : StoreInfo, cache : LocalObjectCache) as self =

        let store = storeInfo.Store
        let storeId = storeInfo.Id

        static let extension = "ref"
        static let postfix s = sprintf' "%s.%s" s extension

        // TODO 1 : refine exception handling for reads
        // TODO 2 : decide if file is cloud ref by reading header, not file extension.

        static let serialize (value:obj) (t:Type) (stream:Stream) = async {
            Serialization.DefaultPickler.Serialize<Type>(stream, t)
            Serialization.DefaultPickler.Serialize<obj>(stream, value)
        }

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
                return 
                    files
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

        member self.Delete<'T> (cloudRef : CloudRef<'T>) : Async<unit> =
            async {
                let id = postfix cloudRef.Name
                let! containsKey = cache.ContainsKey id
                if containsKey then
                    do! cache.Delete(id)

                return! store.Delete(cloudRef.Container, id)
            }

        member self.Dereference<'T> (cref : CloudRef<'T>) : Async<'T> = 
            async {
                let cacheId = sprintf' "%s/%s" cref.Container cref.Name

                // get value
                let! cacheResult = cache.TryFind cacheId
                match cacheResult with
                | Some result ->
                    let _,value = result :?> Type * obj
                    return value :?> 'T
                | None ->
                    let! ty, value = read cref.Container <| postfix cref.Name
                    if typeof<'T> <> ty then 
                        let msg = sprintf' "CloudRef type mismatch. Internal type %s, expected %s." ty.FullName typeof<'T>.FullName
                        raise <| StoreException(msg)
                    // update cache
                    cache.Set(cacheId, (ty, value))
                    return value :?> 'T
            }

        interface ICloudRefProvider with

            member self.Create (container : string, id : string, value : 'T) : Async<ICloudRef<'T>> = 
                async {
                    do! store.CreateImmutable(container, postfix id, serialize value typeof<'T>, false)

                    return new CloudRef<'T>(id, container, self) :> _
            }

            member self.Create (container : string, id : string, t : Type, value : obj) : Async<ICloudRef> = 
                async {
                    do! store.CreateImmutable(container, postfix id, serialize value t, false)

                    // construct & return
                    return defineUntyped(t, container, id)
            }

            member self.GetExisting(container, id) : Async<ICloudRef> =
                async {
                    let! t = readType container (postfix id)
                    return defineUntyped(t, container, id)
                }

            member self.GetContainedRefs(container : string) : Async<ICloudRef []> =
                async {
                    let! ids = getIds container

                    return!
                        ids 
                        |> Array.map (fun id -> (self :> ICloudRefProvider).GetExisting(container,id))
                        |> Async.Parallel
                }