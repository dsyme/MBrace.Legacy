namespace Nessos.MBrace.Runtime

    open System
    open System.Collections.Concurrent
    open System.IO
    open System.Reflection
    open System.Runtime.Serialization

    open Nessos.Thespian.ConcurrencyTools

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Store
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.StoreUtils

    type CloudRef<'T> internal (id : string, container : string, storeId : StoreId) as self =

        let provider : Lazy<CloudRefProvider> = lazy CloudRefProvider.GetById storeId

        let mutable value : 'T option = None
        let valueLazy () =
            match value with
            | Some v -> v
            | None ->                
                let v = provider.Value.Dereference self |> Async.RunSynchronously
                value <- Some v
                v
        member __.Name = id
        member __.Container = container

        override self.ToString() = sprintf' "cloudref:%s/%s" container id

        interface ICloudDisposable with
            member self.Dispose () = async {
                do! provider.Value.Delete self
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
            
            new CloudRef<'T>(id, container, storeId)
                    
        interface ISerializable with
            override self.GetObjectData(info : SerializationInfo, _ : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)
                info.AddValue("storeId",storeId, typeof<StoreId>)
    

    and CloudRefProvider private (storeId : StoreId, store : ICloudStore, inmem : InMemoryCache, fscache : LocalCache) =

        static let extension = "ref"
        static let postfix s = sprintf' "%s.%s" s extension

        static let providers = new ConcurrentDictionary<StoreId, CloudRefProvider> ()

        // TODO 1 : refine exception handling for reads
        // TODO 2 : decide if file is cloud ref by reading header, not file extension.

        static let serialize (value:obj) (t:Type) (stream:Stream) = async {
            Serialization.DefaultPickler.Serialize<Type>(stream, t, leaveOpen = true)
            Serialization.DefaultPickler.Serialize<obj>(stream, value)
        }

        let storeHash = String.Convert.BytesToBase32(storeId.UUID)

        let read container id : Async<Type * obj> = async {
            use! stream = fscache.Read(container, id) 
            let t = Serialization.DefaultPickler.Deserialize<Type>(stream, leaveOpen = true)
            let o = Serialization.DefaultPickler.Deserialize<obj>(stream)
            return t, o
        }

        let readType container id  = async {
            use! stream = store.ReadImmutable(container, id) // force cache loading?
            let t = Serialization.DefaultPickler.Deserialize<Type>(stream)
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
            let existential = Existential.Create ty
            let ctor =
                {
                    new IFunc<ICloudRef> with
                        member __.Invoke<'T> () = new CloudRef<'T>(id, container, storeId) :> ICloudRef
                }

            existential.Apply ctor

        static member internal Create(storeId : StoreId, store : ICloudStore, inmem : InMemoryCache, fscache : LocalCache) =
            providers.GetOrAdd(storeId, fun id -> new CloudRefProvider(storeId, store, inmem, fscache))

        static member internal GetById(storeId : StoreId) =
            let ok, provider = providers.TryGetValue storeId
            if ok then provider
            else
                raise <| new MBraceException(sprintf "No configuration for store '%s' has been activated." storeId.AssemblyQualifiedName)

        member self.Delete<'T> (cloudRef : CloudRef<'T>) : Async<unit> =
            async {
                let id = postfix cloudRef.Name
                do inmem.DeleteIfExists(id) // delete from fscache?
                return! store.Delete(cloudRef.Container, id)
            } |> onDeleteError cloudRef

        member self.Dereference<'T> (cref : CloudRef<'T>) : Async<'T> = 
            async {
                let inmemCacheId = sprintf' "%s/%s/%s" storeHash cref.Container cref.Name

                // get value
                let cacheResult = inmem.TryFind inmemCacheId
                match cacheResult with
                | Some result ->
                    let _,value = result :?> Type * obj
                    return value :?> 'T
                | None ->
                    let! ty, value = read cref.Container <| postfix cref.Name
                    if typeof<'T> <> ty then 
                        let msg = sprintf' "CloudRef type mismatch. Internal type %s, expected %s." ty.FullName typeof<'T>.FullName
                        return! Async.Raise <| StoreException(msg)
                    // update cache
                    inmem.Set(inmemCacheId, (ty, value))
                    return value :?> 'T
            } |> onDereferenceError cref

        member self.Create (container : string, id : string, value : 'T) : Async<ICloudRef<'T>> = 
            async {
                do! fscache.Create(container, postfix id, serialize value typeof<'T>, false)
                return new CloudRef<'T>(id, container, storeId) :> ICloudRef<_>
            } |> onCreateError container id

        member self.Create (container : string, id : string, t : Type, value : obj) : Async<ICloudRef> = 
            async {
                do! fscache.Create(container, postfix id, serialize value t, false)
                return defineUntyped(t, container, id)
            } |> onCreateError container id

        member self.GetExisting(container, id) : Async<ICloudRef> =
            async {
                let! t = readType container (postfix id)
                return defineUntyped(t, container, id)
            } |> onGetError container id

        member self.GetContainedRefs(container : string) : Async<ICloudRef []> =
            async {
                let! ids = getIds container

                return!
                    ids 
                    |> Array.map (fun id -> self.GetExisting(container,id))
                    |> Async.Parallel
            } |> onListError container