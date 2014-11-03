namespace Nessos.MBrace.Runtime
    
    open Nessos.MBrace.Store
    open Nessos.MBrace.Utils
    open Nessos.Vagrant
    open System.Collections.Concurrent
    open System.Reflection

    [<AutoSerializable(true) ; NoEquality ; NoComparison>]
    type StoreActivationInfo =
        {
            Id : StoreId
            StoreConfigurationPickle : Pickle<ICloudStoreConfiguration>
            Dependencies : AssemblyId list
        }

    [<AutoSerializable(false) ; NoEquality ; NoComparison>]
    type StoreInfo = 
        {
            Store : ICloudStore
            Configuration : ICloudStoreConfiguration
            Dependencies : Map<AssemblyId, Assembly>
            ActivationInfo : StoreActivationInfo

            CloudRefProvider : CloudRefProvider
            CloudFileProvider : CloudFileProvider
            CloudSeqProvider : CloudSeqProvider
            CloudArrayProvider : CloudArrayProvider
            MutableCloudRefProvider : MutableCloudRefProvider

            // TODO : investigate whether inmem cache should be
            // global or restricted to particular stores
            InMemoryCache : InMemoryCache
            CacheStore : CacheStore
        }
    with
        member __.Id = __.ActivationInfo.Id

    // TODO : handle all dependent assemblies
    and StoreRegistry private () =

        static let defaultStore = ref None
        static let localCacheStore : ICloudStore option ref = ref None
        static let registry = new ConcurrentDictionary<StoreId, StoreInfo> ()

        static let getLocalCache () =
            match localCacheStore.Value with
            | None -> invalidOp "No local cache store has been registered."
            | Some c -> c

        static member Register (store : ICloudStore, makeDefault) =
            let mkStoreInfo (id : StoreId) =
                let storeConfig = store.GetStoreConfiguration()
                let assemblies = VagrantRegistry.ComputeDependencies storeConfig
                let ids = assemblies |> List.map VagrantUtils.ComputeAssemblyId
                let dependencies = Seq.zip ids assemblies |> Map.ofSeq

                let info = 
                    { 
                        Id = id
                        StoreConfigurationPickle = Serialization.Pickle storeConfig
                        Dependencies = ids
                    }

                let inmem = new InMemoryCache()
                let cacheStore = getLocalCache()
                let localCache = new CacheStore(sprintf "fscache-%d" <| hash id, cacheStore, store)

                let cRefProvider   = CloudRefProvider.Create(id, store, inmem, localCache)
                let cFileProvider  = CloudFileProvider.Create(id, store, localCache)
                let mRefProvider   = MutableCloudRefProvider.Create(id, store)
                let cSeqprovider   = CloudSeqProvider.Create(id, store, localCache)
                let cArrayProvider = CloudArrayProvider.Create(id, store, localCache)

                {
                    Store = store
                    Configuration = storeConfig
                    Dependencies = dependencies
                    ActivationInfo = info

                    CloudRefProvider        = cRefProvider
                    CloudFileProvider       = cFileProvider
                    CloudSeqProvider        = cSeqprovider
                    CloudArrayProvider      = cArrayProvider
                    MutableCloudRefProvider = mRefProvider

                    InMemoryCache = inmem
                    CacheStore = localCache
                }

            let id = StoreId.Generate store
            let info = registry.GetOrAdd(id, mkStoreInfo)
            if makeDefault then defaultStore := Some info
            info

        static member internal ActivateLocalCacheStore(cacheStore : ICloudStore) =
            lock localCacheStore (fun () ->
                match localCacheStore.Value with
                | None -> localCacheStore := Some cacheStore
                | Some _ -> invalidOp "A local cache store has already been registered.")

        static member Activate(activationInfo : StoreActivationInfo, makeDefault) =
            match StoreRegistry.TryGetStoreInfo activationInfo.Id with
            | Some info as r -> (if makeDefault then defaultStore := r) ; info
            | None ->
                try
                    let definition = Serialization.UnPickle activationInfo.StoreConfigurationPickle
                    let store = definition.Init()
                    StoreRegistry.Register(store, makeDefault = makeDefault)
                with e ->
                    let msg = sprintf "Failed to activate store '%s'." activationInfo.Id.AssemblyQualifiedName
                    raise <| new Nessos.MBrace.StoreException(msg, e)

        static member DefaultStoreInfo = 
            match defaultStore.Value with
            | None -> invalidOp "a default store has not been registered."
            | Some ds -> ds

        static member TryGetDefaultStoreInfo () = defaultStore.Value

        static member TryGetStoreInfo id = registry.TryFind id

        static member GetStoreInfo id =
            let ok, info = registry.TryGetValue id
            if ok then info
            else
                invalidOp <| sprintf "A store with id '%O' has not been registered." id