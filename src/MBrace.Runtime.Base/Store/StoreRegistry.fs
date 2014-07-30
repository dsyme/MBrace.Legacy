namespace Nessos.MBrace.Runtime
    
    open System
    open System.IO
    open System.Collections.Concurrent
    open System.Runtime
    open System.Reflection

    open Nessos.Vagrant

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Store

    [<AutoSerializable(true) ; NoEquality ; NoComparison>]
    type StoreActivationInfo =
        {
            Id : StoreId
            StoreDefinitionPickle : Pickle<StoreDefinition>
            Dependencies : AssemblyId list
        }

    [<AutoSerializable(false) ; NoEquality ; NoComparison>]
    type StoreInfo = 
        {
            Store : ICloudStore
            Definition : StoreDefinition
            Dependencies : Map<AssemblyId, Assembly>
            ActivationInfo : StoreActivationInfo

            CloudRefProvider : CloudRefProvider
            CloudFileProvider : CloudFileProvider
            CloudSeqProvider : CloudSeqProvider
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

        static let activate (definition : StoreDefinition) =
            let factory = Activator.CreateInstance definition.StoreFactoryType :?> ICloudStoreFactory
            factory.CreateStoreFromConnectionString definition.ConnectionString

        static let getLocalCache () =
            match localCacheStore.Value with
            | None -> invalidOp "No local cache store has been registered."
            | Some c -> c

        static member Activate (definition : StoreDefinition, makeDefault) =
            let mkStoreInfo (id : StoreId) =

                let store = activate definition
                let assemblies = VagrantRegistry.ComputeDependencies definition.StoreFactoryType

                let ids = assemblies |> List.map VagrantUtils.ComputeAssemblyId
                let dependencies = Seq.zip ids assemblies |> Map.ofSeq

                let info = 
                    { 
                        Id = id
                        StoreDefinitionPickle = Serialization.Pickle definition
                        Dependencies = ids
                    }

                let inmem = new InMemoryCache()
                let cacheStore = getLocalCache()
                let localCache = new CacheStore(sprintf "fscache-%d" <| hash id, cacheStore, store)

                let cRefProvider = CloudRefProvider.Create(info.Id, store, inmem, localCache)
                let cFileProvider = CloudFileProvider.Create(info.Id, store, localCache)
                let mRefProvider = MutableCloudRefProvider.Create(info.Id, store)
                let cSeqprovider = CloudSeqProvider.Create(info.Id, store, localCache)

                {
                    Store = store
                    Definition = definition
                    Dependencies = dependencies
                    ActivationInfo = info

                    CloudRefProvider = cRefProvider
                    CloudFileProvider = cFileProvider
                    MutableCloudRefProvider = mRefProvider
                    CloudSeqProvider = cSeqprovider

                    InMemoryCache = inmem
                    CacheStore = localCache
                }

            let info = registry.GetOrAdd(definition.Id, mkStoreInfo)
            if makeDefault then defaultStore := Some info
            info

        static member internal ActivateLocalCacheStore(definition : StoreDefinition) =
            lock localCacheStore (fun () ->
                match localCacheStore.Value with
                | None -> 
                    let cacheStore = activate definition
                    localCacheStore := Some cacheStore

                | Some _ -> invalidOp "A local cache store has already been registered.")

        static member Activate(activationInfo : StoreActivationInfo, makeDefault) =
            match StoreRegistry.TryGetStoreInfo activationInfo.Id with
            | Some info as r -> (if makeDefault then defaultStore := r) ; info
            | None ->
                try
                    let definition = Serialization.UnPickle activationInfo.StoreDefinitionPickle
                    StoreRegistry.Activate(definition, makeDefault = makeDefault)
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