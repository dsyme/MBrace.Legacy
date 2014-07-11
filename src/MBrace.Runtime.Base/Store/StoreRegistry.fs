namespace Nessos.MBrace.Runtime
    
    open System
    open System.IO
    open System.Collections.Concurrent
    open System.Runtime
    open System.Reflection
    open System.Security.Cryptography
    open System.Text

    open Nessos.Vagrant

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Store

    [<AutoSerializable(true) ; StructuralEquality ; StructuralComparison>]
    type StoreId = 
        {
            AssemblyQualifiedName : string
            UUID                  : byte []
        }
    with 
        override this.ToString () = sprintf "StoreId:%s" this.AssemblyQualifiedName

    [<AutoSerializable(true) ; NoEquality ; NoComparison>]
    type StoreActivationInfo =
        {
            Id : StoreId
            FactoryQualifiedName : string
            ConnectionString : string
            Dependencies : AssemblyId list
        }

//    // TODO : add LocalCache and Cached store instances here
//    [<AutoSerializable(false) ; NoEquality ; NoComparison>]
//    type LocalCacheInfo =
//        {
//            Store : ICloudStore
//            ActivationInfo : StoreActivationInfo
//            InMemoryCache : InMemoryCache
//        }

    [<AutoSerializable(false) ; NoEquality ; NoComparison>]
    type StoreInfo = 
        {
            Store : ICloudStore
            Definition : StoreDefinition
            Dependencies : Map<AssemblyId, Assembly>
            ActivationInfo : StoreActivationInfo
            
            // TODO : make optional
            InMemoryCache : InMemoryCache
            CacheStore : LocalCache
        }
    with
        member __.Id = __.ActivationInfo.Id

    // TODO : handle all dependent assemblies
    and StoreRegistry private () =

        static let defaultStore = ref None
        static let localCacheStore = ref None
        static let registry = new ConcurrentDictionary<StoreId, StoreInfo> ()

        static let hashAlgorithm = SHA256Managed.Create() :> HashAlgorithm
        static let computeHash (txt : string) = hashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes txt)

        static let activate (definition : StoreDefinition) =
            let factory = Activator.CreateInstance definition.StoreFactoryType :?> ICloudStoreFactory
            factory.CreateStoreFromConnectionString definition.ConnectionString

        static member Activate (definition : StoreDefinition, cacheStore : StoreDefinition, makeDefault) =
            let store = activate definition
            let id = { AssemblyQualifiedName = store.GetType().AssemblyQualifiedName ; UUID = computeHash store.UUID }

            let mkStoreInfo (id : StoreId) =
                let assemblies = VagrantRegistry.ComputeDependencies definition.StoreFactoryType

                let ids = assemblies |> List.map VagrantUtils.ComputeAssemblyId
                let dependencies = Seq.zip ids assemblies |> Map.ofSeq

                let info = 
                    { 
                        Id = id
                        FactoryQualifiedName = definition.StoreFactoryQualifiedName
                        ConnectionString = definition.ConnectionString
                        Dependencies = ids
                    }

                let inmem = new InMemoryCache()
                let cacheStore = activate cacheStore
                let localCache = new LocalCache(sprintf "fscache-%d" <| hash id, cacheStore, store)

                {
                    Store = store
                    Definition = definition
                    Dependencies = dependencies
                    ActivationInfo = info

                    InMemoryCache = new InMemoryCache()
                    CacheStore = localCache
                }

            let info = registry.GetOrAdd(id, mkStoreInfo)
            if makeDefault then defaultStore := Some info
            info

        static member ActivateLocalCacheStore(definition : StoreDefinition) =
            lock localCacheStore (fun () ->
                let cacheStore = 
            )



//        static member internal Register(store : StoreInfo, ?makeDefault) =
//            registry.[store.ActivationInfo.Id] <- store
//            if defaultArg makeDefault false then
//                defaultStore := Some store

        static member TryGetStoreDefinition(info : StoreActivationInfo) =
            match Type.GetType(info.FactoryQualifiedName, throwOnError = false) with
            | null -> None
            | factoryType -> Some <| StoreDefinition.Create(factoryType, info.ConnectionString)



        static member TryActivate(activationInfo : StoreActivationInfo, makeDefault) =
            match StoreRegistry.TryGetStoreInfo activationInfo.Id with
            | Some info as r -> StoreRegistry.Activate(info, makeDefault = makeDefault) ; r
            | None ->
                match StoreRegistry.TryGetStoreProvider activationInfo with
                | Some p -> StoreRegistry.Activate(p, ?makeDefault = makeDefault) |> Some
                | None -> None

//        static member ActivateLocalCache(cacheDefinition : StoreDefinition) =
//            lock localCache (fun () ->
//                let info = initStore cacheDefinition
//                let cacheInfo = { ActivationInfo = info.ActivationInfo ; Store = info.Store ; InMemoryCache = new InMemoryCache() }
//                localCache := Some cacheInfo)
//
//        static member LocalCache =
//            match localCache.Value with
//            | None -> invalidOp "a local cache has not been registered."
//            | Some lc -> lc
//
//        static member DefaultStoreInfo = 
//            match defaultStore.Value with
//            | None -> invalidOp "a default store has not been registered."
//            | Some ds -> ds

        static member TryGetStoreInfo id = registry.TryFind id

        static member GetStoreInfo id =
            let ok, info = registry.TryGetValue id
            if ok then info
            else
                invalidOp <| sprintf "A store with id '%O' has not been registered." id