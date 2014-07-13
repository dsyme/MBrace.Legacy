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
        static let localCacheStore = ref None
        static let registry = new ConcurrentDictionary<StoreId, StoreInfo> ()

        static let hashAlgorithm = SHA256Managed.Create() :> HashAlgorithm
        static let computeHash (txt : string) = hashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes txt)

        static let activate (definition : StoreDefinition) =
            let factory = Activator.CreateInstance definition.StoreFactoryType :?> ICloudStoreFactory
            factory.CreateStoreFromConnectionString definition.ConnectionString

        static let getLocalCache () =
            match localCacheStore.Value with
            | None -> invalidOp "No local cache store has been registered."
            | Some c -> c

        static member Activate (definition : StoreDefinition, makeDefault) =
            let store = activate definition
            let id = { AssemblyQualifiedName = store.GetType().AssemblyQualifiedName ; UUID = computeHash store.UUID }

            let mkStoreInfo (id : StoreId) =
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

                {
                    Store = store
                    Definition = definition
                    Dependencies = dependencies
                    ActivationInfo = info

                    InMemoryCache = inmem
                    CacheStore = localCache
                }

            let info = registry.GetOrAdd(id, mkStoreInfo)
            if makeDefault then defaultStore := Some info
            info

        static member internal ActivateLocalCacheStore(definition : StoreDefinition) =
            lock localCacheStore (fun () ->
                match localCacheStore.Value with
                | None -> 
                    let cacheStore = activate definition
                    localCacheStore := Some cacheStore

                | Some _ -> invalidOp "A local cache store has already been registered.")

        static member TryActivate(activationInfo : StoreActivationInfo, makeDefault) =
            match StoreRegistry.TryGetStoreInfo activationInfo.Id with
            | Some _ as r -> (if makeDefault then defaultStore := r) ; r
            | None ->
                let definition = 
                    try Some <| Serialization.UnPickle activationInfo.StoreDefinitionPickle
                    with _ -> None

                match definition with
                | None -> None
                | Some d -> Some <| StoreRegistry.Activate(d, makeDefault = makeDefault)

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