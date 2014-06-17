namespace Nessos.MBrace.Client

    open System
    open System.Reflection
    open System.IO

    open Nessos.MBrace.Runtime.Store
    
    type ICloudStore = Nessos.MBrace.Runtime.Store.ICloudStore
    type ICloudStoreFactory = Nessos.MBrace.Runtime.Store.ICloudStoreFactory

    /// Represents the storage provider used by CloudRefs etc.
    /// This can be the the local filesystem (for local usage),
    /// a shared filesystem (like a UNC path)
    /// or any custom provider that implements the ICloudStore interface.
    type StoreProvider private (factoryType : Type, connectionString : string) =

        member __.StoreFactoryQualifiedName = factoryType.AssemblyQualifiedName
        member __.StoreFactoryType = factoryType
        member __.ConnectionString = connectionString

        /// Defines a new store provider
        static member Define<'Factory when 'Factory :> ICloudStoreFactory>(connectionString : string) =
            new StoreProvider(typeof<'Factory>, connectionString)

        /// Create a StoreProvider object from the storeProvider, storeEndpoint configuration.
        static member Parse(storeFactoryQualifiedName : string, connectionString : string) =
            match storeFactoryQualifiedName with
            | "LocalFS" -> StoreProvider.LocalFS
            | "FileSystem" -> StoreProvider.FileSystem connectionString
            | _ ->
                let factoryType = Type.GetType(storeFactoryQualifiedName, throwOnError = true)
                if typeof<ICloudStoreFactory>.IsAssignableFrom factoryType then
                    new StoreProvider(factoryType, connectionString)
                else
                    invalidArg "storeFactoryQualifiedName" "Type is not a store factory"

        /// A store provider using the file system with an endpoint being either a
        /// path in the local file system, or a UNC path.
        static member FileSystem (path : string) = StoreProvider.Define<FileSystemStoreFactory>(path)

        /// A store provider using the local file system (and a folder in the users temp path).
        /// Any endpoint given will be ignored.
        static member LocalFS = StoreProvider.FileSystem(Path.Combine(Path.GetTempPath(), "mbrace-localfs"))



namespace Nessos.MBrace.Runtime.Store
    
    open System
    open System.Collections.Concurrent
    open System.Runtime
    open System.Reflection
    open System.Security.Cryptography
    open System.Text

    open Nessos.Thespian.ConcurrencyTools

    open Nessos.MBrace.Core
    open Nessos.MBrace.Client
    open Nessos.MBrace.Utils

    [<StructuralEquality ; StructuralComparison>]
    type StoreId = 
        {
            AssemblyQualifiedName : string
            UUID                  : byte []
        }

    with override this.ToString () = sprintf "StoreId:%s" this.AssemblyQualifiedName

    // TODO : add LocalCache and Cached store instances here
    type LocalCacheInfo =
        {
            Id : StoreId
            Provider : StoreProvider
            Store : ICloudStore
            InMemoryCache : InMemCache
        }

    [<AutoSerializable(false)>]
    type StoreInfo = 
        {
            Id : StoreId
            Provider : StoreProvider
            Dependencies : Assembly list
            Store : ICloudStore
            Primitives : PrimitiveConfiguration
        }

//    [<Serializable>]
//    type StoreActivator internal (id : StoreId, dependencies : AssemblyId list, provider : StoreProvider) =
//        
//        member __.Id = id
//        member __.Dependencies = dependencies
//        member __.Provider = provider

    // TODO : handle all dependent assemblies
    and StoreRegistry private () =

        static let defaultStore = ref None
        static let localCache = ref None
        static let registry = new ConcurrentDictionary<StoreId, StoreInfo> ()

        static let hashAlgorithm = SHA256Managed.Create() :> HashAlgorithm
        static let computeHash (txt : string) = hashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes txt)

//        static member SetVagrantServer(server : VagrantServer) = 
//            lock vagrantServer (fun () -> vagrantServer := Choice2Of3 server)
//
//        static member SetVagrantCache(cache : VagrantCache) =
//            lock vagrantServer (fun () -> vagrantServer := Choice3Of3 cache)

//        static member TryGetStoreActivator(id : StoreId) =
//            match registry.TryFind id with
//            | None -> None
//            | Some info ->
//                let dependencies =
//                    match vagrantServer.Value with
//                    | None -> VagrantUtils.ComputeAssemblyDependencies info.Provider
//                    | Some v -> v.ComputeObjectDependencies(info.Provider, permitCompilation = true)
//                    |> List.map VagrantUtils.ComputeAssemblyId
//
//                let sa = new StoreActivator(id, dependencies, info.Provider)
//                Some sa

        static member IsAvailableStoreProvider(id : StoreId) =
            Type.GetType(id.AssemblyQualifiedName, throwOnError = false) <> null

        static member internal InitStore(provider : StoreProvider) =
            let factory = Activator.CreateInstance provider.StoreFactoryType :?> ICloudStoreFactory
            let store = factory.CreateStoreFromConnectionString provider.ConnectionString
            let id = { AssemblyQualifiedName = store.GetType().AssemblyQualifiedName ; UUID = computeHash store.UUID }
            id, store

        static member internal Register(store : StoreInfo, ?makeDefault) : bool =
            let success = registry.TryAdd(store.Id, store)
            if success && defaultArg makeDefault false then
                lock defaultStore (fun () -> defaultStore := Some store)
            success

        static member ActivateLocalCache(provider : StoreProvider) =
            lock localCache (fun () ->
                let id, store = StoreRegistry.InitStore provider
                let info = { Id = id ; Provider = provider ; Store = store ; InMemoryCache = InMemCache() }
                localCache := Some info)

        static member TryGetLocalCache () = localCache.Value
        static member LocalCache =
            match localCache.Value with
            | None -> invalidOp "a local cache has not been registered."
            | Some lc -> lc

        static member TryGetDefaultStoreInfo () = defaultStore.Value
        static member DefaultStoreInfo = 
            match defaultStore.Value with
            | None -> invalidOp "a default store has not been registered."
            | Some ds -> ds

        static member TryGetStoreInfo id = registry.TryFind id