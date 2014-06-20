namespace Nessos.MBrace.Runtime.Store
    
    open System
    open System.IO
    open System.Collections.Concurrent
    open System.Runtime
    open System.Reflection
    open System.Security.Cryptography
    open System.Text

    open Nessos.Vagrant
    open Nessos.Thespian.ConcurrencyTools

    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Client

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

    // TODO : add LocalCache and Cached store instances here
    [<AutoSerializable(false) ; NoEquality ; NoComparison>]
    type LocalCacheInfo =
        {
            Store : ICloudStore
            ActivationInfo : StoreActivationInfo
            InMemoryCache : InMemCache
        }

    [<AutoSerializable(false) ; NoEquality ; NoComparison>]
    type StoreInfo = 
        {
            Store : ICloudStore
            Provider : StoreProvider
            Dependencies : Map<AssemblyId, Assembly>
            ActivationInfo : StoreActivationInfo
            Primitives : PrimitiveConfiguration
        }
    with
        member __.Id = __.ActivationInfo.Id

    // TODO : handle all dependent assemblies
    and StoreRegistry private () =

        static let defaultStore = ref None
        static let localCache = ref None
        static let registry = new ConcurrentDictionary<StoreId, StoreInfo> ()

        static let hashAlgorithm = SHA256Managed.Create() :> HashAlgorithm
        static let computeHash (txt : string) = hashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes txt)

        static member internal InitStore(provider : StoreProvider) =
            let store = provider.InitStore()
            let id = { AssemblyQualifiedName = store.GetType().AssemblyQualifiedName ; UUID = computeHash store.UUID }
            let assemblies = VagrantUtils.ComputeAssemblyDependencies provider.StoreFactoryType
            let ids = assemblies |> List.map VagrantUtils.ComputeAssemblyId
            let dependencies = Seq.zip ids assemblies |> Map.ofSeq

            let info = 
                { 
                    Id = id
                    FactoryQualifiedName = provider.StoreFactoryQualifiedName
                    ConnectionString = provider.ConnectionString
                    Dependencies = ids
                }
            info, dependencies, store

        static member internal Register(store : StoreInfo, ?makeDefault) =
            registry.[store.ActivationInfo.Id] <- store
            if defaultArg makeDefault false then
                defaultStore := Some store

        static member TryGetStoreProvider(info : StoreActivationInfo) =
            StoreProvider.TryDefine(info.FactoryQualifiedName, info.ConnectionString, throwOnError = false)

        static member ActivateLocalCache(provider : StoreProvider) =
            lock localCache (fun () ->
                let info, _, store = StoreRegistry.InitStore provider
                let cacheInfo = { ActivationInfo = info ; Store = store ; InMemoryCache = InMemCache() }
                localCache := Some cacheInfo)

        static member LocalCache =
            match localCache.Value with
            | None -> invalidOp "a local cache has not been registered."
            | Some lc -> lc

        static member DefaultStoreInfo = 
            match defaultStore.Value with
            | None -> invalidOp "a default store has not been registered."
            | Some ds -> ds

        static member TryGetStoreInfo id = registry.TryFind id
