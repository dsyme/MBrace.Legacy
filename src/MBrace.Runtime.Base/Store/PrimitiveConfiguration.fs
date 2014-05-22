namespace Nessos.MBrace.Runtime.Store

    
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module PrimitiveConfiguration =
    
        open Nessos.MBrace.Core
        open Nessos.MBrace.Utils
        open Nessos.MBrace.Runtime

        let activate (storeInfo : StoreInfo, cacheStoreEndpoint) : PrimitiveConfiguration =
            match StoreRegistry.TryGetCoreConfiguration storeInfo.Id with
            | Some config -> config
            | None ->
                let store = storeInfo.Store
                // fsStore used but caches
                // inMemCache used by cref store
                // localCache used by cseq/cfile store
                let fsStore = new FileSystemStore(cacheStoreEndpoint)
                let inMemCache = new LocalObjectCache(fsStore)
                let localCache = new LocalCacheStore("localCacheStore", fsStore, store)

                let crefStore  = new CloudRefProvider(storeInfo, inMemCache)  :> ICloudRefProvider
                let cseqStore  = new CloudSeqProvider(storeInfo, localCache)  :> ICloudSeqProvider
                let mrefStore  = new MutableCloudRefProvider(storeInfo)       :> IMutableCloudRefProvider
                let cfileStore = new CloudFileProvider(storeInfo, localCache) :> ICloudFileProvider

                let coreConfig : PrimitiveConfiguration =
                    {
                        CloudRefProvider        = crefStore
                        CloudSeqProvider        = cseqStore
                        CloudFileProvider       = cfileStore
                        MutableCloudRefProvider = mrefStore
                    }

                StoreRegistry.RegisterCoreConfiguration(storeInfo.Id, coreConfig)

                coreConfig
        