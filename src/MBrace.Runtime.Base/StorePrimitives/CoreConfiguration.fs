namespace Nessos.MBrace.Runtime.Store

    
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module CoreConfiguration =
    
        open Nessos.MBrace.Core
        open Nessos.MBrace.Utils
        open Nessos.MBrace.Runtime
        open Nessos.FsPickler

        let Activate (logger : ILogger, storeInfo : StoreInfo, cacheStoreEndpoint) : CoreConfiguration =
            match StoreRegistry.TryGetCoreConfiguration storeInfo.Id with
            | Some config -> config
            | None ->
                let store = storeInfo.Store
                // fsStore used but caches
                // inMemCache used by cref store
                // localCache used by cseq/cfile store
                let fsStore = new FileSystemStore(cacheStoreEndpoint)
                let inMemCache = new Cache(fsStore, Serializer.Pickler)
                let localCache = new LocalCacheStore(fsStore, store)

                let crefStore  = new CloudRefProvider(storeInfo, inMemCache)  :> ICloudRefProvider
                let cseqStore  = new CloudSeqProvider(store, localCache)  :> ICloudSeqProvider
                let mrefStore  = new MutableCloudRefProvider(storeInfo)       :> IMutableCloudRefProvider
                let cfileStore = new CloudFileProvider(store, localCache) :> ICloudFileProvider
                let clogsStore = new StoreLogger(store, batchCount = 50, batchTimespan = 500) 

                let cloner = 
                    {
                        new IObjectCloner with
                            member __.Clone(t : 'T) =
                                use m = new System.IO.MemoryStream()
                                Serializer.Pickler.Serialize(m, t)
                                m.Position <- 0L
                                Serializer.Pickler.Deserialize<'T>(m)
                    }

                let coreConfig =
                    {
                        CloudRefProvider        = crefStore
                        CloudSeqProvider        = cseqStore
                        CloudFileProvider       = cfileStore
                        MutableCloudRefProvider = mrefStore
                        CloudLogger             = clogsStore
                        Cloner                  = cloner
                    }

                StoreRegistry.RegisterCoreConfiguration(storeInfo.Id, coreConfig)

                coreConfig
        