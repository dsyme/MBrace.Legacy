namespace Nessos.MBrace.Core

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module CoreConfiguration =
    
    open Nessos.MBrace.Store
    open Nessos.MBrace.Caching
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Utils
    open Nessos.FsPickler

    let Create (logger : ILogger, pickler : FsPickler, store : IStore, cacheStoreEndpoint) : CoreConfiguration =
        // fsStore used but caches
        // inMemCache used by cref store
        // localCache used by cseq/cfile store
        let fsStore = new FileSystemStore(cacheStoreEndpoint)
        let inMemCache = new Cache(fsStore, Serializer.Pickler)
        let localCache = new LocalCacheStore(fsStore, store)

        let crefStore  = new CloudRefStore(store, inMemCache)  :> ICloudRefStore
        let cseqStore  = new CloudSeqStore(store, localCache)  :> ICloudSeqStore
        let mrefStore  = new MutableCloudRefStore(store)       :> IMutableCloudRefStore
        let cfileStore = new CloudFileStore(store, localCache) :> ICloudFileStore
        let clogsStore = new StoreLogger(store, batchCount = 50, batchTimespan = 500) :> ILogStore

        {
            CloudRefStore           = lazy crefStore
            CloudSeqStore           = lazy cseqStore
            CloudFileStore          = lazy cfileStore
            MutableCloudRefStore    = lazy mrefStore
            LogStore                = lazy clogsStore
            Logger                  = lazy logger
            Serializer              = lazy pickler
        }
        