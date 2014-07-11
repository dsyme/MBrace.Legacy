namespace Nessos.MBrace.Runtime

    type PrimitiveConfiguration =
        {
            CloudRefProvider : CloudRefProvider
            CloudFileProvider : CloudFileProvider
            CloudSeqProvider : CloudSeqProvider
            MutableCloudRefProvider : MutableCloudRefProvider
        }

    with
        static member Init(storeId : StoreId) =
            let info = StoreRegistry.GetStoreInfo storeId

            let cRefProvider = CloudRefProvider.Create(storeId, info.Store, info.InMemoryCache, info.CacheStore)
            let cFileProvider = CloudFileProvider.Create(storeId, info.Store, info.CacheStore)
            let mRefProvider = MutableCloudRefProvider.Create(storeId, info.Store)
            let cSeqprovider = CloudSeqProvider.Create(storeId, info.Store, info.CacheStore)

            {
                CloudRefProvider = cRefProvider
                CloudFileProvider = cFileProvider
                CloudSeqProvider = cSeqprovider
                MutableCloudRefProvider = mRefProvider
            }