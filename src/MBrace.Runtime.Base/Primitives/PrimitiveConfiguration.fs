namespace Nessos.MBrace.Runtime

    type PrimitiveConfiguration =
        {
            CloudRefProvider : CloudRefProvider
            CloudFileProvider : CloudFileProvider
            CloudSeqProvider : CloudSeqProvider
            MutableCloudRefProvider : MutableCloudRefProvider
        }

    with
        static member Init(?storeId : StoreId) =
            let info = 
                match storeId with
                | None -> StoreRegistry.DefaultStoreInfo
                | Some id -> StoreRegistry.GetStoreInfo id

            let cRefProvider = CloudRefProvider.Create(info.Id, info.Store, info.InMemoryCache, info.CacheStore)
            let cFileProvider = CloudFileProvider.Create(info.Id, info.Store, info.CacheStore)
            let mRefProvider = MutableCloudRefProvider.Create(info.Id, info.Store)
            let cSeqprovider = CloudSeqProvider.Create(info.Id, info.Store, info.CacheStore)

            {
                CloudRefProvider = cRefProvider
                CloudFileProvider = cFileProvider
                CloudSeqProvider = cSeqprovider
                MutableCloudRefProvider = mRefProvider
            }