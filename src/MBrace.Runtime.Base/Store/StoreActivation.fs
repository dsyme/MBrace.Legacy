namespace Nessos.MBrace.Runtime.Store

    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Client

    [<AutoOpen>]
    module StoreActivation =
    
        type StoreRegistry with
            static member Activate(provider : StoreProvider, ?makeDefault) =
                let id, store = StoreRegistry.InitStore(provider)
                match StoreRegistry.TryGetStoreInfo id with
                | Some info -> info
                | None ->

                    // TODO : move cache stuff to StoreRegistry
                    let cacheInfo = StoreRegistry.LocalCache
                    let inMemCache = new InMemCache()
                    let localCache = new LocalCache(sprintf "fscache-%d" <| hash id, cacheInfo.Store, store)

                    let crefStore  = new CloudRefProvider(id, store, inMemCache, localCache)  :> ICloudRefProvider
                    let cseqStore  = new CloudSeqProvider(id, store, localCache)              :> ICloudSeqProvider
                    let mrefStore  = new MutableCloudRefProvider(id, store)                   :> IMutableCloudRefProvider
                    let cfileStore = new CloudFileProvider(id, store, localCache)             :> ICloudFileProvider

                    let primitives =
                        {
                            CloudRefProvider        = crefStore
                            CloudSeqProvider        = cseqStore
                            CloudFileProvider       = cfileStore
                            MutableCloudRefProvider = mrefStore
                        }

                    let info = new StoreInfo(id, provider, store, primitives)

                    StoreRegistry.Register(info, ?makeDefault = makeDefault) |> ignore

                    info