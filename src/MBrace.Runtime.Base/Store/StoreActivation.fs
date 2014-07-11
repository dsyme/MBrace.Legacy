namespace Nessos.MBrace.Runtime.Store

//    open Nessos.MBrace.Core
//    open Nessos.MBrace.Utils
//    open Nessos.MBrace.Runtime
//
//    open Nessos.Vagrant
//
//    [<AutoOpen>]
//    module StoreActivation =
//    
//        type StoreRegistry with
//            static member Activate(provider : StoreProvider, ?makeDefault) =
//                
//                match provider.Id |> Option.bind StoreRegistry.TryGetStoreInfo with
//                | Some info -> info
//                | None ->
//                    let activationInfo, dependencies, store = StoreRegistry.InitStore(provider)
//
//                    // TODO : move cache stuff to StoreRegistry
//                    let cacheInfo = StoreRegistry.LocalCache
//                    let localCache = new LocalCache(sprintf "fscache-%d" <| hash activationInfo.Id, cacheInfo.Store, store)
//
//                    let crefStore  = new CloudRefProvider(activationInfo.Id, store, cacheInfo.InMemoryCache, localCache) 
//                    let cseqStore  = new CloudSeqProvider(activationInfo.Id, store, localCache)                          
//                    let mrefStore  = new MutableCloudRefProvider(activationInfo.Id, store)                               
//                    let cfileStore = new CloudFileProvider(activationInfo.Id, store, localCache)
//
//                    let primitives =
//                        {
//                            CloudRefProvider        = crefStore   :> ICloudRefProvider
//                            CloudSeqProvider        = cseqStore   :> ICloudSeqProvider
//                            CloudFileProvider       = cfileStore  :> ICloudFileProvider
//                            MutableCloudRefProvider = mrefStore   :> IMutableCloudRefProvider
//                        }
//
//                    let info =
//                        {
//                            ActivationInfo = activationInfo
//                            Store = store
//                            Provider = provider
//                            Dependencies = dependencies
//                            Primitives = primitives
//                        }
//
//                    StoreRegistry.Register(info, ?makeDefault = makeDefault) |> ignore
//
//                    info
//
//            static member TryActivate(activationInfo : StoreActivationInfo, ?makeDefault) =
//                match StoreRegistry.TryGetStoreInfo activationInfo.Id with
//                | Some info as r -> StoreRegistry.Register(info, ?makeDefault = makeDefault) ; r
//                | None ->
//                    match StoreRegistry.TryGetStoreProvider activationInfo with
//                    | Some p -> StoreRegistry.Activate(p, ?makeDefault = makeDefault) |> Some
//                    | None -> None