module internal Nessos.MBrace.Runtime.Definitions.StoreManager

    open Nessos.Vagrant
    open Nessos.Thespian
    open Nessos.Thespian.Cluster

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Store

    // Vagrant's cache & assembly loader are thread safe, but this should probably be moved inside the actor state.
    let private cache = lazy IoC.Resolve<VagrantCache>()
    let private loader = lazy IoC.Resolve<VagrantClient> ()

    let storeManagerBehavior (ctx: BehaviorContext<_>) (msg: StoreManager) = async {
        
        match msg with
        | GetStoreStatus(RR ctx reply, id) ->
            try
                let info = StoreRegistry.GetStoreLoadStatus id
                reply <| Value info

            with e ->
                ctx.LogError e
                reply <| Exception e

        | SetDefaultStorageProvider (RR ctx reply, provider) ->
            try
                let info = StoreRegistry.Activate(provider, makeDefault = true, ?server = None)
                reply nothing
            with e ->
                ctx.LogError e
                reply <| Exception e

        | GetDefaultStorageProvider (RR ctx reply) ->
            try
                let info = StoreRegistry.DefaultStoreInfo
                reply <| Value info.Provider
            with e ->
                ctx.LogError e
                reply <| Exception e

        | GetDefaultStoreInfo (RR ctx reply) ->
            try
                let info = StoreRegistry.DefaultStoreInfo
                reply <| Value (info.Id, info.Dependencies |> List.map VagrantUtils.ComputeAssemblyId)
            with e ->
                ctx.LogError e
                reply <| Exception e

        | StoreManager.GetAssemblyLoadInfo (RR ctx reply, ids) ->
            try
                let loadInfo = loader.Value.GetAssemblyLoadInfo ids
                reply <| Value loadInfo

            with e ->
                ctx.LogError e
                reply <| Exception e

        | UploadAssemblies (RR ctx reply, pas) ->
            try
                // load in current app domain
                let loadInfo = loader.Value.LoadPortableAssemblies pas
                // save in cache
                let _ = cache.Value.Cache pas
                reply <| Value loadInfo

            with e ->
                ctx.LogError e
                reply <| Exception e

        | DownloadAssemblies (RR ctx reply, ids) ->
            try
                let pas = ids |> List.map (fun id -> cache.Value.GetCachedAssembly(id, includeImage = true))
                reply <| Value pas

            with e ->
                ctx.LogError e
                reply <| Exception e
    }