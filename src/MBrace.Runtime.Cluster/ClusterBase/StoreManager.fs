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
        | ActivateStore(RR ctx reply, info) ->
            try
                let response =
                    match StoreRegistry.TryActivate(info, makeDefault = true) with
                    | Some _ -> StoreLoadResponse.Success
                    | None ->
                        info.Dependencies 
                        |> loader.Value.GetAssemblyLoadInfo
                        |> List.choose (function Loaded _ -> None | info -> Some info.Id)
                        |> MissingAssemblies

                reply <| Value response

            with e ->
                ctx.LogError e
                reply <| Exception e

        | GetDefaultStore (RR ctx reply) ->
            try
                let info = StoreRegistry.DefaultStoreInfo
                reply <| Value info.ActivationInfo
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