module internal Nessos.MBrace.Runtime.Definitions.StoreManager

    open Nessos.Vagrant
    open Nessos.Thespian
    open Nessos.Thespian.Cluster

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Store

    // Vagrant's cache & assembly loader are thread safe, but this should probably be moved inside the actor state.
    let private cache = lazy IoC.Resolve<VagrantCache> ()
    let private loader = lazy IoC.Resolve<VagrantClient> ()

    let storeManagerBehavior (ctx: BehaviorContext<_>) (msg: StoreManager) = async {
        
        match msg with
        | ActivateStore(RR ctx reply, info) ->
            try
                if StoreRegistry.DefaultStoreInfo.Id = info.Id then
                    reply <| Value StoreLoadResponse.Success
                else
                    let missingDependencies =
                        loader.Value.GetAssemblyLoadInfo(info.Dependencies, requireIdentical = false, loadPolicy = AssemblyLocalResolutionPolicy.All)
                        |> List.choose (function Loaded _ -> None | info -> Some info.Id)

                    if missingDependencies.Length > 0 then
                        reply <| Value (MissingAssemblies missingDependencies)
                    else
                        match StoreRegistry.TryActivate(info, makeDefault = true) with
                        | Some _ -> 
                            ctx.LogInfo <| sprintf "switching to store '%s'." info.Id.AssemblyQualifiedName
                            reply <| Value StoreLoadResponse.Success

                        | None -> failwith "could not activate store '%s'." info.Id.AssemblyQualifiedName
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
                let loadInfo = loader.Value.LoadPortableAssemblies(pas, loadPolicy = AssemblyLocalResolutionPolicy.All, requireIdentical = false)
                // save in cache
                let _ = cache.Value.Cache(pas, loadPolicy = AssemblyLocalResolutionPolicy.All, requireIdentical = false)
                reply <| Value loadInfo

            with e ->
                ctx.LogError e
                reply <| Exception e

        | DownloadAssemblies (RR ctx reply, ids) ->
            try
                let pas = ids |> List.map (fun id -> cache.Value.GetCachedAssembly(id, includeImage = true, requireIdentical = false, loadPolicy = AssemblyLocalResolutionPolicy.All))
                reply <| Value pas

            with e ->
                ctx.LogError e
                reply <| Exception e
    }