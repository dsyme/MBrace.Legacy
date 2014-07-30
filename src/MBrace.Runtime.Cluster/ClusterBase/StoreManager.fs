module internal Nessos.MBrace.Runtime.Definitions.StoreManager

    open Nessos.Vagrant
    open Nessos.Thespian
    open Nessos.Thespian.Cluster

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime

    let storeManagerBehavior (ctx: BehaviorContext<_>) (msg: StoreManager) = async {
        
        match msg with
        | ActivateStore(RR ctx reply, info) ->
            try
                match StoreRegistry.TryGetDefaultStoreInfo() with
                | Some defStore when defStore.Id = info.Id -> reply <| Value StoreLoadResponse.Success
                | _ ->
                    let missingDependencies =
                        VagrantRegistry.Instance.GetAssemblyLoadInfo(info.Dependencies, loadPolicy = AssemblyLoadPolicy.ResolveAll)
                        |> List.choose (function Loaded _ -> None | info -> Some info.Id)

                    if missingDependencies.Length > 0 then
                        reply <| Value (MissingAssemblies missingDependencies)
                    else
                        let info = StoreRegistry.Activate(info, makeDefault = true)
                        ctx.LogInfo <| sprintf "activated store '%s'." info.Id.AssemblyQualifiedName
                        reply <| Value StoreLoadResponse.Success
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
                let loadInfo = VagrantRegistry.Instance.LoadPortableAssemblies(pas, loadPolicy = AssemblyLoadPolicy.ResolveAll)

                reply <| Value loadInfo

            with e ->
                ctx.LogError e
                reply <| Exception e

        | DownloadAssemblies (RR ctx reply, ids) ->
            try
                let pas = VagrantRegistry.Instance.CreatePortableAssemblies(ids, includeAssemblyImage = true, loadPolicy = AssemblyLoadPolicy.ResolveAll)
                reply <| Value pas

            with e ->
                ctx.LogError e
                reply <| Exception e
    }