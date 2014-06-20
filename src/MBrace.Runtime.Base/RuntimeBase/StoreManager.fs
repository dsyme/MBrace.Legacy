namespace Nessos.MBrace.Runtime

    open Nessos.Thespian
    open Nessos.Vagrant

    open Nessos.MBrace.Runtime.Store

    type StoreLoadResponse =
        | Success
        | MissingAssemblies of AssemblyId list

    type StoreManager =
        | ActivateStore of IReplyChannel<StoreLoadResponse> * StoreActivationInfo
        | GetDefaultStore of IReplyChannel<StoreActivationInfo>

        | UploadAssemblies of IReplyChannel<AssemblyLoadInfo list> * PortableAssembly list
        | DownloadAssemblies of IReplyChannel<PortableAssembly list> * AssemblyId list


    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module StoreManager =

        let uploadStore (storeInfo : StoreInfo) (remote : ActorRef<StoreManager>) = async {
            let! result = remote <!- fun ch -> ActivateStore(ch, storeInfo.ActivationInfo)

            match result with
            | Success -> return ()
            | MissingAssemblies ids ->
                let pas = ids |> List.map (fun id -> storeInfo.Dependencies.[id] |> VagrantUtils.CreatePortableAssembly)
                let! info = remote <!- fun ch -> UploadAssemblies(ch, pas)

                match info |> List.tryFind (function Loaded _ -> true | _ -> false) with
                | None -> ()
                | Some info -> invalidOp <| sprintf "Failed to upload store '%O' to remote party." storeInfo.Store

                let! result = remote <!- fun ch -> ActivateStore(ch, storeInfo.ActivationInfo)
                match result with
                | Success -> return ()
                | MissingAssemblies _ ->
                    return invalidOp <| sprintf "Failed to upload store '%O' to remote party." storeInfo.Store

        }

//        let tryActivateStore (isLoaded : AssemblyId -> bool) (loadF : PortableAssembly -> unit) (activationInfo : StoreActivationInfo) =
//            match StoreRegistry.TryActivate(activationInfo, makeDefault = true)
//            match StoreRegistry.TryGetStoreProvider activationInfo with
//            | None ->
//                let missing = activationInfo.Dependencies |> List.filter (not << isLoaded)
//                MissingAssemblies missing
//
//            | Some p ->
//                let info = StoreRegistry.Activate(p, makeDefault = true)
//                Success
//
//        let downloadStore (isLoaded : AssemblyId -> bool) (loadF : PortableAssembly -> unit) (remote : ActorRef<StoreManager>) = 
//            async {
//            
//                let! activationInfo = remote <!- GetDefaultStore
//
//                match tryActivateStore isLoaded loadF remote with
//                | Success -> return ()
//                | MissingAssemblies ids ->


//                match StoreRegistry.GetStoreLoadStatus activator.Id with
//                | UnAvailable ->
//                    // download missing dependencies, if any.
//                    let missing = activator.Dependencies |> List.filter isLoaded
//                    let! pas = remote <!- fun ch -> DownloadAssemblies(ch, missing)
//                    for pa in pas do loadF pa
//
//                    return StoreRegistry.Activate(activator, makeDefault = true)
//
//                | Available -> return StoreRegistry.Activate(activator, makeDefault = true)
//                | Installed -> return StoreRegistry.SetDefault(activator.Id)
//                | Activated -> return StoreRegistry.DefaultStoreInfo
//            }