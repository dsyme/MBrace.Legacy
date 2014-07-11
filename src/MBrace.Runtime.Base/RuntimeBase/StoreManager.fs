namespace Nessos.MBrace.Runtime

    open Nessos.Thespian
    open Nessos.Vagrant

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
                let pas = VagrantRegistry.Instance.CreatePortableAssemblies(ids, includeAssemblyImage = true, loadPolicy = AssemblyLoadPolicy.ResolveAll)
                let! info = remote <!- fun ch -> UploadAssemblies(ch, pas)

                match info |> List.tryFind (function Loaded _ -> false | _ -> true) with
                | None -> ()
                | Some info -> invalidOp <| sprintf "Failed to upload store '%O' to remote party." storeInfo.Store

                let! result = remote <!- fun ch -> ActivateStore(ch, storeInfo.ActivationInfo)
                match result with
                | Success -> return ()
                | MissingAssemblies _ ->
                    return invalidOp <| sprintf "Failed to upload store '%O' to remote party." storeInfo.Store

        }

        let downloadStore makeDefault (remote : ActorRef<StoreManager>) = async {
            let! info = remote <!- GetDefaultStore

            let missing = 
                VagrantRegistry.Instance.GetAssemblyLoadInfo(info.Dependencies, loadPolicy = AssemblyLoadPolicy.ResolveAll)
                |> List.filter (function Loaded _ -> false | _ -> true)
                |> List.map (fun l -> l.Id)

            let! pas = remote <!- fun ch -> DownloadAssemblies(ch, missing)

            let results = VagrantRegistry.Instance.LoadPortableAssemblies(pas, loadPolicy = AssemblyLoadPolicy.ResolveAll)

            return
                match StoreRegistry.TryActivate(info, makeDefault = makeDefault) with
                | None -> invalidOp <| sprintf "Failed to activate store '%s'" info.Id.AssemblyQualifiedName
                | Some info -> info
        }