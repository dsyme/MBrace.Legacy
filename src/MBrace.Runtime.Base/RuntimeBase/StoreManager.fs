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

        let uploadStore (getPortableAssembly : AssemblyId -> PortableAssembly) (storeInfo : StoreInfo) (remote : ActorRef<StoreManager>) = async {
            let! result = remote <!- fun ch -> ActivateStore(ch, storeInfo.ActivationInfo)

            match result with
            | Success -> return ()
            | MissingAssemblies ids ->
                let pas = ids |> List.map getPortableAssembly
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

        let downloadStore (client : VagrantClient) makeDefault (remote : ActorRef<StoreManager>) = async {
            let! info = remote <!- GetDefaultStore

            let missing = 
                info.Dependencies 
                |> client.GetAssemblyLoadInfo 
                |> List.filter (function Loaded _ | LoadedWithStaticIntialization _ -> false | _ -> true)
                |> List.map (fun l -> l.Id)

            let! pas = remote <!- fun ch -> DownloadAssemblies(ch, missing)

            let results = client.LoadPortableAssemblies pas

            return
                match StoreRegistry.TryActivate(info, makeDefault = makeDefault) with
                | None -> invalidOp <| sprintf "Failed to activate store '%s'" info.Id.AssemblyQualifiedName
                | Some info -> info
        }