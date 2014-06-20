namespace Nessos.MBrace.Runtime

    open Nessos.Thespian
    open Nessos.Vagrant

    open Nessos.MBrace.Runtime.Store
    open Nessos.MBrace.Client

    type StoreManager =
        | GetStoreStatus of IReplyChannel<StoreStatus> * StoreId
        | SetDefaultStorageProvider of IReplyChannel<unit> * StoreProvider
        | GetDefaultStorageProvider of IReplyChannel<StoreProvider>
        | GetDefaultStoreInfo of IReplyChannel<StoreId * AssemblyId list>
        | GetAssemblyLoadInfo of IReplyChannel<AssemblyLoadInfo list> * AssemblyId list
        | UploadAssemblies of IReplyChannel<AssemblyLoadInfo list> * PortableAssembly list
        | DownloadAssemblies of IReplyChannel<PortableAssembly list> * AssemblyId list


    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module StoreManager =

        let uploadStore (server : VagrantServer option) (storeInfo : StoreInfo) (remote : ActorRef<StoreManager>) = async {
            let! state = remote <!- fun ch -> GetStoreStatus(ch, storeInfo.Id)

            match state with
            | Activated -> return ()
            | Installed | Available -> 
                do! remote <!- fun ch -> SetDefaultStorageProvider(ch, storeInfo.Provider)

            | UnAvailable ->
                let map = storeInfo.Dependencies |> Seq.map (fun d -> VagrantUtils.ComputeAssemblyId d, d) |> Map.ofSeq
                let! info = remote <!- fun ch -> GetAssemblyLoadInfo(ch, map |> Map.toList |> List.map fst)
                let portableAssemblies =
                    info
                    |> List.choose (function Loaded _ -> None | info -> Some info.Id)
                    |> List.map (fun id ->
                        let a = map.[id]
                        match server with 
                        | None -> VagrantUtils.CreatePortableAssembly(a)
                        | Some s -> s.MakePortableAssembly(a, includeAssemblyImage = true))

                let! info = remote <!- fun ch -> UploadAssemblies(ch, portableAssemblies)
                do! remote <!- fun ch -> SetDefaultStorageProvider(ch, storeInfo.Provider)
        }

        let downloadStore (isLoaded : AssemblyId -> bool) (loadF : PortableAssembly -> unit) (remote : ActorRef<StoreManager>) = 
            async {
            
                let! id, dependencies = remote <!- GetDefaultStoreInfo

                match StoreRegistry.GetStoreLoadStatus id with
                | UnAvailable ->
                    // download missing dependencies, if any.
                    let missing = dependencies |> List.filter isLoaded
                    let! pas = remote <!- fun ch -> DownloadAssemblies(ch, missing)
                    for pa in pas do loadF pa

                    // get provider and activate
                    let! provider = remote <!- GetDefaultStorageProvider
                    return StoreRegistry.Activate(provider, makeDefault = true)

                | Available ->
                    let! provider = remote <!- GetDefaultStorageProvider    
                    return StoreRegistry.Activate(provider, makeDefault = true)

                | Installed -> return StoreRegistry.SetDefault(id)
                | Activated -> return StoreRegistry.DefaultStoreInfo
            }