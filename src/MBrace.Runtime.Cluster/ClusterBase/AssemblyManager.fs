module internal Nessos.MBrace.Runtime.Definitions.AssemblyManager

open System
open System.Reflection

open Nessos.Vagrant

open Nessos.Thespian
open Nessos.Thespian.Cluster
open Nessos.Thespian.Cluster.ActorExtensions

open Nessos.MBrace.Core
open Nessos.MBrace.Utils
open Nessos.MBrace.Runtime

let assemblyManagerBehavior (ctx: BehaviorContext<_>) (cachedAssemblies : Set<AssemblyId>) (msg: AssemblyManager) = 

    let loadAssemblies (ids : AssemblyId list) =
        let loadAssembly (id : AssemblyId) =
            match VagrantRegistry.Instance.LoadCachedAssembly(id, AssemblyLoadPolicy.ResolveStrongNames) with
            | Loaded(isAppDomainLoaded = true) -> ()
            | _ -> failwithf "Failed to load assembly '%s'." id.FullName

        for id in ids do loadAssembly id

    let cachePolicy = AssemblyLoadPolicy.ResolveStrongNames ||| AssemblyLoadPolicy.CacheOnly

    async {
        match msg with
        | CacheAssemblies(RR ctx reply, assemblies) -> 
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                let results = VagrantRegistry.Instance.LoadPortableAssemblies(assemblies, cachePolicy)

                results |> Value |> reply

                return cachedAssemblies |> Set.addMany (results |> List.choose(function Loaded(id,_,_) -> Some id | _ -> None))

            with e ->
                ctx.LogError e //"AssemblyManager: Failed to cache assemblies."
                reply (Exception e)
                return cachedAssemblies

        | GetImages(RR ctx reply, assemblies) ->
            try
                let results = assemblies |> List.map (fun (includeImg,id) -> VagrantRegistry.Instance.CreatePortableAssembly(id, includeAssemblyImage = includeImg, loadPolicy = cachePolicy))
                results |> Value |> reply
                return cachedAssemblies

            with e ->
                ctx.LogError e //"AssemblyManager: Failed to get cached images."
                reply <| Exception e
                return cachedAssemblies

        | GetAllImages(RR ctx reply) ->
            try
                let assemblies = VagrantRegistry.Instance.CreatePortableAssemblies(cachedAssemblies, includeAssemblyImage = true, loadPolicy = cachePolicy)

                assemblies |> Value |> reply

                return cachedAssemblies

            with e ->
                ctx.LogError e
                reply <| Exception e
                return cachedAssemblies

        | GetInfo (RR ctx reply, ids) ->
            try
                let info = VagrantRegistry.Instance.GetAssemblyLoadInfo(ids, cachePolicy)

                info |> Value |> reply

                return cachedAssemblies

            with e ->
                ctx.LogError e
                reply <| Exception e
                return cachedAssemblies

        | GetAllInfo(RR ctx reply) ->
            try
                let ids = VagrantRegistry.Instance.GetAssemblyLoadInfo(cachedAssemblies, cachePolicy)

                ids |> Value |> reply

                return cachedAssemblies

            with e ->
                ctx.LogError e
                reply <| Exception e
                return cachedAssemblies

        | AssemblyManager.LoadAssemblies ids ->
            try 
                loadAssemblies ids
                return cachedAssemblies

            with e -> 
                ctx.LogError e
                return cachedAssemblies

        | LoadAssembliesSync(RR ctx reply, ids) ->
            try 
                loadAssemblies ids
                reply nothing
                return cachedAssemblies

            with e -> 
                ctx.LogError e
                reply <| Exception e
                return cachedAssemblies
    }


// appears to operate on the assumption that all assembly managers have identical states; is this valid?

/// silly broadcast with reply operator
let (<!-<) (targets : ActorRef<'T> seq) (msgB : IReplyChannel<'R> -> 'T) =
    targets |> Seq.map (fun t -> t <!- msgB) |> Async.Parallel

let masterAssemblyManagerBehavior (localAssemblyManager: ActorRef<AssemblyManager>) 
                                  (slaveAssemblyManagerProvider: Async<seq<ActorRef<AssemblyManager>>>) 
                                  (ctx: BehaviorContext<_>)
                                  (msg: AssemblyManager) =

    async {
        match msg with
        | CacheAssemblies(RR ctx reply, assemblies) ->
            try
                // step 1. cache local
                let! result = localAssemblyManager <!- fun ch -> CacheAssemblies(ch, assemblies)

                // step 2. broadcast to children
                let! slaves = slaveAssemblyManagerProvider
                let! results = Broadcast.postWithReply (fun ch -> CacheAssemblies(ch, assemblies)) slaves |> Broadcast.exec

                reply <| Value result

            with e ->
                ctx.LogError e
                reply <| Exception e

        | _ -> localAssemblyManager <-- msg
    }