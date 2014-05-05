module internal Nessos.MBrace.Runtime.Definitions.AssemblyManager

open System
open System.Reflection

open Nessos.Vagrant

open Nessos.Thespian
open Nessos.Thespian.Cluster
open Nessos.Thespian.Cluster.ActorExtensions

open Nessos.MBrace.Core
open Nessos.MBrace.Utils
open Nessos.MBrace.Utils.Reflection

// Vagrant's cache & assembly loader are thread safe, but this should probably be moved inside the actor state.
let private cache = lazy IoC.Resolve<VagrantCache>()
let private loader = lazy IoC.Resolve<VagrantClient> ()

let assemblyManagerBehavior (ctx: BehaviorContext<_>) (msg: AssemblyManager) = 

    let loadAssemblies (ids : AssemblyId list) =
        for id in ids do
            let pa = cache.Value.GetCachedAssembly(id, includeImage = true)
            match loader.Value.LoadPortableAssembly pa with
            | Loaded _ | LoadedWithStaticIntialization _ -> ()
            | LoadFault(_,e) -> raise e
            | NotLoaded(id) -> failwithf "Failed to load assembly '%s'." id.FullName

    async {
        match msg with
        | CacheAssemblies(RR ctx reply, assemblies) -> 
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
            try
                let results = cache.Value.Cache assemblies

                results |> Value |> reply

            with e ->
                ctx.LogError e //"AssemblyManager: Failed to cache assemblies."
                reply (Exception e)

        | GetImages(RR ctx reply, assemblies) ->
            try
                let results = assemblies |> List.map (fun (includeImg,id) -> cache.Value.GetCachedAssembly(id, includeImage = includeImg))

                results |> Value |> reply

            with e ->
                ctx.LogError e //"AssemblyManager: Failed to get cached images."
                reply <| Exception e

        | GetAllImages(RR ctx reply) ->
            try
                let assemblies = 
                    cache.Value.CachedAssemblies 
                    |> List.map (fun info -> cache.Value.GetCachedAssembly(info.Id, includeImage = true))

                assemblies |> Value |> reply

            with e ->
                ctx.LogError e
                reply <| Exception e

        | GetInfo (RR ctx reply, ids) ->
            try
                let info = cache.Value.GetCachedAssemblyInfo ids

                info |> Value |> reply

            with e ->
                ctx.LogError e
                reply <| Exception e

        | GetAllInfo(RR ctx reply) ->
            try
                let ids = cache.Value.CachedAssemblies

                ids |> Value |> reply

            with e ->
                ctx.LogError e
                reply <| Exception e

        | LoadAssemblies ids ->
            try loadAssemblies ids
            with e -> ctx.LogError e

        | LoadAssembliesSync(RR ctx reply, ids) ->
            try 
                loadAssemblies ids
                reply nothing

            with e -> 
                ctx.LogError e
                reply <| Exception e
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

//    //Throws ;; nothing
//    let assemblyUploadProtocol (initial : PortableAssembly list) (assemblyManagers : #seq<ActorRef<AssemblyManager>>) =
                
//        // memoize getter for broadcast
//        let tryGetImage = memoize tryGetPacket

//        let printMessageOnce =    
//            fun () -> ctx.LogInfo "Uploading Assemblies to nodes..."
//            |> runOnce

//        //Throws
//        //FailureException => node failure
//        let sendAssemblies assemblies assemblyManager =
//            async {
////                if Array.exists (fun (packet : AssemblyPacket) -> packet.Image.IsSome) assemblies then
////                    printMessageOnce ()
//
//                //return! worker <!- fun ch -> LoadAssemblies(ch, assemblies)
//                //FaultPoint
//                //FailureException => node failure;; do nothing
//                return! ReliableActorRef.FromRef assemblyManager <!- fun ch -> CacheAssemblies(ch, assemblies)
//            }
                
//        let uploaderProtocol (assemblyManager : ActorRef<AssemblyManager>) =
//            async {
//                //Throws
//                //FailureException => node failure;; do nothing
//                let! missingAssemblies = sendAssemblies initial assemblyManager
//
//                if missingAssemblies.Length <> 0 then
//                    let missingImages = missingAssemblies |> Array.choose tryGetImage
//                    // assemblyMemoizer.ReplaceHashesWithImages missingAssemblies
//
//                    // should report back an empty missing assembly array.
//                    // if that is not the case, something has gone horribly wrong
//                    //FailureException => node failure;; do nothing
//                    let! _ = sendAssemblies missingImages assemblyManager 
//
//                    return ()
//            }
//
//        async {
//            for assemblyManager in assemblyManagers do
//                //Throws
//                //FailureException => node failure;; ignore;; allow later mechanisms to handle
//                try
//                    do! uploaderProtocol assemblyManager
//                with (FailureException _ as e) -> ctx.LogWarning e
//        }
//
//    async {
//        match msg with
//        | CacheAssemblies(RR ctx reply, assemblyPackets) ->
//            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
//            try
//                //TODO!
//                //Broadcast in general and report any missing.
//
//                //Throws ;; nothing
//                let! missingAssemblies = localAssemblyManager <!- fun ch -> CacheAssemblies(ch, assemblyPackets)
//
//                if missingAssemblies.Length <> 0 then
//                    //logger.LogInfo "ProcessManager: Assemblies missing. Requesting transmission from client."
//                    reply <| Value missingAssemblies
//
//                else
//                    let! slaveAssemblyManagers = slaveAssemblyManagerProvider
//                    ctx.LogInfo <| sprintf' "Caching assemblies to slave assembly managers: %A" (Seq.toList slaveAssemblyManagers)
//                    //Throws;; nothing
//                    do! assemblyUploadProtocol assemblyPackets slaveAssemblyManagers
//
//                    reply <| Value Array.empty
//            with e ->
//                ctx.LogError e
//                reply (Exception e)
//
//        | _ ->
//            localAssemblyManager <-- msg
//    }

