module internal Nessos.MBrace.Runtime.Definitions.AssemblyManager

open System
open System.Reflection

open Nessos.Vagrant

open Nessos.Thespian
open Nessos.Thespian.Cluster

open Nessos.MBrace.Core
open Nessos.MBrace.Utils
open Nessos.MBrace.Utils.Reflection

//let private tryGetPacket id =
//    match Assembly.TryFind id.FullName with
//    | None -> AssemblyCache.TryGetPacket id
//    | Some a -> Some <| AssemblyPacket.OfAssembly a

let private cache = lazy IoC.Resolve<AssemblyCache>()
let private vagrant = lazy IoC.Resolve<VagrantClient> ()

let assemblyManagerBehavior (ctx: BehaviorContext<_>) (cached: Set<AssemblyId>) (msg: AssemblyManager) = 
    async {
        return raise <| new NotImplementedException()
//        match msg with
//        | CacheAssemblies(RR ctx reply, assemblies) -> 
            //ASSUME ALL EXCEPTIONS PROPERLY HANDLED AND DOCUMENTED
//            try
//                let cacheRef = ref cached
//
//                // returns 'Some id' iff not cached
//                let cache (pa : PortableAssembly) =
//                    match defaultArg (cacheRef.Value.TryFind pa.Id) with
//                    | None
//                    | Some 
//                    if cacheRef.Value.Contains packet.Header then
//                        // the paranoid will point out that if somebody manually deletes
//                        // assemblies from the directory, the actor cache state will be rendered corrupt
//                        None
//                    elif (Assembly.TryFind packet.Header.FullName).IsSome || AssemblyCache.Contains packet.Header then
//                        cacheRef := cacheRef.Value.Add packet.Header
//                        None
//                    else
//                        match packet.Image with
//                        | None -> Some packet.Header
//                        | Some image -> 
//                            AssemblyCache.Save image |> ignore
//                            cacheRef := cacheRef.Value.Add packet.Header 
//                            None
//
//                assemblyPackets |> Array.choose cachePacket |> Value |> reply
//
//                return cacheRef.Value
//            with e ->
//                ctx.LogError e //"AssemblyManager: Failed to cache assemblies."
//                reply (Exception e)
//
//                return cached
//
//        | GetImages(RR ctx reply, from) ->
//            try
//                let source = 
//                    match from with
//                    | None -> cached :> _ seq
//                    | Some hashes -> hashes :> _ seq
//
//                source |> Seq.map (tryGetPacket >> Option.get)
//                        |> Seq.toArray
//                        |> Value
//                        |> reply
//            with e ->
//                ctx.LogError e //"AssemblyManager: Failed to get cached images."
//                reply <| Exception e
//
//            return cached
//
//        | GetAllHashes(RR ctx reply) ->
//            cached |> Set.toArray |> Value |> reply
//
//            return cached
//
//        | AssemblyManager.Clear ->
//            return Set.empty
//            
//        | LoadAssemblies assemblies ->
//            try
//                for assembly in assemblies do
//                    AssemblyId.TryLoad assembly |> ignore
//            with e -> ctx.LogError e
//
//            return cached
//
//        | LoadAssembliesSync(RR ctx reply, assemblies) ->
//            try
//                for assembly in assemblies do
//                    AssemblyId.TryLoad assembly |> ignore
//
//                reply nothing
//            with e -> ctx.LogError e
//
//            return cached
    }


let masterAssemblyManagerBehavior (localAssemblyManager: ActorRef<AssemblyManager>) 
                                  (slaveAssemblyManagerProvider: Async<seq<ActorRef<AssemblyManager>>>) 
                                  (ctx: BehaviorContext<_>)
                                  (msg: AssemblyManager) =

    raise <| NotImplementedException() : Async<unit>

//    //Throws ;; nothing
//    let assemblyUploadProtocol (initial : AssemblyPacket []) (assemblyManagers : #seq<ActorRef<AssemblyManager>>) =
//                
//        // memoize getter for broadcast
//        let tryGetImage = memoize tryGetPacket
//
//        let printMessageOnce =    
//            fun () -> ctx.LogInfo "Uploading Assemblies to nodes..."
//            |> runOnce
//
//        //Throws
//        //FailureException => node failure
//        let sendAssemblies assemblies assemblyManager =
//            async {
//                if Array.exists (fun (packet : AssemblyPacket) -> packet.Image.IsSome) assemblies then
//                    printMessageOnce ()
//
//                //return! worker <!- fun ch -> LoadAssemblies(ch, assemblies)
//                //FaultPoint
//                //FailureException => node failure;; do nothing
//                return! ReliableActorRef.FromRef assemblyManager <!- fun ch -> CacheAssemblies(ch, assemblies)
//            }
//                
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

