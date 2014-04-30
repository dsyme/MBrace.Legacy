namespace Nessos.MBrace.Runtime

    open Nessos.Thespian

    type Pool<'T> =
        | Add of ActorRef<'T>
        | Remove of ActorRef<'T>
        | GetAll of IReplyChannel<ActorRef<'T>[]>

    module Pool = 
        let poolBehavior (pool: Set<ActorRef<'T>>) (msg: Pool<'T>) =
            async {
                match msg with
                | Add ref -> return pool |> Set.add ref
                | Remove ref -> return pool |> Set.remove ref
                | GetAll(R(reply)) ->
                    pool |> Set.toArray |> Value |> reply
                    return pool
            }

    module Actor =
        let weakBroadcast (pool: Actor<Pool<'T>>): Actor<'T> =
            let rec weakBroadcastBehavior (self: Actor<'T>) =
                async {
                    let! msg = self.Receive()

                    let! swarm = !pool <!- GetAll

                    do! swarm |> Array.map (fun ref -> async { try ref <-- msg with e -> self.LogWarning e })
                                |> Async.Parallel |> Async.Ignore

                    return! weakBroadcastBehavior self
                }

            Actor.bindLinked weakBroadcastBehavior [pool :> ActorBase]
        

        

