module Nessos.MBrace.Runtime.Definitions.Logger

open Nessos.Thespian
open Nessos.Thespian.Cluster
open Nessos.Thespian.Cluster.BehaviorExtensions

open Nessos.MBrace.Utils
open Nessos.MBrace.Caching

let rec loggerActorBehaviour (logger : ILogger) (ctx: BehaviorContext<_>) (entry : LoggerMsg) =
    async {
        Logger.safelog entry logger

        return ()
    }

let wrap (logger : ActorRef<LoggerMsg>) = Logger.create (fun entry -> logger <-- entry)
let lazyWrap (loggerF : unit -> ActorRef<LoggerMsg>) =
    let logger = lazy(loggerF ())
    Logger.create (fun entry -> logger.Value <-- entry)
