module Nessos.MBrace.Runtime.Definitions.Logger

    open Nessos.Thespian
    open Nessos.Thespian.Cluster
    open Nessos.Thespian.Cluster.BehaviorExtensions

    open Nessos.MBrace.Runtime.Logging

    let rec loggerActorBehaviour (logger : ISystemLogger) (ctx: BehaviorContext<_>) (entry : SystemLogEntry) =
        async {
            Logger.logSafe entry logger
        }

    let wrap (logger : ActorRef<SystemLogEntry>) = Logger.create (fun entry -> logger <-- entry)

    let lazyWrap (logger : Lazy<ActorRef<SystemLogEntry>>) = Logger.create (fun entry -> logger.Value <-- entry)