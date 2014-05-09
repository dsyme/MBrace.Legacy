namespace Nessos.MBrace.Runtime

    open System

    open Nessos.MBrace
    open Nessos.MBrace.Utils

    open Nessos.Thespian

    type MLogLevel = Nessos.MBrace.Utils.LogLevel
    type TLogLevel = Nessos.Thespian.LogLevel

    type ThespianLogger(logger : Nessos.MBrace.Utils.ILogger) =

        interface Nessos.Thespian.ILogger with
            member this.Log(msg:string, lvl:TLogLevel, time:DateTime) =
                let lvl' = 
                    match lvl with
                    | Info      -> MLogLevel.Info
                    | Error     -> MLogLevel.Error
                    | Warning   -> MLogLevel.Warning
                logger.LogEntry(SystemLog(msg, lvl', time))

        static member Register(logger : Utils.ILogger) =
            let tl = new ThespianLogger(logger)
            Logger.Register(tl)

       