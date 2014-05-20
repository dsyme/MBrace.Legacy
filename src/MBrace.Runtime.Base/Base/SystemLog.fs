namespace Nessos.MBrace.Runtime.Logging

    open System
    open System.IO
    open System.Threading

    open Microsoft.FSharp.Control

    open Nessos.Thespian

    open Nessos.MBrace.Utils.Json
    open Nessos.MBrace.Runtime.Store


    /// abstract system logger
    type ISystemLogger =
        abstract LogEntry : SystemLogEntry -> unit

    and SystemLogEntry = 
        {
            Message : string
            Level : LogLevel
            Date : DateTime
        }
    with
        member e.Print(showDate : bool) =
            if showDate then
                let date = e.Date.ToString("yyyy-MM-dd H:mm:ss")
                sprintf "[%s] %O : %s" date e.Level e.Message
            else 
                sprintf "%O : %s" e.Level e.Message

    and LogLevel = 
        | Info
        | Warning
        | Error
    with
        override lvl.ToString() =
            match lvl with
            | Info -> "INFO"
            | Warning -> "WARNING"
            | Error -> "ERROR"
        
        member lvl.Value =
            match lvl with
            | Info -> 0
            | Warning -> 1
            | Error -> 2

        static member Parse i =
            match i with
            | 0 -> Info
            | 1 -> Warning
            | 2 -> Error
            | i -> invalidArg "i" "invalid log level"


    // Thespian Logging support

    type private TLogLevel = Nessos.Thespian.LogLevel

    type ThespianLogger(logger : ISystemLogger) =

        interface Nessos.Thespian.ILogger with
            member this.Log(msg:string, lvl:TLogLevel, time:DateTime) =
                let lvl' = 
                    match lvl with
                    | TLogLevel.Info     -> Info
                    | TLogLevel.Error    -> Error
                    | TLogLevel.Warning  -> Warning

                logger.LogEntry { Message = msg ; Level = lvl' ; Date = time }

        static member Register(logger : ISystemLogger) =
            let tl = new ThespianLogger(logger)
            Logger.Register(tl)

    // Logger primitives

    /// Do nothing logger
    type NullLogger () =
        interface ISystemLogger with
            member __.LogEntry _ = ()

    /// Writes logs to stdout
    type ConsoleLogger (?showDate) =
        let showDate = defaultArg showDate false

        interface ISystemLogger with
            member __.LogEntry e = Console.WriteLine (e.Print(showDate))

    /// Writes logs to local file
    type FileLogger (path : string, ?showDate, ?append) =

        let showDate = defaultArg showDate true
        let fileMode = if defaultArg append true then FileMode.OpenOrCreate else FileMode.Create
        let fs = new FileStream(path, fileMode, FileAccess.Write, FileShare.Read)
        let writer = new StreamWriter(fs)

        interface ISystemLogger with
            member __.LogEntry e = writer.WriteLine (e.Print(showDate))
             
        interface IDisposable with
            member __.Dispose () = writer.Flush () ; writer.Close () ; fs.Close()

    /// A logger that serializes to JSON ; NOT thread safe
    type JsonLogger (stream : Stream) =
        let writer = JsonSequence.CreateSerializer<SystemLogEntry>(stream, newLine = true)

        interface ISystemLogger with
            member __.LogEntry e = writer.WriteNext e

        interface IDisposable with  
            member __.Dispose () = (writer :> IDisposable).Dispose()

    /// A logger that serializes JSON to file ; NOT thread safe
    type JsonFileLogger (path : string, ?append) =
        inherit JsonLogger(
            let fileMode = if defaultArg append true then FileMode.OpenOrCreate else FileMode.Create in
            let fs = new FileStream(path, fileMode, FileAccess.Write, FileShare.Read) in
            fs
        )

        static member ReadLogs(path : string) =
            use fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
            let d = JsonSequence.CreateDeserializer<SystemLogEntry>(fs)
            Seq.toArray d

    /// A logger that serializes to store

    type StoreSystemLogger(store : ICloudStore, container, logPrefix) =
        inherit StoreLogger<SystemLogEntry>(store, container, logPrefix)

        static member GetReader(store : ICloudStore, container) =
            new StoreLogReader<SystemLogEntry>(store, container)

        interface ISystemLogger with
            member __.LogEntry (e : SystemLogEntry) = base.LogEntry e


    /// Asynchronous log wrapper
        
    type AsyncLogger (underlying : ISystemLogger) =

        let cts = new CancellationTokenSource ()

        let rec behaviour (inbox : MailboxProcessor<SystemLogEntry>) =
            async {
                let! entry = inbox.Receive ()

                try underlying.LogEntry entry
                with _ -> ()
                
                return! behaviour inbox
            }
            
        let actor = MailboxProcessor.Start (behaviour, cts.Token)

        interface ISystemLogger with
            member __.LogEntry e = actor.Post e

        interface IDisposable with
            member __.Dispose () = cts.Cancel ()       

    
    [<RequireQualifiedAccess>]
    module Logger =

        let inline private mkEntry level message =
            { Date = DateTime.Now ; Message = message ; Level = level }

        let inline logEntry entry (l : ISystemLogger) = l.LogEntry entry
        let inline log txt lvl (l : ISystemLogger) = l.LogEntry <| mkEntry lvl txt
        let inline logWithException (exn : exn) txt lvl (l : ISystemLogger) = 
            let message = sprintf "%s:\nException=%O" txt  exn
            l.LogEntry <| mkEntry lvl message

        let inline logF (l : ISystemLogger) lvl fmt = Printf.ksprintf (fun msg -> l.LogEntry <| mkEntry lvl msg) fmt
        let inline logInfo txt (l : ISystemLogger) = l.LogEntry <| mkEntry Info txt
        let inline logError txt (l : ISystemLogger) = l.LogEntry <| mkEntry Error txt
        let inline logWarning txt (l : ISystemLogger) = l.LogEntry <| mkEntry Warning txt
        
        let inline logSafe e (l : ISystemLogger) = try l.LogEntry e with _ -> ()

        let createNullLogger () = (new NullLogger ()) :> ISystemLogger
        let createConsoleLogger () = (new ConsoleLogger ()) :> ISystemLogger
        let createFileLogger path = (new FileLogger (path)) :> ISystemLogger

        // combinators

        /// <summary>Constructs an abstract Logger</summary>
        /// <param name="append">the append log entry behavior.</param>
        let inline create (append : SystemLogEntry -> unit) =
            {
                new ISystemLogger with
                    member m.LogEntry e = append e
            }

        let wrapAsync (l : ISystemLogger) = (new AsyncLogger(l)) :> ISystemLogger

        let broadcast (loggers : ISystemLogger list) =
            create (fun e -> for l in loggers do logSafe e l)

        let map convertF (logger : ISystemLogger) =
            create (fun e -> logEntry (convertF e) logger)

        let filter (filterP : SystemLogEntry -> bool) (logger : ISystemLogger) =
            create (fun e -> if filterP e then logEntry e logger)

        let maxLogLevel (lvl : LogLevel) (logger : ISystemLogger) = 
            filter (fun e -> e.Level <= lvl) logger


    [<AutoOpen>]
    module LogUtils =

        type ISystemLogger with
            member l.Log (text : string) (lvl : LogLevel) = Logger.log text lvl l
            member l.LogWithException exn text lvl = Logger.logWithException exn text lvl l
            member l.Logf lvl fmt = Logger.logF l lvl fmt
            member l.LogInfo text = Logger.logInfo text l
            member l.LogError text = Logger.logError text l
            member l.LogWarning text = Logger.logWarning text l