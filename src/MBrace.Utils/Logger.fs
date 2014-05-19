namespace Nessos.MBrace.Utils.Logging

    open System
    open System.IO
    open System.Threading

    open Microsoft.FSharp.Control

    type ILogger =
        abstract LogEntry : LogEntry -> unit

    and LogEntry = 
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

//        member internal x.CompareTo (y : obj) =
//            match y with
//            | :? LogLevel as y -> x.Value - y.Value
//            | _ -> raise <| new ArgumentException("Invalid argument","y")
//
//        override x.Equals y = x.CompareTo y = 0
//        override x.GetHashCode () = x.Value
//        interface IComparable with member x.CompareTo y = x.CompareTo y

//    [<AbstractClass>]
//    type ILogger () =
//        abstract LogEntry : LogEntry -> unit        
//
//        member l.Log txt lvl = l.LogEntry (SystemLog (txt, lvl, DateTime.Now))
//        member l.Logf lvl fmt = Printf.ksprintf (fun str -> l.Log str lvl) fmt
//        member l.LogWithException (exn : exn) txt lvl =
//            let txt' = sprintf' "%s\nException=%s" txt <| exn.ToString ()
//            l.LogEntry <| SystemLog (txt', lvl, DateTime.Now)
//        member l.LogInfo txt = l.Log txt LogLevel.Info
//        member l.LogError exn txt = l.LogWithException exn txt LogLevel.Error

//    and LogEntry = 
//        | SystemLog of string * LogLevel * DateTime
//        | UserLog of string
//        | Trace of TraceInfo
//
//    with
//        member e.Print(?showDate) =
//            let showDate = defaultArg showDate false
//            match e with
//            | SystemLog(txt,lvl,date) ->
//                if showDate then
//                    sprintf' "[%s] %s : %s" <| date.ToString("yyyy-MM-dd H:mm:ss") <| lvl.ToString() <| txt
//                else sprintf' "%s : %s" <| lvl.ToString() <| txt
//            | UserLog info -> info
////                Nessos.MBrace.Utils.String.string {
//////                    if showDate 
//////                    then yield sprintf' "[%s] " <| info.DateTime.ToString("yyyy-MM-dd H:mm:ss")
//////                    yield sprintf' "Pid %A " info.ProcessId
//////                    yield sprintf' "TaskId %A " info.TaskId
//////                    yield sprintf' "%s\n" info.Message 
////                } |> Nessos.MBrace.Utils.String.String.build 
//            | Trace info ->
//                Nessos.MBrace.Utils.String.string {
////                    if showDate 
////                    then yield sprintf' "[%s] " <| info.DateTime.ToString("yyyy-MM-dd H:mm:ss")
////                    if info.File.IsSome then yield sprintf' "File %A " info.File.Value
////                    if info.Line.IsSome then yield sprintf' "Line %A " info.Line.Value
//                    if info.Function.IsSome then yield sprintf' "Function %A" info.Function.Value
//                    yield "\n"
//                    yield sprintf' "%s\n" info.Message
//                    if info.Environment <> null && not <| Seq.isEmpty info.Environment then
//                        yield sprintf' "--- Begin {m}brace dump ---\n"
//                        for KeyValue (var, str) in info.Environment do yield sprintf' "val %s = %s\n" var str
//                        yield sprintf' "--- End {m}brace dump ---\n" 
//                } |> Nessos.MBrace.Utils.String.String.build  

    // Logger primitives

    type NullLogger () =
        interface ILogger with
            member __.LogEntry _ = ()

    type ConsoleLogger (?showDate) =
        let showDate = defaultArg showDate false

        interface ILogger with
            member __.LogEntry e =
                Console.WriteLine (e.Print(showDate))

    type FileLogger (path : string, ?initMsg : string, ?showDate, ?append) =

        let showDate = defaultArg showDate true
        let writer = TextWriter.Synchronized(new StreamWriter(path, defaultArg append true))

        do initMsg |> Option.iter writer.WriteLine

        interface ILogger with
            member __.LogEntry e =
                writer.WriteLine (e.Print(showDate))
                writer.Flush ()
             
        interface IDisposable with
            member __.Dispose () = writer.Close ()
        
    type AsyncLogger (underlying : ILogger) =

        let cts = new CancellationTokenSource ()

        let rec behaviour (inbox : MailboxProcessor<LogEntry>) =
            async {
                let! entry = inbox.Receive ()

                try underlying.LogEntry entry
                with _ -> ()
                
                return! behaviour inbox
            }
            
        let actor = MailboxProcessor.Start (behaviour, cts.Token)

        interface ILogger with
            member __.LogEntry e = actor.Post e

        interface IDisposable with
            member __.Dispose () = cts.Cancel ()       

    
    [<RequireQualifiedAccess>]
    module Logger =

        let inline private mkEntry level message =
            { Date = DateTime.Now ; Message = message ; Level = level }

        let inline logEntry entry (l : ILogger) = l.LogEntry entry
        let inline log txt lvl (l : ILogger) = l.LogEntry <| mkEntry lvl txt
        let inline logWithException (exn : exn) txt lvl (l : ILogger) = 
            let message = sprintf "%s:\nException=%O" txt  exn
            l.LogEntry <| mkEntry lvl message

        let inline logInfo txt (l : ILogger) = l.LogEntry <| mkEntry Info txt
        let inline logError txt (l : ILogger) = l.LogEntry <| mkEntry Error txt
        let inline logWarning txt (l : ILogger) = l.LogEntry <| mkEntry Warning txt
        
        let inline logSafe e (l : ILogger) = try l.LogEntry e with _ -> ()

        let createNullLogger () = (new NullLogger ()) :> ILogger
        let createConsoleLogger () = (new ConsoleLogger ()) :> ILogger
        let createFileLogger path = (new FileLogger (path)) :> ILogger

        // combinators

        /// <summary>Constructs an abstract Logger</summary>
        /// <param name="append">the append log entry behavior.</param>
        let inline create (append : LogEntry -> unit) =
            {
                new ILogger with
                    member m.LogEntry e = append e
            }

        let wrapAsync (l : ILogger) = (new AsyncLogger(l)) :> ILogger

        let propagate (loggers : ILogger list) =
            create (fun e -> for l in loggers do logSafe e l)

        let map convertF (logger : ILogger) =
            create (fun e -> logEntry (convertF e) logger)

        let filter (filterP : LogEntry -> bool) (logger : ILogger) =
            create (fun e -> if filterP e then logEntry e logger)

        let maxLogLevel (lvl : LogLevel) (logger : ILogger) = 
            filter (fun e -> e.Level <= lvl) logger


    [<AutoOpen>]
    module LogUtils =

        type ILogger with
            member l.Log (text : string) (lvl : LogLevel) = Logger.log text lvl l
            member l.LogWithException exn text lvl = Logger.logWithException exn text lvl
            member l.LogInfo text = Logger.logInfo text l
            member l.LogError text = Logger.logError text l
            member l.LogWarning text = Logger.logWarning text l
