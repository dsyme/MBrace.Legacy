namespace Nessos.MBrace.Utils

    open System
    open System.IO
    open System.Threading
    open System.Collections.Generic
    open System.Collections.Concurrent
    open Microsoft.FSharp.Control

    [<AbstractClass>]
    type ILogger () =
        abstract LogEntry : LogEntry -> unit        

        member l.Log txt lvl = l.LogEntry (SystemLog (txt, lvl, DateTime.Now))
        member l.Logf lvl fmt = Printf.ksprintf (fun str -> l.Log str lvl) fmt
        member l.LogWithException (exn : exn) txt lvl =
            let txt' = sprintf' "%s\nException=%s" txt <| exn.ToString ()
            l.LogEntry <| SystemLog (txt', lvl, DateTime.Now)
        member l.LogInfo txt = l.Log txt LogLevel.Info
        member l.LogError exn txt = l.LogWithException exn txt LogLevel.Error

    and TraceInfo = { 
            Line        : int option
            File        : string option
            Function    : string option
            Message     : string
            DateTime    : DateTime
            Environment : IDictionary<string, string>
            ProcessId   : int
            TaskId      : string
            Id          : int64
        }

    and UserLogInfo = {
            Message     : string
            DateTime    : DateTime
            ProcessId   : int
            TaskId      : string
            Id          : int64
        }

    and LogEntry = 
        | SystemLog of string * LogLevel * DateTime
        | UserLog of UserLogInfo
        | Trace of TraceInfo

    with
        member e.Print(?showDate) =
            let showDate = defaultArg showDate false
            match e with
            | SystemLog(txt,lvl,date) ->
                if showDate then
                    sprintf' "[%s] %s : %s" <| date.ToString("yyyy-MM-dd H:mm:ss") <| lvl.ToString() <| txt
                else sprintf' "%s : %s" <| lvl.ToString() <| txt
            | UserLog info ->
                Nessos.MBrace.Utils.String.string {
                    if showDate 
                    then yield sprintf' "[%s] " <| info.DateTime.ToString("yyyy-MM-dd H:mm:ss")
                    yield sprintf' "Pid %A " info.ProcessId
                    yield sprintf' "TaskId %A " info.TaskId
                    yield sprintf' "%s\n" info.Message 
                } |> Nessos.MBrace.Utils.String.String.build 
            | Trace info ->
                Nessos.MBrace.Utils.String.string {
                    if showDate 
                    then yield sprintf' "[%s] " <| info.DateTime.ToString("yyyy-MM-dd H:mm:ss")
                    if info.File.IsSome then yield sprintf' "File %A " info.File.Value
                    if info.Line.IsSome then yield sprintf' "Line %A " info.Line.Value
                    if info.Function.IsSome then yield sprintf' "Function %A" info.Function.Value
                    yield "\n"
                    yield sprintf' "%s\n" info.Message
                    if info.Environment <> null && not <| Seq.isEmpty info.Environment then
                        yield sprintf' "--- Begin {m}brace dump ---\n"
                        for KeyValue (var, str) in info.Environment do yield sprintf' "val %s = %s\n" var str
                        yield sprintf' "--- End {m}brace dump ---\n" 
                } |> Nessos.MBrace.Utils.String.String.build 

    and 
        [<CustomEquality>]
        [<CustomComparison>]
        LogLevel = 
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
            | i -> raise <| new ArgumentException(sprintf' "Invalid log level %d." i)

        member internal x.CompareTo (y : obj) =
            match y with
            | :? LogLevel as y -> x.Value - y.Value
            | _ -> raise <| new ArgumentException("Invalid argument","y")

        override x.Equals y = x.CompareTo y = 0
        override x.GetHashCode () = x.Value
        interface IComparable with member x.CompareTo y = x.CompareTo y
            

    module Logger =

        let log txt lvl (l : ILogger) = l.Log txt lvl
        let logWithException (exn : exn) txt lvl (l : ILogger) = l.LogWithException exn txt lvl
        let logInfo txt (l : ILogger) = l.LogInfo txt
        let logError (exn : exn) txt (l : ILogger) = l.LogError exn txt

        let logEntry entry (l : ILogger) = l.LogEntry entry
        let safelog entry (l : ILogger) = try l.LogEntry entry with _ -> ()

        // Logger primitives

        type NullLogger () =
            inherit ILogger ()
            override m.LogEntry _ = ()

        type ConsoleLogger (?showDate) =
            inherit ILogger ()
            let showDate = defaultArg showDate false

            override m.LogEntry e =
                Console.WriteLine (e.Print(showDate))

        type FileLogger (path : string, ?initMsg : string, ?showDate, ?append) =
            inherit ILogger ()

            let showDate = defaultArg showDate true
            let writer = TextWriter.Synchronized(new StreamWriter(path, defaultArg append true))

            do initMsg |> Option.iter writer.WriteLine

            override __.LogEntry e =
                writer.WriteLine (e.Print(showDate))
                writer.Flush ()
             
            interface IDisposable with
                member __.Dispose () = writer.Close ()

        type InMemoryLogger () =
            inherit ILogger ()
            let mutable container = new ConcurrentQueue<LogEntry> ()

            override __.LogEntry e = container.Enqueue e

            member __.Dump () = container.ToArray()
            member __.Clear () = container <- new ConcurrentQueue<LogEntry> ()
        
        type AsyncLogger (underlying : ILogger) =
            inherit ILogger ()

            let cts = new CancellationTokenSource ()

            let rec behaviour (inbox : MailboxProcessor<LogEntry>) =
                async {
                    let! entry = inbox.Receive ()

                    safelog entry underlying
                
                    return! behaviour inbox
                }
            
            let actor = MailboxProcessor.Start (behaviour, cts.Token)

            override __.LogEntry e = actor.Post e

            interface IDisposable with
                member __.Dispose () = cts.Cancel ()


        let createNullLogger () = (new NullLogger ()) :> ILogger
        let createConsoleLogger () = (new ConsoleLogger ()) :> ILogger
        let createFileLogger path = (new FileLogger (path)) :> ILogger
        let createInMemoryLogger () = (new InMemoryLogger ()) :> ILogger

        // combinators

        /// <summary>Constructs an abstract Logger</summary>
        /// <param name="append">the append log entry behavior.</param>
        let create (append : LogEntry -> unit) =
            {
                new ILogger () with
                    member m.LogEntry e = append e
            }

        let wrapAsync (l : ILogger) = (new AsyncLogger(l)) :> ILogger

        let propagate (loggers : ILogger list) =
            create (fun e -> List.iter (safelog e) loggers)

        let convert convertF (logger : ILogger) =
            create (fun e -> logEntry (convertF e) logger)

        let filter (filterP : LogEntry -> bool) (logger : ILogger) =
            create (fun e -> if filterP e then logEntry e logger)

        let maxLogLevel (lvl : LogLevel) (logger : ILogger) = 
            filter (fun (SystemLog(_,lvl',_)) -> lvl <= lvl') logger
