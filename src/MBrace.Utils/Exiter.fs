namespace Nessos.MBrace.Utils

    open System

    type ParserIExiter = UnionArgParser.IExiter

    type IExiter =
        abstract Exit : ?message : string * ?id : int -> 'T
        abstract ExitEvent : IEvent<int * string option>

    type ExceptionExiter (exnBuilder : string option -> exn) =
        let event = new Event<int * string option> ()
        
        interface IExiter with
            member __.ExitEvent = event.Publish
            member __.Exit(?message, ?id) =
                try do event.Trigger ((defaultArg id 0), message) with _ -> ()
                raise <| exnBuilder message

    type ConsoleProcessExiter (?subscribeUnhandledExceptions) as self =
        let subscribeUnhandledExceptions = defaultArg subscribeUnhandledExceptions false
        let exitEvent = new Event<int * string option> ()

        do if subscribeUnhandledExceptions then
            AppDomain.CurrentDomain.UnhandledException.Add(fun ueea -> 
                let e = ueea.ExceptionObject :?> Exception
                (self :> IExiter).Exit(sprintf "Unhandled exception, please report to the {m}brace team:\n %A" e, -1))

        interface IExiter with
            member __.ExitEvent = exitEvent.Publish
            member __.Exit(?message : string, ?id) =
                let id = defaultArg id 0
                match message with
                | None -> ()
                | Some msg ->
                    if id = 0 then Console.Out.WriteLine msg
                    else Console.Error.WriteLine msg
                
                try exitEvent.Trigger (id, message) with _ -> ()

                Console.Out.Flush()
                Console.Error.Flush()

                if isConsoleWindow then
                    Console.Out.Write("Press any key to exit.")
                    let _ = Console.ReadKey () in ()

                exit id


    [<AutoOpen>]
    module Exiter =
        
        let plugExiter (exiter : IExiter) : ParserIExiter =
            {
                new UnionArgParser.IExiter with
                    member __.Exit(msg:string,?errorCode:int) =
                        exiter.Exit(message = msg, ?id = errorCode)
            }