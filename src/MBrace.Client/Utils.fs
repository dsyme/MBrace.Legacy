namespace Nessos.MBrace.Client

    open Nessos.Thespian

    open Nessos.MBrace
    open Nessos.MBrace.Runtime.MBraceException
    open Nessos.MBrace.Utils

//    type internal Shell =
//        static member Settings = Nessos.MBrace.Shell.Shared.ShellSettingsRegistry.Value
//        
//        static member Compile () =
//            match Shell.Settings with
//            | Some conf ->
//                if conf.Verbose then do printfn "compiling interactions... "
//                match conf.ShellActor.PostAndReply RequestCompilation with
//                | Choice1Of2 assembly -> assembly
//                | Choice2Of2 e -> mfailwithInner e "compilation of interactions has failed; please report to the {m}brace team."
//            | _ -> mfailwith "Compiling interactions only supported in {m}brace shell."


    module internal Error =

        let handle =
            function
            | MBraceExn e -> raise e : 'T
            | e -> raise <| MBraceException("Unexpected error, please report to the mbrace team.", e)

        let handleAsync (f : Async<'T>) =
            async {
                try
                    return! f
                with
                | MBraceExn exn -> return! Async.Raise exn
                | e -> return! Async.Raise <| MBraceException("Unexpected error, please report to the mbrace team.", e)
            }

        let handleAsync2 (f : Async<'T>) = 
            try Async.RunSynchronously f
            with e -> handle e