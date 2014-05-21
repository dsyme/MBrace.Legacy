namespace Nessos.MBrace.Client

    open Nessos.Thespian
    open Nessos.Thespian.ConcurrencyTools

    open Nessos.Vagrant

    open Nessos.MBrace
    open Nessos.MBrace.Runtime.MBraceException
    open Nessos.MBrace.Utils

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