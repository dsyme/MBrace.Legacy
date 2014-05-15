namespace Nessos.MBrace.Runtime.Tests

    open NUnit.Framework

    [<AutoOpenAttribute>]
    module Helpers =

        let wait (n : int) = System.Threading.Thread.Sleep n

        let shouldFailwith<'Exception when 'Exception :> exn>(f : unit -> unit) =
            let result =
                try f () ; Choice1Of3 ()
                with
                | :? 'Exception -> Choice2Of3 ()
                | e -> Choice3Of3 e

            match result with
            | Choice1Of3 () ->
                let msg = sprintf "Expected exception '%s', but was successful." typeof<'Exception>.Name
                raise <| new AssertionException(msg)
            | Choice2Of3 () -> ()
            | Choice3Of3 e ->
                let msg = sprintf "An unexpected exception type was thrown\nExpected: '%s'\n but was: '%s'." (e.GetType().Name) typeof<'Exception>.Name
                raise <| new AssertionException(msg)

        let shouldMatch (f : 'a -> bool) (x : 'a) = //Assert.That(f x)
            if f x then () else raise <| new AssertionException(sprintf "Got Result: %A." x)