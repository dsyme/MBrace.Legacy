namespace Nessos.MBrace.Runtime.Interpreter

    open System
    open System.Reflection

    open Nessos.MBrace.Utils


    module internal Utils =

        let (|TargetInvocationException|_|) (e : exn) =
            let rec aux depth (e : exn) =
                match e with
                | :? TargetInvocationException -> aux (depth + 1) e.InnerException
                | e when depth = 0 -> None
                | e -> Some e

            aux 0 e

        type private ContainerException<'T>(value : 'T) =
            inherit System.Exception()
            member __.Value = value

        type Async with
            
            /// A general-purpose parallelism combinator that extends the facility of Async.Parallel. 
            /// Takes a collection of tasks that return either accumulative or exceptional results.
            /// Returns either the accumulated results of *all* tasks or a single result from an exceptional.
            /// This is general enough to allow us to mix and match cancellation semantics without 
            /// bothering with exception handling when implementing custom parallelism combinators 
            /// in the context of abstract trampoline evaluators.
            static member ParGeneric(tasks : Async<Choice<'Acc, 'Exc>> []) : Async<Choice<'Acc [], 'Exc>> =
                let wrap (task : Async<Choice<'Acc, 'Exc>>) = async {
                    let! res = task
                    match res with
                    | Choice1Of2 a -> return a
                    | Choice2Of2 e -> return! Async.Raise <| ContainerException e
                }

                async {
                    try
                        let! aggregates = tasks |> Array.map wrap |> Async.Parallel
                        return Choice1Of2 aggregates

                    with :? ContainerException<'Exc> as e -> return Choice2Of2 e.Value
                }

            /// efficient raise
            static member Raise(e : exn) = Async.FromContinuations(fun (_,ec,_) -> ec e)