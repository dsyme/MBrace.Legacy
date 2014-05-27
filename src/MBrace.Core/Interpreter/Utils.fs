namespace Nessos.MBrace.Core

    open System
    open System.Reflection
    open System.Collections.Concurrent
    open System.Text.RegularExpressions

    module internal Utils =

        let memoize (f : 'T -> 'U) : 'T -> 'U =
            let cache = new ConcurrentDictionary<'T,'U>()
            fun x -> cache.GetOrAdd(x, f)

        /// memoized regex active pattern
        let (|RegexMatch|_|) =
            let regex = memoize(fun pattern -> Regex(pattern))
            
            fun (pat : string) (inp : string) ->
                let m = (regex pat).Match inp in
                if m.Success 
                then Some (List.tail [ for g in m.Groups -> g.Value ])
                else None

        let private remoteStackTraceField =
            let getField name = typeof<System.Exception>.GetField(name, BindingFlags.Instance ||| BindingFlags.NonPublic)
            match getField "remote_stack_trace" with
            | null ->
                match getField "_remoteStackTraceString" with
                | null -> failwith "a piece of unreliable code has just failed."
                | f -> f
            | f -> f

        let inline reraise' (e : #exn) =
            remoteStackTraceField.SetValue(e, e.StackTrace + System.Environment.NewLine)
            raise e

        type MethodInfo with
            member m.GuardedInvoke(instance : obj, parameters : obj []) =
                try m.Invoke(instance, parameters)
                with :? TargetInvocationException as e when e.InnerException <> null ->
                    reraise' e.InnerException

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