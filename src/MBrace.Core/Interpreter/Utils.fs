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

        type Async with
            static member Raise(e : exn) = Async.FromContinuations(fun (_,ec,_) -> ec e)
            static member Choice<'T>(tasks : Async<'T option> seq) : Async<'T option> =
                let wrap task =
                    async {
                        let! res = task
                        match res with
                        | None -> return ()
                        | Some r -> return! Async.Raise <| SuccessException r
                    }

                async {
                    try
                        do!
                            tasks
                            |> Seq.map wrap
                            |> Async.Parallel
                            |> Async.Ignore

                        return None
                    with 
                    | :? SuccessException<'T> as ex -> return Some ex.Value
                }

        and private SuccessException<'T>(value : 'T) =
            inherit System.Exception()
            member __.Value = value