namespace Nessos.MBrace.Utils

    open System
    open System.Diagnostics
    open System.IO
    open System.Net
    open System.Threading
    open System.Threading.Tasks

    open Microsoft.FSharp.Control
    open Microsoft.FSharp.Control.WebExtensions


    [<AutoOpen>]
    module AsyncExtensions =

        type private SuccessException<'T>(value : 'T) =
            inherit Exception()
            member self.Value = value

        type Microsoft.FSharp.Control.Async with
            //correct sleep implementation
            static member SleepSafe (timeout: int) = async {
                let! ct = Async.CancellationToken
                let tmr = ref (null : System.Threading.Timer)
                let cancellationCont = ref (ignore : System.OperationCanceledException -> unit)
                use! cancelHandler = Async.OnCancel(fun () -> (if tmr.Value <> null then tmr.Value.Dispose()); cancellationCont.Value (new System.OperationCanceledException()))
                do! Async.FromContinuations(fun (success, error, cancel) ->
                    cancellationCont := cancel
                    tmr := 
                        new System.Threading.Timer(
                            new System.Threading.TimerCallback(fun _ -> if not ct.IsCancellationRequested then success()), 
                            null, 
                            timeout, 
                            System.Threading.Timeout.Infinite
                        )
                )
            }

            /// untyped awaitTask
            static member AwaitTask (t : Task) = t.ContinueWith ignore |> Async.AwaitTask
            /// non-blocking awaitTask with timeout
            static member AwaitTask (t : Task<'T>, timeout : int) =
                async {
                    use cts = new CancellationTokenSource()
                    use timer = Task.Delay (timeout, cts.Token)
                    try
                        let! completed = Async.AwaitTask <| Task.WhenAny(t, timer)
                        if completed = (t :> Task) then
                            let! result = Async.AwaitTask t
                            return Some result
                        else return None

                    finally cts.Cancel()
                }
            /// eficient raise
            static member Raise (e : #exn) : Async<'T> = Async.FromContinuations(fun (_,econt,_) -> econt e)
            /// a more functional RunSynchronously wrapper
            static member Run timeout (comp : Async<'T>) =
                match timeout with
                | None -> Async.RunSynchronously comp
                | Some t -> Async.RunSynchronously(comp, t)
            /// nondeterministic choice
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

            static member IsolateCancellation (computationF : CancellationToken -> Async<'T>, ?cancellationToken : CancellationToken) : Async<'T> =
                async {
                    let! ct = 
                        match cancellationToken with
                        | None -> Async.CancellationToken
                        | Some ct -> async.Return ct

                    try
                        return! Async.AwaitTask <| Async.StartAsTask(computationF ct)
                    with :? AggregateException as e when e.InnerExceptions.Count = 1 ->
                        return! Async.Raise <| e.InnerExceptions.[0]
                }


        type AsyncResultCell<'T>() =
            let completionSource = new TaskCompletionSource<'T>()

            member c.RegisterResult(result: 'T) = completionSource.SetResult(result)
            member c.AsyncWaitResult(millisecondsTimeout: int): Async<'T option> =
                Async.AwaitTask(completionSource.Task, millisecondsTimeout)

            // use default AwaitTask when no timeout overload is given
            member c.AsyncWaitResult(): Async<'T> =
                Async.AwaitTask(completionSource.Task)


        type Microsoft.FSharp.Control.Async with 
            static member AwaitObservable(observable: IObservable<'T>, ?timeout) =
                let resultCell = new AsyncResultCell<'T>()
                let rec observer = (fun result ->
                    resultCell.RegisterResult(result)
                    remover.Dispose())
                and remover: IDisposable = observable.Subscribe resultCell.RegisterResult

                match timeout with
                | None -> resultCell.AsyncWaitResult()
                | Some t ->
                    async {
                        let! r = resultCell.AsyncWaitResult t
                        
                        match r with
                        | None -> return! Async.Raise <| TimeoutException()
                        | Some v -> return v
                    }

        [<RequireQualifiedAccess>]
        module Async =
            /// postcompose covariant operation
            let map (f : 'T -> 'S) (w : Async<'T>) : Async<'S> =
                async { let! r = w in return f r }

            /// lifting of lambdas to async funcs
            let lift (f : 'T -> 'S) = fun t -> async { return f t }

            /// nodeterministic pick
            let tryPick (f : 'T -> Async<'S option>) (ts : seq<'T>) : Async<'S option> =
                ts |> Seq.map f |> Async.Choice

            /// nondeterministic pick
            let pick (f : 'T -> Async<'S>) (ts : seq<'T>) : Async<'S> =
                async {
                    let! result = ts |> Seq.map (fun t -> map Some (f t)) |> Async.Choice

                    return result.Value
                }

            /// nondeterministic forall
            let forall (f : 'T -> Async<bool>) (ts : seq<'T>) : Async<bool> =
                let wrapper t = map (function true -> None | false -> Some ()) (f t)
                ts |> Seq.map wrapper |> Async.Choice |> map Option.isNone

            /// nondeterministic existential
            let exists (f : 'T -> Async<bool>) (ts : seq<'T>) : Async<bool> =
                let wrapper t = map (function true -> Some () | false -> None) (f t)
                ts |> Seq.map wrapper |> Async.Choice |> map Option.isSome

        [<RequireQualifiedAccess>]
        module Seq =
            /// parallel pick
            let pPick (f : 'T -> 'S) (ts : seq<'T>) = Async.pick (Async.lift f) ts |> Async.RunSynchronously
            /// parallel tryPick
            let pTryPick (f : 'T -> 'S option) (ts : seq<'T>) = Async.tryPick (Async.lift f) ts |> Async.RunSynchronously
            /// parallel forall
            let pForall (f : 'T -> bool) (ts : seq<'T>) = Async.forall (Async.lift f) ts |> Async.RunSynchronously
            /// parallel exists
            let pExists (f : 'T -> bool) (ts : seq<'T>) = Async.exists (Async.lift f) ts |> Async.RunSynchronously

        /// async failwith
        let afailwith msg = Async.Raise(Exception msg) : Async<'T>
        /// async failwithf
        let afailwithf fmt = Printf.ksprintf afailwith fmt : Async<'T>


    [<RequireQualifiedAccess>]
    module Process =
        let startAndAwaitTerminationAsync(psi : ProcessStartInfo) =
            async {
                let proc = new Process()
                proc.StartInfo <- psi
                proc.EnableRaisingEvents <- true
                if proc.Start() then
                    let! _ = Async.AwaitObservable proc.Exited
                    return proc
                else
                    return failwith "error starting process"
            }

        let startAndAwaitTermination(psi : ProcessStartInfo) =
            startAndAwaitTerminationAsync psi |> Async.RunSynchronously


    [<RequireQualifiedAccess>]
    module List =
        let rec foldAsync (foldF: 'U -> 'T -> Async<'U>) (state: 'U) (items: 'T list): Async<'U> =
            async {
                match items with
                | [] -> return state
                | item::rest ->
                    let! nextState = foldF state item
                    return! foldAsync foldF nextState rest
            }

        let foldBackAsync (foldF: 'T -> 'U -> Async<'U>) (items: 'T list) (state: 'U): Async<'U> =
            let rec loop is k = async {
                match is with
                | [] -> return! k state
                | h::t -> return! loop t (fun acc -> async { let! acc' = foldF h acc in return! k acc' })
            }

            loop items async.Return

        let mapAsync (mapF: 'T -> Async<'U>) (items: 'T list): Async<'U list> =
            foldBackAsync (fun i is -> async { let! i' = mapF i in return i'::is }) items []

        let chooseAsync (choiceF: 'T -> Async<'U option>) (items: 'T list): Async<'U list> =
            foldBackAsync (fun i is -> async { let! r = choiceF i in return match r with Some i' -> i'::is | _ -> is }) items []
         
    [<RequireQualifiedAccess>]   
    module Array =
        let foldAsync (foldF: 'U -> 'T -> Async<'U>) (state: 'U) (items: 'T []): Async<'U> =
            let rec foldArrayAsync foldF state' index =
                async {
                    if index = items.Length then
                        return state'
                    else
                        let! nextState = foldF state' items.[index]
                        return! foldArrayAsync foldF nextState (index + 1)
                }
            foldArrayAsync foldF state 0