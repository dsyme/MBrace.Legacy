namespace Nessos.MBrace.Utils

    open System
    open System.Diagnostics
    open System.Threading

    open Nessos.Thespian.ConcurrencyTools
    
    /// thread safe cache with expiry semantics
    type CacheAtom<'T> internal (provider : 'T option -> 'T, ?interval : int, ?initial : 'T) =

        let interval = defaultArg interval 1000 |> float |> TimeSpan.FromMilliseconds
        let initial = match initial with None -> Undefined | Some t -> Success(t, DateTime.Now)
        let container = Atom.atom<CacheState<'T>> initial

        let update state =
            let inline compute lastSuccessful =
                try Success(provider lastSuccessful, DateTime.Now)
                with e -> Error(e, DateTime.Now, lastSuccessful)

            let state' =
                match state with
                | Undefined -> compute None
                | Success (_,time) when DateTime.Now - time <= interval -> state
                | Error (_,time,_) when DateTime.Now - time <= interval -> state
                | Success (t,_) -> compute <| Some t
                | Error (_,_,last) -> compute last

            match state' with
            | Success(t,_) -> state', Choice1Of2 t
            | Error(e,_,_) -> state', Choice2Of2 e
            | _ -> failwith "impossible"

        member __.Value =
            match container.Transact update with
            | Choice1Of2 v -> v
            | Choice2Of2 e -> raise e

    and private CacheState<'T> =
        | Undefined
        | Error of exn * DateTime * 'T option // last successful state
        | Success of 'T * DateTime

    and CacheAtom =

        static member Create(provider : unit -> 'T, ?keepLastResultOnError, ?interval, ?initial) =
            let keepLastResultOnError = defaultArg keepLastResultOnError false
            let providerW (state : 'T option) =
                try provider ()
                with e ->
                    match state with
                    | Some t when keepLastResultOnError -> t
                    | _ -> reraise ()
            
            new CacheAtom<'T>(providerW, ?interval = interval, ?initial = initial)

        static member Create(init : 'T, f : 'T -> 'T, ?interval, ?keepLastResultOnError) =
            let keepLastResultOnError = defaultArg keepLastResultOnError false
            let providerW (state : 'T option) =
                try f state.Value
                with e ->
                    match state with
                    | Some t when keepLastResultOnError -> t
                    | _ -> reraise ()

            new CacheAtom<'T>(providerW, ?interval = interval, initial = init)


    /// thread-safe operators
    [<RequireQualifiedAccess>]
    module ThreadSafe =

        let incr (t : int ref) = Interlocked.Increment t |> ignore
        let decr (t : int ref) = Interlocked.Decrement t |> ignore

        let rec update (f : int -> int) (cell : int ref) =
            let value = cell.Value
            let result = Interlocked.CompareExchange(cell, f value, value)
            if result = value then () else update f cell

        /// will only return a mutex if name hasn't been claimed yet
        let tryClaimGlobalMutex (name : string) =
            // @ http://social.msdn.microsoft.com/forums/en-US/netfxbcl/thread/47e6ee95-f2dc-45cd-b456-0e755b99bb52
            let name = @"Global\" + name.Replace('\\', '/')
            let isCreatedNew = ref false
            try
                let mtx = new Mutex(true, name, isCreatedNew)
                if !isCreatedNew then Some mtx
                else mtx.Close () ; None
            with _ -> None


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