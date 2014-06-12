namespace Nessos.MBrace.Utils

    open System
    open System.IO
    open System.Net
    open System.Net.Sockets
    open System.Collections.Generic
    open System.Runtime.Serialization
    open System.Reflection
    open System.Text
    open System.Text.RegularExpressions

    open Microsoft.FSharp.Quotations

    open Nessos.Thespian.ConcurrencyTools

    #nowarn "42"

    [<AutoOpen>]
    module Utils =

        /// a raise operator that does not appear in the stack trace
        let inline raise (e: System.Exception) = (# "throw" e : 'U #)

        /// raise exception with given object
        let inline throwInvalidState value = failwithf "Invalid state %A" value

        /// strips nullable types
        let inline denull< ^T when ^T : null> (x : ^T) = 
            match x with null -> None | x -> Some x

        /// System.Diagnostics.Process.GetCurrentProcess()
        let selfProc = System.Diagnostics.Process.GetCurrentProcess()

        /// detect the running version of F#
        let fsharpVersion = typeof<int option>.Assembly.GetName().Version

        let isFsharp31 = fsharpVersion >= System.Version("4.3.1")

        /// determines if current process runs in console window
        let isConsoleWindow = Environment.UserInteractive && selfProc.MainWindowHandle <> 0n
        
        /// a guid that identifies this specific AppDomain
        let processUUID = Guid.NewGuid ()

        let private remoteStackTraceField =
            let getField name = typeof<System.Exception>.GetField(name, BindingFlags.Instance ||| BindingFlags.NonPublic)
            match getField "remote_stack_trace" with
            | null ->
                match getField "_remoteStackTraceString" with
                | null -> failwith "a piece of unreliable code has just failed."
                | f -> f
            | f -> f

        let setStackTrace (trace : string) (e : exn) = remoteStackTraceField.SetValue(e, trace)

        /// reraise exception, keeping its stacktrace intact
        let inline reraise' (e : #exn) =
            setStackTrace (e.StackTrace + System.Environment.NewLine) e
            raise e


        //
        //  memoization combinators
        //

        /// higher-order thread-safe memoizer operator
        let memoize f =
            let cache = Atom Map.empty

            fun id ->
                match cache.Value.TryFind id with
                | None ->
                    let result = f id
                    Atom.swap cache (fun c -> c.Add(id, result))
                    result
                | Some result -> result

        /// only memoize successful results of given function
        let tryMemoize (cmp : 'T -> 'Comparable) (f : 'T -> 'S option) =
            let cache = Atom Map.empty

            fun x ->
                let id = cmp x
                match cache.Value.TryFind id with
                | None ->
                    match f x with
                    | None -> None
                    | Some v -> Atom.swap cache (fun c -> c.Add(id, v)) ; Some v
                | Some v -> Some v

        /// memoizing fixpoint combinator
        let Ymemoize (F : ('T -> 'S) -> 'T -> 'S) : 'T -> 'S =
            let cache = Atom Map.empty

            let rec YF t =
                match cache.Value.TryFind t with
                | None ->
                    let s = F YF t
                    cache.Swap(fun c -> c.Add(t,s)) ; s
                | Some s -> s

            YF

        /// memoization with comparison delegate
        let memoizeBy (comparer : 'T -> 'Comparable) (f : 'T -> 'S) =
            let cache = Atom Map.empty

            fun x ->
                let id = comparer x
                match cache.Value.TryFind id with
                | None ->
                    let result = f x
                    Atom.swap cache (fun c -> c.Add(id, result))
                    result
                | Some result -> result


        /// projective equality
        let inline equalsOn f x (yobj:obj) =
            match yobj with
            | :? 'T as y -> (f x = f y)
            | _ -> false
        
        /// projective hashcode
        let inline hashOn f x = hash (f x)
 
        /// projective comparison
        let inline compareOn f x (yobj: obj) =
            match yobj with
            | :? 'T as y -> compare (f x) (f y)
            | _ -> invalidArg "yobj" "cannot compare values of different types"

        //
        //  set with custom comparison semantics
        //

        let runsOnMono =
            match Type.GetType("Mono.Runtime") with
            | null -> false
            | _ -> true


        /// wraps the function in a memoized context
        let runOnce (f : unit -> 'a) =
            let result = lazy( f () )
            fun () -> result.Value

        /// a really, *really* bad way of getting available tcp ports
        let getAvailableTcpPorts total =
            let getListener _ = 
                let listener = new TcpListener(IPEndPoint(IPAddress.Loopback, 0));
                listener.Start()
                let port = (listener.LocalEndpoint :?> IPEndPoint).Port
                listener,port

            [1..total]  
            |> List.map getListener 
            |> List.map (fun (listener,port) -> listener.Stop() ; port)

        /// mutable hashset combinator
        let hset (xs : seq<'T>) = new HashSet<'T>(xs)

        // fast sprintf implementation:
        // http://t0yv0.blogspot.com/2012/07/speeding-up-f-printf.html

        type internal Cache<'T> private () =
            static let cache = new System.Collections.Concurrent.ConcurrentDictionary<_,_>()

            static member Format(format: Printf.StringFormat<'T>) : 'T =
                let key = format.Value
                let ok, value = cache.TryGetValue key
                if ok then value
                else
                    let f = sprintf format
                    let _ = cache.TryAdd(key, f)
                    f

        /// fast sprintf
        let sprintf' fmt =
            if isFsharp31 then sprintf fmt
            else
                Cache<_>.Format(fmt)

        /// memoized regex active pattern
        let (|RegexMatch|_|) =
            let regex = memoize(fun pattern -> Regex(pattern))
            
            fun (pat : string) (inp : string) ->
                let m = (regex pat).Match inp in
                if m.Success 
                then Some (List.tail [ for g in m.Groups -> g.Value ])
                else None


        /// a heuristic host identifier
        /// an id generated by the local computer
        /// should be able to uniquely identify it within the context
        /// of all possible subnets/domain names that connect to it
        type HostId =
            private {
                HostName : string
                Interfaces : Set<string>
            }
        with
            static member Local =
                let hostname = Dns.GetHostName()
                let ifs = Dns.GetHostAddresses(hostname) |> Array.map (fun i -> i.ToString()) |> Set.ofArray
                { HostName = hostname ; Interfaces = ifs }

        /// inherit this class if you want to scrap all the comparison boilerplate
        type ProjectionComparison<'Id, 'Cmp when 'Cmp : comparison> (token : 'Cmp) =
            member private __.ComparisonToken = token

            interface IComparable with
                member x.CompareTo y =
                    match y with
                    | :? ProjectionComparison<'Id, 'Cmp> as y -> compare token y.ComparisonToken
                    | _ -> invalidArg "y" "invalid comparand."

            override x.Equals y =
                match y with
                | :? ProjectionComparison<'Id, 'Cmp> as y -> token = y.ComparisonToken
                | _ -> false

            override x.GetHashCode() = hash token