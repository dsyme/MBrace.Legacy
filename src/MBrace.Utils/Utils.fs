namespace Nessos.MBrace.Utils

    open System
    open System.IO
    open System.Net
    open System.Net.Sockets
    open System.Collections.Generic
    open System.Runtime.Serialization
    open System.Text
    open System.Text.RegularExpressions

    open Microsoft.FSharp.Quotations

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

        /// determines if current process runs in console window
        let isConsoleWindow = Environment.UserInteractive && selfProc.MainWindowHandle <> 0n
        
        /// a guid that identifies this specific AppDomain
        let processUUID = Guid.NewGuid ()


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


        /// an incrementing id generator that begins at given id
        let getIdGenerator (init : int) =
            let current = ref init
            fun () -> incr current ; current.Value

        /// try Choice1Of2 <| f () with e -> Choice2Of2 e
        let contain (f : unit -> 'T) : Choice<'T, exn> =
            try Choice1Of2 <| f () with e -> Choice2Of2 e

        let containAsync (f : Async<'T>) : Async<Choice<'T, exn>> =
            async {
                try
                    let! value = f
                    return Choice1Of2 value
                with e ->
                    return Choice2Of2 e
            }

        /// try Some <| f () with ex -> None
        let tryMe (f : unit -> 'T) : 'T option =
            try Some <| f () with ex -> None

        // option monad

        type OptionBuilder () =
            member __.Zero () = Some ()
            member __.Return x = Some x
            member __.Bind(x,f) = Option.bind f x
            member __.Using<'T, 'U when 'T :> IDisposable>(x : 'T, f : 'T -> 'U option) : 'U option =
                let r = f x in x.Dispose () ; r
            member __.Combine(x : unit option, y : 'T option) = y
            member __.ReturnFrom (x : 'T option) = x

        /// optional monad
        let maybe = new OptionBuilder ()

        /// mutable hashset combinator
        let hset (xs : seq<'T>) = new HashSet<'T>(xs)

        // fast sprintf implementation:
        // http://t0yv0.blogspot.com/2012/07/speeding-up-f-printf.html

        type internal Cache<'T> private () =
            static let atomMap = Atom.atom Map.empty<string, 'T>

            static member Format(format: Printf.StringFormat<'T>) : 'T =
                let key = format.Value
                match atomMap.Value.TryFind(key) with
                | Some r -> r
                | _ ->
                    let r = sprintf format
                    Atom.swap atomMap (fun map -> Map.add key r map)
                    r

        /// fast sprintf
        let sprintf' fmt = Cache<_>.Format(fmt)

        /// memoized regex active pattern
        let (|RegexMatch|_|) =
            let regex = memoize(fun pattern -> Regex(pattern))
            
            fun (pat : string) (inp : string) ->
                let m = (regex pat).Match inp in
                if m.Success 
                then Some (List.tail [ for g in m.Groups -> g.Value ])
                else None


        //
        // a "generic" parser for basic types
        //

        let private parsers =
            let inline dc x = x :> obj
            [
                typeof<bool>, System.Boolean.Parse >> dc
                typeof<byte>, byte >> dc
                typeof<sbyte>, sbyte >> dc
                typeof<int32>, int32 >> dc
                typeof<int64>, int64 >> dc
                typeof<int16>, int16 >> dc
                typeof<uint16>, uint16 >> dc
                typeof<uint32>, uint32 >> dc
                typeof<uint64>, uint64 >> dc
                typeof<float32>, float32 >> dc
                typeof<float>, float >> dc
                typeof<nativeint>, int64 >> dc
                typeof<unativeint>, uint64 >> dc
                typeof<decimal>, decimal >> dc
                typeof<System.DateTime>, System.DateTime.Parse >> dc
                typeof<System.Decimal>, System.Decimal.Parse >> dc
                typeof<System.Guid>, System.Guid.Parse >> dc
                typeof<string>, dc
            ] 
            |> Seq.map(fun (t,p) -> t.AssemblyQualifiedName, p)
            |> Map.ofSeq

        let tryGetParser<'T> = 
            let inline uc (x : obj) = x :?> 'T
            parsers.TryFind typeof<'T>.AssemblyQualifiedName
            |> Option.map (fun p -> p >> uc)