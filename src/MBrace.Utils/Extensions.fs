namespace Nessos.MBrace.Utils

    open System
    open System.Collections.Generic
    open System.Runtime.Serialization

    [<AutoOpen>]
    module Extensions =

        [<RequireQualifiedAccess>]
        module Option =

            let filter f x =
                match x with
                | None -> None
                | Some x -> if f x then Some x else None

            let ofNullable<'T when 'T : null> (x : 'T) = 
                match x with null -> None | x -> Some x

            /// returns the head of a list if nonempty
            let ofList = function [] -> None | h :: _ -> Some h

            /// match t with None -> s | Some t0 -> f t0
            let bind2 (f : 'T -> 'S) (s : 'S) (t : 'T option) =
                match t with None -> s | Some t0 -> f t0


        [<RequireQualifiedAccess>]
        module Seq =

            let ofOption (xs : seq<'a> option) : seq<'a> =
                match xs with
                | None -> seq []
                | Some xs -> xs

            let toOption (xs : seq<'a>) : seq<'a> option =
                if Seq.isEmpty xs then None else Some xs

            let tryHead (xs: seq<'a>): 'a option =
                if Seq.isEmpty xs then None else xs |> Seq.head |> Some

            /// a wrapper for Async.Parallel
            let parMap (f : 'T -> 'S) (inputs : 'T seq) : 'S [] =
                inputs
                |> Seq.map (fun x -> async { return f x }) 
                |> Async.Parallel
                |> Async.RunSynchronously


        [<RequireQualifiedAccess>]
        module List =

            let take n (xs : 'a list) = 
                let rec aux =
                    function
                    | carry, 0, _ -> List.rev carry
                    | carry, n, x::xs' -> aux (x::carry, n-1, xs')
                    | _ -> raise <| new ArgumentException("List.take: list too small or bad argument")

                aux ([],n,xs)

            let rec drop n (xs : 'a list) =
                match n,xs with
                | 0, _ -> xs
                | n, x::xs' -> drop (n-1) xs'
                | _ -> raise <| new ArgumentException("List.take: list too small or bad argument")

            let filteri (predicate: int -> 'a -> bool) (list : 'a list) =
                let rec aux i carry rest =
                    match rest with
                    | [] -> List.rev carry
                    | hd :: tl ->
                        if predicate i hd then
                            aux (i+1) (hd :: carry) tl
                        else
                            aux (i+1) carry tl

                aux 0 [] list

            let partitioni (f : int -> 'a -> bool) (xs : 'a list) =
                let rec partitioner left right i rest =
                    match rest with
                    | [] -> (List.rev left, List.rev right)
                    | x :: rest' ->
                        if f i x then
                            partitioner (x :: left) right (i+1) rest'
                        else
                            partitioner left (x :: right) (i+1) rest'

                partitioner [] [] 0 xs

            let splitAt n (xs : 'a list) =
                let rec splitter n (left : 'a list) right =
                    match n, right with
                    | 0 , _ | _ , [] -> List.rev left, right
                    | n , h :: right' -> splitter (n-1) (h::left) right'

                splitter n [] xs

            let split (xs : 'a list) = splitAt (xs.Length / 2) xs

            let groupBy (f : 'T -> 'S) (xs : 'T list) =
                let map = Seq.groupBy f xs |> Seq.map (fun (x,y) -> x, List.ofSeq y) |> Map.ofSeq
                fun (s : 'S) -> match map.TryFind s with None -> [] | Some xs -> xs

            let rec last xs =
                match xs with
                | [] -> invalidArg "xs" "input list is empty."
                | [x] -> x
                | _ :: rest -> last rest

            let rec tryLast xs =
                match xs with
                | [] -> None
                | [x] -> Some x
                | _ :: rest -> tryLast rest


            let tryMap (f : 'T -> 'S option) (ts : 'T list) : 'S list option =
                let rec gather acc rest =
                    match rest with
                    | [] -> Some <| List.rev acc
                    | h :: t ->
                        match f h with
                        | Some s -> gather (s :: acc) t
                        | None -> None

                gather [] ts

            /// List.map wrapper for Async.Parallel
            let parMap (f : 'T -> 'S) (xs : 'T list) =
                Seq.parMap f xs |> Seq.toList

            /// List.choose wrapper for Async.Parallel
            let parChoose (f : 'T -> 'S option) (xs : 'T list) =
                Seq.parMap f xs |> Seq.choose id |> Seq.toList

            let (|Map|) f xs = List.map f xs
            let (|TryMap|_|) f xs = tryMap f xs

        [<RequireQualifiedAccess>]
        module Choice =
            let split (inputs : Choice<'T, 'S> list) =
                let rec helper (ts, ss, rest) =
                    match rest with
                    | [] -> List.rev ts, List.rev ss
                    | Choice1Of2 t :: rest -> helper (t :: ts, ss, rest)
                    | Choice2Of2 s :: rest -> helper (ts, s :: ss, rest)

                helper ([], [], inputs)

            let splitArray (inputs: Choice<'T, 'U> []): 'T[] * 'U[] =
                    inputs |> Array.choose (function Choice1Of2 r -> Some r | _ -> None),
                    inputs |> Array.choose (function Choice2Of2 r -> Some r | _ -> None)

        [<RequireQualifiedAccess>]
        module Map =
            let AddMany (pairs : ('k * 'v) seq) (m : Map<'k,'v>) =
                let mutable m = m
                for k,v in pairs do
                    m <- m.Add(k,v)
                m

        [<RequireQualifiedAccess>]
        module Set =
            let addMany (ts : 'T seq) (s : Set<'T>) =
                let mutable s = s
                for t in ts do
                    s <- s.Add t
                s

            let (|Empty|Cons|) (s : Set<'T>) =
                if s.IsEmpty then Empty
                else
                    let first = Seq.head s
                    Cons(first, Set.remove first s)

        [<RequireQualifiedAccess>]
        module Boolean =
            let tryParse (x : string) =
                let mutable res = false
                if Boolean.TryParse(x,&res) then Some res else None

        type SerializationInfo with
            member inline s.Read<'T> param = s.GetValue(param, typeof<'T>) :?> 'T
            member inline s.Write<'T> param (value : 'T) = s.AddValue(param, value)


        type IDictionary<'K,'V> with
            member d.TryFind (k : 'K) =
                let found, v = d.TryGetValue k
                if found then Some v else None


        [<AbstractClass>]
        type Existential internal () =
            abstract Type : Type
            abstract Apply : func:IFunc<'R> -> 'R

            static member Create(t : Type) =
                let et = typedefof<Existential<_>>.MakeGenericType [|t|]
                Activator.CreateInstance(et) :?> Existential

        and Existential<'T> () =
            inherit Existential()

            override __.Type = typeof<'T>
            override __.Apply func = func.Invoke<'T> ()

        and IFunc<'R> =
            abstract Invoke<'T> : unit -> 'R