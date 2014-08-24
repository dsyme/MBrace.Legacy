module Nessos.MBrace.Lib.MapReduce

    open Nessos.MBrace

    /// <summary>
    ///     Represents a binary-decomposable recursive type abstraction for Map-Reduce workflows
    /// </summary>
    type ContainerState<'Container, 'T> =
        | Empty
        | Leaf of 'T
        | Branch of 'Container * 'Container
    
    /// A representation of the current execution context.
    /// The context can be cloud distribution, local distribution across cpu cores,
    /// or sequential execution.
    type private ParallelismContext = 
        | CloudParallel 
        | LocalParallel 
        | Sequential

    /// <summary>
    ///     A distributed divide-and-conquer MapReduce workflow.
    ///     Branches out until cluster size is exhausted; thereby continuing with thread local parallel semantics.
    ///     Works with any input that can be decomposed to a binary tree-like structure.
    /// </summary>
    /// <param name="decomposeF">input data decompose function.</param>
    /// <param name="mapF">map function.</param>
    /// <param name="reduceF">reduce function.</param>
    /// <param name="identity">identity element.</param>
    /// <param name="input">initial input data.</param>
    [<Cloud>]
    let mapReduce (decomposeF : 'I -> Cloud<ContainerState<'I, 'T>>)
                    (mapF : 'T -> Cloud<'R>) (reduceF : 'R -> 'R -> Cloud<'R>) 
                    (identity : unit -> Cloud<'R>) (input : 'I) =

        let rec aux context depth (input : 'I) = cloud {

            let! result = decomposeF input

            match context, depth, result with
            | _, _, Empty -> return! identity ()
            | _, _, Leaf t -> return! mapF t
            | CloudParallel, 0, _ ->
                let cores = System.Environment.ProcessorCount
                let depth = log2 cores
                return! Cloud.ToLocal <| aux LocalParallel depth input

            | LocalParallel, 0, _ -> return! aux Sequential 0 input
            | (CloudParallel | LocalParallel), d, Branch(left, right) ->
                let! r1,r2 = aux context (depth - 1) left <||> aux context (depth - 1) right
                return! reduceF r1 r2

            | Sequential, _, Branch(left, right) ->
                let! r1 = aux Sequential 0 left
                let! r2 = aux Sequential 0 right
                return! reduceF r1 r2
        }

        cloud {
            let! workers = Cloud.GetWorkerCount()
            let depth = log2 workers
            return! aux CloudParallel depth input
        }


    [<RequireQualifiedAccess>]
    module Seq =
        
        /// <summary>
        ///     A distributed divide-and-conquer MapReduce workflow.
        ///     Branches out until cluster size is exhausted; thereby continuing with thread local parallel semantics.
        /// </summary>
        /// <param name="mapF">map function.</param>
        /// <param name="reduceF">reduce function.</param>
        /// <param name="identity">identity element.</param>
        /// <param name="input">initial input data.</param>
        [<Cloud>]
        let mapReduce (mapF : 'T -> Cloud<'R>) 
                        (reduceF : 'R -> 'R -> Cloud<'R>)
                        (identity : unit -> Cloud<'R>) (input : seq<'T>) : Cloud<'R> =

            let decompose (input : 'T []) = cloud {
                return
                    match input.Length with
                    | 0 -> Empty
                    | 1 -> Leaf <| input.[0]
                    | _ -> let l,r = Array.split input in Branch(l,r)
            }

            mapReduce decompose mapF reduceF identity (Seq.toArray input)


    [<RequireQualifiedAccess>]
    module CloudTree =

        type private Container<'T> = ContainerState<CloudTreeRef<'T>, 'T>

        /// <summary>
        ///     A distributed divide-and-conquer MapReduce workflow.
        ///     Branches out until cluster size is exhausted; thereby continuing with thread local parallel semantics.
        /// </summary>
        /// <param name="mapF">map function.</param>
        /// <param name="reduceF">reduce function.</param>
        /// <param name="identity">identity element.</param>
        /// <param name="input">initial input data.</param>
        [<Cloud>]
        let mapReduce (mapF : 'T -> Cloud<'R>) 
                        (reduceF : 'R -> 'R -> Cloud<'R>)
                        (identity : unit -> Cloud<'R>) (input : CloudTreeRef<'T>) : Cloud<'R> =

            let decompose (tree : CloudTreeRef<'T>) : Cloud<Container<'T>> = cloud {
                return
                    match tree.Value with
                    | CloudTree.Empty -> Container<'T>.Empty
                    | CloudTree.Leaf t -> Container<'T>.Leaf t
                    | CloudTree.Branch(l,r) -> Container<'T>.Branch(l,r)
            }

            mapReduce decompose mapF reduceF identity input