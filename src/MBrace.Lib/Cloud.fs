namespace Nessos.MBrace.Lib

    open Nessos.MBrace

    /// Cloud combinators module.
    [<RequireQualifiedAccess>]
    module Cloud =

        /// <summary>
        ///     Builds a parallel cloud computation of applying the given function
        ///     to each of the elements of the collection.
        /// </summary>
        /// <param name="mapping">The computation builder function.</param>
        /// <param name="inputs">The input list.</param>
        /// <returns>A parallel cloud expression.</returns>
        [<Cloud>]
        let parmap (mapping : 'I -> Cloud<'R>) (inputs : seq<'I>) : Cloud<'R []> =
            inputs |> Seq.map mapping |> Cloud.Parallel

        /// <summary>
        ///   Returns the first computation that returns Some x, None if all return None.  
        /// </summary>
        /// <param name="f">Selection function.</param>
        /// <param name="inputs">input sequence.</param>
        [<Cloud>]
        let tryPick (f : 'I -> Cloud<'R option>) (inputs : seq<'I>) : Cloud<'R option> =
            inputs |> Seq.map f |> Cloud.Choice

        /// <summary>
        ///     Distributively checks the existential quantifier for given set of inputs.
        /// </summary>
        /// <param name="predicate">predicate to be checked.</param>
        /// <param name="inputs">input elements.</param>
        [<Cloud>]
        let exists (predicate : 'I -> Cloud<bool>) (inputs : seq<'I>) : Cloud<bool> =
            let check inp =
                cloud {
                    let! r = predicate inp
                    return
                        if r then Some ()
                        else None
                }

            cloud {
                let! result = tryPick check inputs
                return result.IsSome
            }

        /// <summary>
        ///     Distributively checks the universal quantifier for given set of inputs.
        /// </summary>
        /// <param name="predicate">predicate to be checked.</param>
        /// <param name="inputs">input elements.</param>
        [<Cloud>]
        let forall (predicate : 'I -> Cloud<bool>) (inputs : seq<'I>) : Cloud<bool> =
            let check inp =
                cloud {
                    let! r = predicate inp
                    return
                        if r then None
                        else Some ()
                }

            cloud {
                let! result = tryPick check inputs
                return result.IsNone
            }

        /// <summary>
        ///   Returns the result of the first computation that happens to finish.  
        /// </summary>
        /// <param name="f">pick function.</param>
        /// <param name="inputs">inputs.</param>
        [<Cloud>]
        let pick (f : 'I -> Cloud<'R>) (inputs : seq<'I>) : Cloud<'R> =
            let check inp = cloud { let! r = f inp in return Some r }

            cloud {
                let! result = tryPick check inputs
                return result.Value
            }

        /// <summary>
        ///     Sequential fold workflow.
        /// </summary>
        /// <param name="folder">fold function.</param>
        /// <param name="state">initial state.</param>
        /// <param name="list">input list</param>
        [<Cloud>]
        let rec fold (folder : 'State -> 'T -> Cloud<'State>) (state : 'State) (list : 'T list) = cloud {
            match list with
            | [] -> return state
            | hd :: tl ->
                let! state' = folder state hd
                return! fold folder state' tl
        }

        /// <summary>
        ///     Lifts a function returning 'b to a function returning Cloud<'b>
        /// </summary>
        /// <param name="f">function to be lifted.</param>
        [<Cloud>]
        let lift (f: 'a -> 'b) : 'a -> Cloud<'b> =
            fun x -> cloud { return f x }

        /// <summary>
        ///     Like lift but with two curried arguments.  
        /// </summary>
        /// <param name="f">function to be lifted.</param>
        [<Cloud>]
        let lift2 (f: 'a -> 'b -> 'c) : 'a -> 'b -> Cloud<'c> =
            fun x y -> cloud { return f x y }

    /// CloudRef combinators module.
    [<RequireQualifiedAccess>]
    module CloudRef = 

        /// <summary>
        ///     Applies the function to a CloudRef and returns a new CloudRef with the resulting value.  
        /// </summary>
        /// <param name="f">map function.</param>
        /// <param name="r">input CloudRef.</param>
        [<Cloud>]
        let map (f : 'T -> 'S) (r : ICloudRef<'T>) : Cloud<ICloudRef<'S>> =
            cloud {
                let v = r.Value
                return! CloudRef.New (f v)
            }