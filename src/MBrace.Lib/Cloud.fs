namespace Nessos.MBrace.Lib

    open Nessos.MBrace

    [<RequireQualifiedAccess>]
    module Cloud =

        /// <summary>Builds a parallel cloud computation of applying the given function
        /// to each of the elements of the collection.</summary>
        /// <param name="mapping">The computation builder function.</param>
        /// <param name="inputs">The input list.</param>
        /// <returns>A parallel cloud expression.</returns>
        [<Cloud>]
        let parmap (mapping : 'I -> Cloud<'R>) (inputs : 'I []) : Cloud<'R []> =
                inputs |> Array.map mapping |> Cloud.Parallel

        /// Returns the first computation that returns Some x, None if all return None.
        [<Cloud>]
        let tryPick (f : 'I -> Cloud<'R option>) (inputs : 'I []) : Cloud<'R option> =
                inputs |> Array.map f |> Cloud.Choice

        /// Returns whether any of the inputs satisfy the given function.
        /// Based on the choice operator.
        [<Cloud>]
        let exists (f : 'I -> Cloud<bool>) (inputs : 'I []) : Cloud<bool> =
            let wrapper inp =
                cloud {
                    let! r = f inp
                    return
                        if r then Some ()
                        else None
                }
            cloud {
                let! result = tryPick wrapper inputs

                return
                    match result with
                    | None -> false
                    | Some _ -> true
            }

        /// Checks if all the inputs satisfy the given function.
        /// Based on the choice operator.
        [<Cloud>]
        let forall (f : 'I -> Cloud<bool>) (inputs : 'I []) : Cloud<bool> =
            cloud { 
                let! r = exists (fun v -> cloud { let! v' = f v in return not v' }) inputs 
                return not r
            }

        /// Returns the result of the first computation that happens to finish.
        [<Cloud>]
        let pick (f : 'I -> Cloud<'R>) (inputs : 'I []) : Cloud<'R> =
            let wrapper inp = cloud { let! r = f inp in return Some r }

            cloud {
                let! result = tryPick wrapper inputs

                return result.Value
            }

        [<Cloud>]
        /// Lifts a function returning 'b to a function returning Cloud<'b>
        let lift (f: 'a -> 'b) : 'a -> Cloud<'b> =
            fun x -> cloud { return f x }

        [<Cloud>]
        /// Like lift but with two curried arguments.
        let lift2 (f: 'a -> 'b -> 'c) : 'a -> 'b -> Cloud<'c> =
            fun x y -> cloud { return f x y }

    [<RequireQualifiedAccess>]
    module CloudRef = 

        [<Cloud>]
        /// Applies the function to a CloudRef and returns a new CloudRef with the resulting value.
        let map (f : 'T -> 'S) (r : ICloudRef<'T>) : Cloud<ICloudRef<'S>> =
            cloud {
                let v = r.Value
                return! CloudRef.New (f v)
            }