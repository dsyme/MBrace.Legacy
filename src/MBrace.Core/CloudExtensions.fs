namespace Nessos.MBrace

    [<AutoOpen>]
    /// A module that provides extensions to use with the CloudRef primitive.
    module CloudRefExtensions =

        [<Cloud>]
        /// Creates a new CloudRef. This is a shorthand for the 'CloudRef.New' method.
        let newRef value = CloudRef.New value 
    
        /// An active pattern that matches an ICloudRef and returns its value.
        let (|CloudRef|) (cloudRef : ICloudRef<'T>) = cloudRef.Value

    [<AutoOpen>]
    /// Some extensions to the Cloud type that are not primitives but don't belong to MBrace.Lib.
    module CloudExtensions = 
        type Cloud with
            /// <summary>Performs given cloud computation discarding the result.</summary>
            /// <param name="computation">The computation input.</param>
            /// <returns>A cloud expression returning unit.</returns>
            [<Cloud; NoTraceInfo>]
            static member Ignore (computation : Cloud<'T>) : Cloud<unit> =
                cloud { let! _ = computation in return () }

            /// <summary>Disposes a cloud resource.</summary>
            /// <param name="resource">The resource for disposal.</param>
            /// <returns>A cloud expression returning unit.</returns>
            [<Cloud; NoTraceInfo>]
            static member Dispose<'T when 'T :> ICloudDisposable>(resource : 'T) : Cloud<unit> =
                Cloud.OfAsync (resource.Dispose())
            
            /// <summary>Creates a cloud computation that executes a specified computation.
            /// If this computation completes successfully, then this method returns Choice1Of2 
            /// with the returned value. If this computation raises an exception before it
            /// completes then return Choice2Of2 with the raised exception.</summary>
            /// <param name="computation">The computation input.</param>
            /// <returns>A cloud computation that returns a Choice of type 'T or an exception.</returns>
            [<Cloud; NoTraceInfo>]
            static member Catch(computation : Cloud<'T>) : Cloud<Choice<'T, exn>> =
                cloud {
                    try
                        let! result = computation
                        return Choice1Of2 result
                    with ex ->
                        return Choice2Of2 ex
                }
            
            /// <summary>Creates a cloud computation that will sleep for the given time.</summary>
            /// <param name="millisecondsDueTime">The number of milliseconds to sleep.</param>
            /// <returns>A cloud computation that will sleep for the given time.</returns>
            [<Cloud; NoTraceInfo>]
            static member Sleep(millisecondsDueTime : int) =
                Cloud.OfAsync <| Async.Sleep(millisecondsDueTime)

    [<AutoOpen>]
    /// A module containing some useful operators for cloud computations.
    module CloudOperators =

        [<Cloud>]
        /// Converts an Cloud computation to a computation that will
        /// be executed locally.
        let local (computation : Cloud<'T>) : Cloud<'T> =
            Cloud.ToLocal computation

        /// <summary>
        /// Combines two cloud computations into one that executes them in parallel.
        /// </summary>
        /// <param name="left">The first cloud computation.</param>
        /// <param name="right">The second cloud computation.</param>
        [<Cloud>]
        [<NoTraceInfo>]
        let (<||>) (left : Cloud<'a>) (right : Cloud<'b>) : Cloud<'a * 'b> = 
            cloud { 
                let! result = 
                        Cloud.Parallel<obj> [| cloud { let! value = left in return value :> obj }; 
                                                cloud { let! value = right in return value :> obj } |]
                return (result.[0] :?> 'a, result.[1] :?> 'b) 
            }

        /// <summary>
        ///  Combines two cloud computations into one that executes them in parallel and returns the
        ///  result of the first computation that completes and cancels the other.
        /// </summary>
        /// <param name="left">The first cloud computation.</param>
        /// <param name="right">The second cloud computation.</param>
        [<Cloud>]
        [<NoTraceInfo>]
        let (<|>) (left : Cloud<'a>) (right : Cloud<'a>) : Cloud<'a> =
            cloud {
                let! result = 
                    Cloud.Choice [| cloud { let! value = left  in return Some (value) }
                                    cloud { let! value = right in return Some (value) }  |]

                return result.Value
            }

        /// <summary>
        /// Combines two cloud computations into one that executes them sequentially.
        /// </summary>
        /// <param name="left">The first cloud computation.</param>
        /// <param name="right">The second cloud computation.</param>
        [<Cloud>]
        [<NoTraceInfo>]
        let (<.>) first second = cloud { let! v1 = first in let! v2 = second in return (v1, v2) }
