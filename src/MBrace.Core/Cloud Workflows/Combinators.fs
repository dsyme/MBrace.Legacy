namespace Nessos.MBrace

    open Nessos.MBrace.CloudExpr

    /// Primitive combinators for cloud workflows
    type Cloud =

        /// <summary>
        ///     Raise an exception inside the monad.
        /// </summary>
        /// <param name="exn">exception to be raised.</param>
        static member Raise(exn : exn) : Cloud<'T> =
            Cloud.OfAsync <| Async.FromContinuations(fun (_,ec,_) -> ec exn)

        /// <summary>
        ///     Wraps a cloud computation in a computation that will return the
        ///     same result but will also write trace information in the user logs.
        /// </summary>
        /// <param name="cloudComputation">The computation to be traced.</param>
        static member Trace (cloudComputation : Cloud<'T>) : Cloud<'T> = 
            CloudExpr.wrap <| TraceExpr (CloudExpr.unwrap cloudComputation)

        /// Writes a string to the user logs.
        static member Log (msg : string) : Cloud<unit> = 
            CloudExpr.wrap <| LogExpr msg

        /// Writes a string to the user logs using the specified format.
        static member Logf (fmt : Printf.StringFormat<_, Cloud<unit>>) =
            Printf.ksprintf (LogExpr >> CloudExpr.wrap) fmt

        /// <summary>Converts an asynchronous computation to a cloud computation.</summary>
        /// <param name="asyncComputation">The computation to be converted.</param>
        static member OfAsync<'T>(asyncComputation : Async<'T>) : Cloud<'T> =
            let asyncContainer = 
                {
                    new IAsyncContainer with
                        member self.Unpack<'R> (c : IAsyncConsumer<'R>) =
                            c.Invoke<'T>(asyncComputation) 
                }

            CloudExpr.wrap <| OfAsyncExpr asyncContainer
                  
        /// <summary>
        ///     Converts a cloud computation to a computation that will 
        ///     be executed locally (on the same node).
        /// </summary>
        /// <param name="cloudComputation">The computation to be converted.</param>  
        static member ToLocal<'T>(cloudComputation : Cloud<'T>) : Cloud<'T> =
            CloudExpr.wrap <| LocalExpr (CloudExpr.unwrap cloudComputation)

        /// Returns the number of worker nodes in the current runtime.
        /// This operator may create distribution.
        static member GetWorkerCount() : Cloud<int> =
            CloudExpr.wrap GetWorkerCountExpr

        /// Returns the ProcessId of the current process.
        static member GetProcessId() : Cloud<ProcessId> =
            CloudExpr.wrap GetProcessIdExpr

        /// Returns the taskId of the current executing context.
        static member GetTaskId() : Cloud<string> =
            CloudExpr.wrap GetTaskIdExpr
    
        /// <summary>
        ///     Returns a cloud computation that will execute the given computations
        ///     possibly in parallel and returns the array of their results.
        ///     This operator may create distribution.
        ///     Exceptions raised by children carry cancellation semantics.
        /// </summary>
        /// <param name="computations">The computations to be executed in parallel.</param>
        static member Parallel<'T>(computations : seq<Cloud<'T>>) : Cloud<'T []> =
            let computations = Seq.toArray computations
            CloudExpr.wrap <| ParallelExpr (computations |> Array.map CloudExpr.unwrap, typeof<'T>)

        /// <summary>
        ///     Returns a cloud computation that will execute the given computations
        ///     possibly in parallel and will return when any of the supplied computations
        ///     have returned a successful value or if all of them fail to succeed. 
        ///     If a computation succeeds the rest of them are canceled.
        ///     The success of a computation is encoded as an option type.
        ///     This operator may create distribution.
        /// </summary>
        /// <param name="computations">Computations to be executed nondeterministically.</param>
        static member Choice<'T>(computations : seq<Cloud<'T option>>) : Cloud<'T option> =
            let computations = Seq.toArray computations
            CloudExpr.wrap <| ChoiceExpr (computations |> Array.map CloudExpr.unwrap, typeof<'T option>)


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
            
        /// <summary>
        ///     Creates a cloud computation that executes a specified computation.
        ///     If this computation completes successfully, then this method returns Choice1Of2 
        ///     with the returned value. If this computation raises an exception before it
        ///     completes then return Choice2Of2 with the raised exception.
        /// </summary>
        /// <param name="computation">The computation input.</param>
        /// <returns>A cloud computation that returns a Choice of type 'T or an exception.</returns>
        [<Cloud; NoTraceInfo>]
        static member Catch(computation : Cloud<'T>) : Cloud<Choice<'T, exn>> =
            cloud {
                try let! result = computation in return Choice1Of2 result
                with ex -> return Choice2Of2 ex
            }
            
        /// <summary>Creates a cloud computation that will sleep for the given time.</summary>
        /// <param name="millisecondsDueTime">The number of milliseconds to sleep.</param>
        /// <returns>A cloud computation that will sleep for the given time.</returns>
        [<Cloud; NoTraceInfo>]
        static member Sleep(millisecondsDueTime : int) =
            Cloud.OfAsync <| Async.Sleep(millisecondsDueTime)



    /// [omit]
    /// A module containing some useful operators for cloud computations.

    [<AutoOpen>]
    module CloudOperators =

        [<Cloud>]
        /// Converts an Cloud computation to a computation that will
        /// be executed locally.
        let local (computation : Cloud<'T>) : Cloud<'T> =
            Cloud.ToLocal computation

        /// <summary>
        ///     Combines two cloud computations into one that executes them in parallel.
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
        ///     Combines two cloud computations into one that executes them in parallel and returns the
        ///     result of the first computation that completes and cancels the other.
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
        ///     Combines two cloud computations into one that executes them sequentially.
        /// </summary>
        /// <param name="left">The first cloud computation.</param>
        /// <param name="right">The second cloud computation.</param>
        [<Cloud>]
        [<NoTraceInfo>]
        let (<.>) first second = cloud { let! v1 = first in let! v2 = second in return (v1, v2) }


        /// catch exceptions of given type ; used by the store primitives
        let internal mkTry<'Exc, 'T when 'Exc :> exn > (expr : Cloud<'T>) : Cloud<'T option> =
            cloud { 
                try 
                    let! r = expr in return Some r
                with 
                | :? 'Exc -> return None
                | ex -> return! Cloud.Raise ex
            }