namespace Nessos.MBrace

    // Some extensions to the Cloud type that are not primitives but
    // don't belong to MBrace.Lib 
    [<AutoOpen>]
    module CloudExtensions = 

        type Cloud with
            /// <summary>Performs given cloud computation discarding the result.</summary>
            /// <param name="computation">The computation input.</param>
            /// <returns>A cloud expression returning unit.</returns>
            [<Cloud>]
            [<NoTraceInfo>]
            static member Ignore (computation : ICloud<'T>) : ICloud<unit> =
                cloud { let! _ = computation in return () }

            /// <summary>Disposes a cloud resource.</summary>
            /// <param name="resource">The resource for disposal.</param>
            /// <returns>A cloud expression returning unit.</returns>
            [<Cloud; NoTraceInfo>]
            static member Dispose<'T when 'T :> ICloudDisposable>(resource : 'T) : ICloud<unit> =
                Cloud.OfAsync (resource.Dispose())

        [<AutoOpen>]
        module CloudOperators =

            [<Cloud>]
            /// Converts an ICloud computation to a computation that will
            /// be executed locally.
            let local (computation : ICloud<'T>) : ICloud<'T> =
                Cloud.ToLocal computation

            [<Cloud>]
            [<NoTraceInfo>]
            let (<||>) (left : ICloud<'a>) (right : ICloud<'b>) : ICloud<'a * 'b> = 
                cloud { 
                    let! result = 
                            Cloud.Parallel<obj> [| cloud { let! value = left in return value :> obj }; 
                                                    cloud { let! value = right in return value :> obj } |]
                    return (result.[0] :?> 'a, result.[1] :?> 'b) 
                }

            [<Cloud>]
            [<NoTraceInfo>]
            let (<|>) (left : ICloud<'a>) (right : ICloud<'a>) : ICloud<'a> =
                cloud {
                    let! result = 
                        Cloud.Choice [| cloud { let! value = left  in return Some (value) }
                                        cloud { let! value = right in return Some (value) }  |]

                    return result.Value
                }

            [<Cloud>]
            [<NoTraceInfo>]
            let (<.>) first second = cloud { let! v1 = first in let! v2 = second in return (v1, v2) }
