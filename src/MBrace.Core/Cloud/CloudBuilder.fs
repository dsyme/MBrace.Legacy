namespace Nessos.MBrace
    
    open System
    open System.IO
    open System.Runtime.Serialization

    [<AutoOpen>]
    module CloudModule = 

        // Wrap/UnWrap functions
        let inline internal wrapCloudExpr (cloudExpr : CloudExpr) = new ICloud<'T>(cloudExpr)
        let inline internal unWrapCloudExpr (cloudValue : ICloud<'T>) : CloudExpr = cloudValue.CloudExpr

        type Cloud =

            static member Trace (cloudComputation : ICloud<'T>) : ICloud<'T> = 
                wrapCloudExpr <| TraceExpr (unWrapCloudExpr cloudComputation)

            static member Log (msg : string) : ICloud<unit> = 
                wrapCloudExpr <| LogExpr msg

            static member Logf (fmt : Printf.StringFormat<_, ICloud<unit>>) =
                Printf.ksprintf (LogExpr >> wrapCloudExpr) fmt

            static member OfAsync<'T>(asyncComputation : Async<'T>) : ICloud<'T> =
                let cloudAsync = 
                    { new ICloudAsync with
                        member self.UnPack (polyMorpInvoker : IPolyMorphicMethodAsync) =
                            polyMorpInvoker.Invoke<'T>(asyncComputation) }
                wrapCloudExpr <| OfAsyncExpr cloudAsync
                    
            static member ToLocal<'T>(cloudComputation : ICloud<'T>) : ICloud<'T> =
                wrapCloudExpr <| LocalExpr (unWrapCloudExpr cloudComputation)

            static member GetWorkerCount() : ICloud<int> =
                wrapCloudExpr GetWorkerCountExpr

            static member GetProcessId() : ICloud<ProcessId> =
                wrapCloudExpr GetProcessIdExpr

            static member GetTaskId() : ICloud<string> =
                wrapCloudExpr GetTaskIdExpr
    
            static member Parallel<'T>(computations : ICloud<'T> []) : ICloud<'T []> =
                wrapCloudExpr <| ParallelExpr (computations |> Array.map unWrapCloudExpr, typeof<'T>)

            static member Choice<'T>(computations : ICloud<'T option> []) : ICloud<'T option> =
                wrapCloudExpr <| ChoiceExpr (computations |> Array.map unWrapCloudExpr, typeof<'T option>)

        // The Monadic Builder - Computation Expressions
        and CloudBuilder() =

            member self.Return (value : 'T) : ICloud<'T> = wrapCloudExpr <| ReturnExpr (value, typeof<'T>)

            member self.ReturnFrom (computation : ICloud<'T>) : ICloud<'T> = computation

            member self.Bind(computation : ICloud<'T>, bindF : ('T -> ICloud<'U>)) : ICloud<'U> = 
                wrapCloudExpr <| BindExpr (unWrapCloudExpr computation, (fun value -> unWrapCloudExpr <| bindF (value :?> 'T)), bindF :> obj)

            member self.Delay (f : unit -> ICloud<'T>) : ICloud<'T> = 
                wrapCloudExpr <| DelayExpr ((fun () -> unWrapCloudExpr <| f () ), f)

            member self.Zero() : ICloud<unit> = wrapCloudExpr <| ReturnExpr ((), typeof<unit>)

            member self.TryWith (computation : ICloud<'T>, exceptionF : (exn -> ICloud<'T>)) : ICloud<'T> = 
                wrapCloudExpr <| TryWithExpr (unWrapCloudExpr computation, (fun ex -> unWrapCloudExpr <| exceptionF ex), exceptionF)

            member self.TryFinally (computation :  ICloud<'T>, compensation : (unit -> unit)) : ICloud<'T> = 
                wrapCloudExpr <| TryFinallyExpr (unWrapCloudExpr computation, compensation)

            member self.For(values : 'T [], bindF : ('T -> ICloud<unit>)) : ICloud<unit> = 
                wrapCloudExpr <| ForExpr (values |> Array.map (fun value -> value :> obj), (fun value -> unWrapCloudExpr <| bindF (value :?> 'T)), bindF)

            member self.While (guardF : (unit -> bool), body : ICloud<unit>) : ICloud<unit> = 
                wrapCloudExpr <| WhileExpr (guardF, unWrapCloudExpr body)

            member self.Combine (first : ICloud<unit>, second : ICloud<'T>) : ICloud<'T> = 
                wrapCloudExpr <| CombineExpr (unWrapCloudExpr first, unWrapCloudExpr second)

            member self.Using<'T, 'U when 'T :> ICloudDisposable>(value : 'T, bindF : 'T -> ICloud<'U>) : ICloud<'U> =
                wrapCloudExpr <| DisposableBindExpr (value :> ICloudDisposable, typeof<'T>, (fun value -> unWrapCloudExpr <| bindF (value :?> 'T)), bindF :> obj)

        let cloud = new CloudBuilder()

        let internal mkTry<'Exc, 'T when 'Exc :> exn > (expr : ICloud<'T>) : ICloud<'T option> =
            cloud { 
                try 
                    let! r = expr
                    return Some r
                with 
                | :? 'Exc -> return None
                | ex -> return raise ex
            }