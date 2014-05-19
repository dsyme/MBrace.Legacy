namespace Nessos.MBrace

    open System

    open Nessos.MBrace.Core

    type CloudAttribute = ReflectedDefinitionAttribute
    type ProcessId = Nessos.MBrace.Core.ProcessId

    [<Sealed>]
    [<System.AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Method ||| AttributeTargets.Property ||| AttributeTargets.Constructor, AllowMultiple = false)>]
    type NoTraceInfoAttribute() = 
            inherit System.Attribute()
            member self.Name = "NoTraceInfo"

    [<AbstractClass>]
    type ICloud internal (cloudExpr : CloudExpr) =
        abstract Type : Type
        member internal __.CloudExpr = cloudExpr

    [<Sealed>]
    type ICloud<'T> internal (cloudExpr : CloudExpr) =
        inherit ICloud(cloudExpr)
        override __.Type = typeof<'T>

    module internal CloudExpr =
        let inline internal wrap (cloudExpr : CloudExpr) = new ICloud<'T>(cloudExpr)
        let inline internal unwrap (cloudBlock : ICloud<'T>) = cloudBlock.CloudExpr

    type Cloud =

        static member Trace (cloudComputation : ICloud<'T>) : ICloud<'T> = 
            CloudExpr.wrap <| TraceExpr (CloudExpr.unwrap cloudComputation)

        static member Log (msg : string) : ICloud<unit> = 
            CloudExpr.wrap <| LogExpr msg

        static member Logf (fmt : Printf.StringFormat<_, ICloud<unit>>) =
            Printf.ksprintf (LogExpr >> CloudExpr.wrap) fmt

        static member OfAsync<'T>(asyncComputation : Async<'T>) : ICloud<'T> =
            let cloudAsync = 
                { new ICloudAsync with
                    member self.UnPack (polyMorpInvoker : IPolyMorphicMethodAsync) =
                        polyMorpInvoker.Invoke<'T>(asyncComputation) }
            CloudExpr.wrap <| OfAsyncExpr cloudAsync
                    
        static member ToLocal<'T>(cloudComputation : ICloud<'T>) : ICloud<'T> =
            CloudExpr.wrap <| LocalExpr (CloudExpr.unwrap cloudComputation)

        static member GetWorkerCount() : ICloud<int> =
            CloudExpr.wrap GetWorkerCountExpr

        static member GetProcessId() : ICloud<ProcessId> =
            CloudExpr.wrap GetProcessIdExpr

        static member GetTaskId() : ICloud<string> =
            CloudExpr.wrap GetTaskIdExpr
    
        static member Parallel<'T>(computations : ICloud<'T> []) : ICloud<'T []> =
            CloudExpr.wrap <| ParallelExpr (computations |> Array.map CloudExpr.unwrap, typeof<'T>)

        static member Choice<'T>(computations : ICloud<'T option> []) : ICloud<'T option> =
            CloudExpr.wrap <| ChoiceExpr (computations |> Array.map CloudExpr.unwrap, typeof<'T option>)

    // The Monadic Builder - Computation Expressions
    type CloudBuilder() =

        member self.Return (value : 'T) : ICloud<'T> = CloudExpr.wrap <| ReturnExpr (value, typeof<'T>)

        member self.ReturnFrom (computation : ICloud<'T>) : ICloud<'T> = computation

        member self.Bind(computation : ICloud<'T>, bindF : ('T -> ICloud<'U>)) : ICloud<'U> = 
            CloudExpr.wrap <| BindExpr (CloudExpr.unwrap computation, (fun value -> CloudExpr.unwrap <| bindF (value :?> 'T)), bindF :> obj)

        member self.Delay (f : unit -> ICloud<'T>) : ICloud<'T> = 
            CloudExpr.wrap <| DelayExpr ((fun () -> CloudExpr.unwrap <| f () ), f)

        member self.Zero() : ICloud<unit> = CloudExpr.wrap <| ReturnExpr ((), typeof<unit>)

        member self.TryWith (computation : ICloud<'T>, exceptionF : (exn -> ICloud<'T>)) : ICloud<'T> = 
            CloudExpr.wrap <| TryWithExpr (CloudExpr.unwrap computation, (fun ex -> CloudExpr.unwrap <| exceptionF ex), exceptionF)

        member self.TryFinally (computation :  ICloud<'T>, compensation : (unit -> unit)) : ICloud<'T> = 
            CloudExpr.wrap <| TryFinallyExpr (CloudExpr.unwrap computation, compensation)

        member self.For(values : 'T [], bindF : ('T -> ICloud<unit>)) : ICloud<unit> = 
            CloudExpr.wrap <| ForExpr (values |> Array.map (fun value -> value :> obj), (fun value -> CloudExpr.unwrap <| bindF (value :?> 'T)), bindF)

        member self.While (guardF : (unit -> bool), body : ICloud<unit>) : ICloud<unit> = 
            CloudExpr.wrap <| WhileExpr (guardF, CloudExpr.unwrap body)

        member self.Combine (first : ICloud<unit>, second : ICloud<'T>) : ICloud<'T> = 
            CloudExpr.wrap <| CombineExpr (CloudExpr.unwrap first, CloudExpr.unwrap second)

        member self.Using<'T, 'U when 'T :> ICloudDisposable>(value : 'T, bindF : 'T -> ICloud<'U>) : ICloud<'U> =
            CloudExpr.wrap <| DisposableBindExpr (value :> ICloudDisposable, typeof<'T>, (fun value -> CloudExpr.unwrap <| bindF (value :?> 'T)), bindF :> obj)


    [<AutoOpen>]
    module CloudModule =
        
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