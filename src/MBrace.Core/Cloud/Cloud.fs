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

    [<Sealed>]
    type Cloud<'T> internal (cloudExpr : CloudExpr) =
        member __.Type = typeof<'T>
        member __.CloudExpr = cloudExpr

    type Cloud =

        static member inline internal wrapExpr (cloudExpr : CloudExpr) = new Cloud<'T>(cloudExpr)
        static member inline internal unwrapExpr (cloudBlock : Cloud<'T>) = cloudBlock.CloudExpr

        static member Trace (cloudComputation : Cloud<'T>) : Cloud<'T> = 
            Cloud.wrapExpr <| TraceExpr (Cloud.unwrapExpr cloudComputation)

        static member Log (msg : string) : Cloud<unit> = 
            Cloud.wrapExpr <| LogExpr msg

        static member Logf (fmt : Printf.StringFormat<_, Cloud<unit>>) =
            Printf.ksprintf (LogExpr >> Cloud.wrapExpr) fmt

        static member OfAsync<'T>(asyncComputation : Async<'T>) : Cloud<'T> =
            let cloudAsync = 
                { new ICloudAsync with
                    member self.UnPack (polyMorpInvoker : IPolyMorphicMethodAsync) =
                        polyMorpInvoker.Invoke<'T>(asyncComputation) }
            Cloud.wrapExpr <| OfAsyncExpr cloudAsync
                    
        static member ToLocal<'T>(cloudComputation : Cloud<'T>) : Cloud<'T> =
            Cloud.wrapExpr <| LocalExpr (Cloud.unwrapExpr cloudComputation)

        static member GetWorkerCount() : Cloud<int> =
            Cloud.wrapExpr GetWorkerCountExpr

        static member GetProcessId() : Cloud<ProcessId> =
            Cloud.wrapExpr GetProcessIdExpr

        static member GetTaskId() : Cloud<string> =
            Cloud.wrapExpr GetTaskIdExpr
    
        static member Parallel<'T>(computations : seq<Cloud<'T>>) : Cloud<'T []> =
            let computations = Seq.toArray computations
            Cloud.wrapExpr <| ParallelExpr (computations |> Array.map Cloud.unwrapExpr, typeof<'T>)

        static member Choice<'T>(computations : seq<Cloud<'T option>>) : Cloud<'T option> =
            let computations = Seq.toArray computations
            Cloud.wrapExpr <| ChoiceExpr (computations |> Array.map Cloud.unwrapExpr, typeof<'T option>)


    // The Monadic Builder - Computation Expressions
    type CloudBuilder() =

        member self.Return (value : 'T) : Cloud<'T> = Cloud.wrapExpr <| ReturnExpr (value, typeof<'T>)

        member self.ReturnFrom (computation : Cloud<'T>) : Cloud<'T> = computation

        member self.Bind(computation : Cloud<'T>, bindF : ('T -> Cloud<'U>)) : Cloud<'U> = 
            Cloud.wrapExpr <| BindExpr (Cloud.unwrapExpr computation, (fun value -> Cloud.unwrapExpr <| bindF (value :?> 'T)), bindF :> obj)

        member self.Delay (f : unit -> Cloud<'T>) : Cloud<'T> = 
            Cloud.wrapExpr <| DelayExpr ((fun () -> Cloud.unwrapExpr <| f () ), f)

        member self.Zero() : Cloud<unit> = Cloud.wrapExpr <| ReturnExpr ((), typeof<unit>)

        member self.TryWith (computation : Cloud<'T>, exceptionF : (exn -> Cloud<'T>)) : Cloud<'T> = 
            Cloud.wrapExpr <| TryWithExpr (Cloud.unwrapExpr computation, (fun ex -> Cloud.unwrapExpr <| exceptionF ex), exceptionF)

        member self.TryFinally (computation :  Cloud<'T>, compensation : (unit -> unit)) : Cloud<'T> = 
            Cloud.wrapExpr <| TryFinallyExpr (Cloud.unwrapExpr computation, compensation)

        member self.For(values : 'T [], bindF : ('T -> Cloud<unit>)) : Cloud<unit> = 
            Cloud.wrapExpr <| ForExpr (values |> Array.map (fun value -> value :> obj), (fun value -> Cloud.unwrapExpr <| bindF (value :?> 'T)), bindF)

        member self.For(values : 'T list, bindF : ('T -> Cloud<unit>)) : Cloud<unit> = 
            self.For(List.toArray values, bindF)

        [<CompilerMessage("While loops in distributed computation not recommended; consider using an accumulator pattern instead.", 44)>]
        member self.While (guardF : (unit -> bool), body : Cloud<unit>) : Cloud<unit> = 
            Cloud.wrapExpr <| WhileExpr (guardF, Cloud.unwrapExpr body)

        member self.Combine (first : Cloud<unit>, second : Cloud<'T>) : Cloud<'T> = 
            Cloud.wrapExpr <| CombineExpr (Cloud.unwrapExpr first, Cloud.unwrapExpr second)

        member self.Using<'T, 'U when 'T :> ICloudDisposable>(value : 'T, bindF : 'T -> Cloud<'U>) : Cloud<'U> =
            Cloud.wrapExpr <| DisposableBindExpr (value :> ICloudDisposable, typeof<'T>, (fun value -> Cloud.unwrapExpr <| bindF (value :?> 'T)), bindF :> obj)


    [<AutoOpen>]
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module CloudBuilder =
        
        let cloud = new CloudBuilder()

        let internal mkTry<'Exc, 'T when 'Exc :> exn > (expr : Cloud<'T>) : Cloud<'T option> =
            cloud { 
                try 
                    let! r = expr
                    return Some r
                with 
                | :? 'Exc -> return None
                | ex -> return raise ex
            }

    // Useful extensions to Cloud type
    type Cloud with
        static member Catch(computation : Cloud<'T>) : Cloud<Choice<'T, exn>> =
            cloud {
                try
                    let! result = computation
                    return Choice1Of2 result
                with ex ->
                    return Choice2Of2 ex
            }

            