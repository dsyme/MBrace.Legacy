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
    type Cloud internal (cloudExpr : CloudExpr) =
        abstract Type : Type
        member internal __.CloudExpr = cloudExpr

    [<Sealed>]
    type Cloud<'T> internal (cloudExpr : CloudExpr) =
        inherit Cloud(cloudExpr)
        override __.Type = typeof<'T>

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    [<RequireQualifiedAccess>]
    module Cloud =

        let inline internal wrapExpr (cloudExpr : CloudExpr) = new Cloud<'T>(cloudExpr)
        let inline internal unwrapExpr (cloudBlock : Cloud<'T>) = cloudBlock.CloudExpr

        let Trace (cloudComputation : Cloud<'T>) : Cloud<'T> = 
            wrapExpr <| TraceExpr (unwrapExpr cloudComputation)

        let Log (msg : string) : Cloud<unit> = 
            wrapExpr <| LogExpr msg

        let Logf (fmt : Printf.StringFormat<_, Cloud<unit>>) =
            Printf.ksprintf (LogExpr >> wrapExpr) fmt

        let OfAsync<'T>(asyncComputation : Async<'T>) : Cloud<'T> =
            let cloudAsync = 
                { new ICloudAsync with
                    member self.UnPack (polyMorpInvoker : IPolyMorphicMethodAsync) =
                        polyMorpInvoker.Invoke<'T>(asyncComputation) }
            wrapExpr <| OfAsyncExpr cloudAsync
                    
        let ToLocal<'T>(cloudComputation : Cloud<'T>) : Cloud<'T> =
            wrapExpr <| LocalExpr (unwrapExpr cloudComputation)

        let GetWorkerCount() : Cloud<int> =
            wrapExpr GetWorkerCountExpr

        let GetProcessId() : Cloud<ProcessId> =
            wrapExpr GetProcessIdExpr

        let GetTaskId() : Cloud<string> =
            wrapExpr GetTaskIdExpr
    
        let Parallel<'T>(computations : seq<Cloud<'T>>) : Cloud<'T []> =
            let computations = Seq.toArray computations
            wrapExpr <| ParallelExpr (computations |> Array.map unwrapExpr, typeof<'T>)

        let Choice<'T>(computations : seq<Cloud<'T option>>) : Cloud<'T option> =
            let computations = Seq.toArray computations
            wrapExpr <| ChoiceExpr (computations |> Array.map unwrapExpr, typeof<'T option>)

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

        [<Obsolete("While loops in distributed computation considered harmful; consider using an accumulator pattern instead.")>]
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