namespace Nessos.MBrace

    // The Monadic Builder - Computation Expressions

    open Nessos.MBrace.CloudExpr

    [<AutoOpen>]
    module private BuilderUtils =

        let inline lift (f: 'T -> Cloud<'U>) (x : 'S) =
            let expr = f (x :> obj :?> 'T) in expr.CloudExpr

    type CloudBuilder() =

        /// Implements the 'return' expression in cloud computations.
        member self.Return (value : 'T) : Cloud<'T> = CloudExpr.wrap <| ReturnExpr (value, typeof<'T>)

        /// Implements the 'return!' expression in cloud computations.
        member self.ReturnFrom (computation : Cloud<'T>) : Cloud<'T> = computation

        /// Implements 'let!' in cloud computations.
        member self.Bind(computation : Cloud<'T>, bindF : ('T -> Cloud<'U>)) : Cloud<'U> = 
            CloudExpr.wrap <| BindExpr (CloudExpr.unwrap computation, lift bindF, bindF :> obj)

        /// Creates a cloud computation that runs a function.
        member self.Delay (f : unit -> Cloud<'T>) : Cloud<'T> = 
            CloudExpr.wrap <| DelayExpr (lift f, f)

        /// Creates a cloud computation that does nothing and returns ().
        member self.Zero() : Cloud<unit> = CloudExpr.wrap <| ReturnExpr ((), typeof<unit>)

        /// Implements the 'try ... with' expression in cloud computations.
        member self.TryWith (computation : Cloud<'T>, exceptionF : (exn -> Cloud<'T>)) : Cloud<'T> = 
            CloudExpr.wrap <| TryWithExpr (CloudExpr.unwrap computation, lift exceptionF, exceptionF)

        /// Implements the 'try ... finally' expression in cloud computations.
        member self.TryFinally (computation :  Cloud<'T>, compensation : (unit -> unit)) : Cloud<'T> = 
            CloudExpr.wrap <| TryFinallyExpr (CloudExpr.unwrap computation, compensation)

        /// Implements the 'for' expression in cloud computations.
        member self.For(values : 'T [], bindF : ('T -> Cloud<unit>)) : Cloud<unit> = 
            CloudExpr.wrap <| ForExpr (values |> Array.map (fun value -> value :> obj), lift bindF, bindF)

        /// Implements the 'for' expression in cloud computations.
        member self.For(values : 'T list, bindF : ('T -> Cloud<unit>)) : Cloud<unit> = 
            self.For(List.toArray values, bindF)

        /// Implements the 'while' keyword in cloud computation expressions.
        [<CompilerMessage("While loops in distributed computation not recommended; consider using an accumulator pattern instead.", 44)>]
        member self.While (guardF : (unit -> bool), body : Cloud<unit>) : Cloud<unit> = 
            CloudExpr.wrap <| WhileExpr (guardF, CloudExpr.unwrap body)

        /// Implements sequential composition in cloud computation expressions.
        member self.Combine (first : Cloud<unit>, second : Cloud<'T>) : Cloud<'T> = 
            CloudExpr.wrap <| CombineExpr (CloudExpr.unwrap first, CloudExpr.unwrap second)

        /// Implements the 'use!' expression in cloud computation expressions.
        member self.Using<'T, 'U when 'T :> ICloudDisposable>(value : 'T, bindF : 'T -> Cloud<'U>) : Cloud<'U> =
            CloudExpr.wrap <| DisposableBindExpr (value :> ICloudDisposable, typeof<'T>, lift bindF, bindF :> obj)


    [<AutoOpen>]
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module CloudBuilder =
        
        /// cloud workflow builder
        let cloud = new CloudBuilder()