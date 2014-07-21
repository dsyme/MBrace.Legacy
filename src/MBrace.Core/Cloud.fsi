namespace Nessos.MBrace

    open System

    open Nessos.MBrace.CloudExpr

    /// Adding this attribute to a let-binding marks that
    /// the value definition contains cloud expressions.
    type CloudAttribute = ReflectedDefinitionAttribute
    
    /// The identifier of the running cloud process.
    type ProcessId = Nessos.MBrace.CloudExpr.ProcessId

    [<Sealed>]
    /// Disable tracing for the current cloud workflow.
    type NoTraceInfoAttribute =
        class
            inherit System.Attribute
            new : unit -> NoTraceInfoAttribute
        end

    [<Sealed>]
    /// Disable static check warnings being generated for current workflow.
    type NoWarnAttribute =
        class
            inherit System.Attribute
            new : unit -> NoWarnAttribute
        end

    [<Sealed>]
    /// Representation of a cloud computation, which, when run will produce a value of type 'T, or raise an exception.
    type Cloud<'T> =
        class
            internal new : cloudExpr:CloudExpr -> Cloud<'T>
            /// The type of the returned value.
            member Type : Type
            member internal CloudExpr : CloudExpr
        end
        
    /// Contains the methods (combinators) to express the primitive computations directly supported by
    /// the MBrace runtime.
    type Cloud =
        class

            static member inline internal wrapExpr   : cloudExpr:CloudExpr -> Cloud<'T>
            static member inline internal unwrapExpr : cloudValue:Cloud<'T> -> CloudExpr

            /// Returns a cloud computation that will execute the given computations
            /// possibly in parallel and will return when any of the supplied computations
            /// have returned a successful value or if all of them fail to succeed. 
            /// If a computation succeeds the rest of them are canceled.
            /// The success of a computation is encoded as an option type.
            /// This operator may create distribution.
            static member Choice : computations:seq<Cloud<'T option>> -> Cloud<'T option>
                
            /// Returns the ProcessId of the current process.
            static member GetProcessId : unit -> Cloud<ProcessId>

            /// Returns the taskId of the current executing context.
            static member GetTaskId : unit -> Cloud<string>
                
            /// Returns the number of worker nodes in the current runtime.
            /// This operator may create distribution.
            static member GetWorkerCount : unit -> Cloud<int>
                
            /// Writes a string to the user logs.
            static member Log : msg:string -> Cloud<unit>

            /// Writes a string to the user logs using the specified format.
            static member Logf : fmt:Printf.StringFormat<'T,Cloud<unit>> -> 'T

            /// <summary>Converts an asynchronous computation to a cloud computation.</summary>
            /// <param name="asyncComputation">The computation to be converted.</param>
            static member OfAsync : asyncComputation:Async<'T> -> Cloud<'T>
                
            /// <summary>Returns a cloud computation that will execute the given computations
            /// possibly in parallel and returns the array of their results.
            /// This operator may create distribution.
            /// If any exceptions are thrown all the results will be aggregated in an exception.</summary>
            /// <param name="computations">The computations to be executed in parallel.</param>  
            static member Parallel : computations:seq<Cloud<'T>> -> Cloud<'T []>

            /// <summary>Converts a cloud computation to a computation that will 
            /// be executed locally (on the same node).
            /// </summary>
            /// <param name="cloudComputation">The computation to be converted.</param>
            static member ToLocal : cloudComputation:Cloud<'T> -> Cloud<'T>
                
            /// <summary>Wraps a cloud computation in a computation that will return the
            /// same result but will also write trace information in the user logs.
            /// </summary>
            /// <param name="cloudComputation">The computation to be traced.</param>
            static member Trace : cloudComputation:Cloud<'T> -> Cloud<'T>
        end

    /// The monadic builder.
    type CloudBuilder =
        class
            new : unit -> CloudBuilder
            
            ///Implements 'let!' in cloud computations.
            member Bind : computation:Cloud<'T> * bindF:('T -> Cloud<'U>) -> Cloud<'U>
            ///Creates a cloud computation that first runs one computation and then runs another computation, returning the result of the second computation.
            member Combine : first:Cloud<unit> * second:Cloud<'T> -> Cloud<'T>
            ///Creates a cloud computation that runs a function.
            member Delay : f:(unit -> Cloud<'T>) -> Cloud<'T>
            ///Implements the 'for' expression in cloud computations.
            member For : values:'T [] * bindF:('T -> Cloud<unit>) -> Cloud<unit>
            ///Implements the 'for' expression in cloud computations.
            member For : values:'T list * bindF:('T -> Cloud<unit>) -> Cloud<unit>
            ///Implements the 'return' expression in cloud computations.
            member Return : value:'T -> Cloud<'T>
            ///Implements the 'return!' expression in cloud computations.
            member ReturnFrom : computation:Cloud<'T> -> Cloud<'T>
            ///Implements the 'try ... finally' expression in cloud computations.
            member TryFinally : computation:Cloud<'T> * compensation:(unit -> unit) -> Cloud<'T>
            ///Implements the 'try ... with' expression in cloud computations.
            member TryWith : computation:Cloud<'T> * exceptionF:(exn -> Cloud<'T>) -> Cloud<'T>
            ///Creates a cloud computation that does nothing and returns ().
            member Zero : unit -> Cloud<unit>
            ///Implements the 'use!' expression in cloud computation expressions.
            member Using<'T, 'U when 'T :> ICloudDisposable> : 'T * ('T -> Cloud<'U>) -> Cloud<'U> 

            [<CompilerMessage("While loops in distributed computation not recommended; consider using an accumulator pattern instead.", 44)>]
            ///Implements the 'while' keyword in cloud computation expressions.
            member While : guardF:(unit -> bool) * body:Cloud<unit> -> Cloud<unit>
        end

    [<AutoOpen>]
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    /// The monadic builder module.
    module CloudBuilder = begin
        /// Builds a cloud workflow. 
        val cloud : CloudBuilder 
        val internal mkTry<'Exc,'T when 'Exc :> exn> :
            expr:Cloud<'T> -> Cloud<'T option>
    end

