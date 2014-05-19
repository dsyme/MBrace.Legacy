namespace Nessos.MBrace

    open System

    open Nessos.MBrace.Core

    type CloudAttribute = ReflectedDefinitionAttribute
    type ProcessId = Nessos.MBrace.Core.ProcessId

    [<Sealed>]
    type NoTraceInfoAttribute =
        class
            inherit System.Attribute
            new : unit -> NoTraceInfoAttribute
            member Name : string
        end

    [<AbstractClass>]
    type ICloud =
        class
            internal new : cloudExpr:CloudExpr -> ICloud
            abstract member Type : Type
            member internal CloudExpr : CloudExpr
        end

    [<Sealed>]
    type ICloud<'T> =
        class
            inherit ICloud
            internal new : cloudExpr:CloudExpr -> ICloud<'T>
            override Type : Type
        end

    module internal CloudExpr = begin
        val inline internal wrap : cloudExpr:CloudExpr -> ICloud<'T>
        val inline internal unwrap : cloudValue:ICloud<'T> -> CloudExpr
    end
        
    /// Contains the methods (combinators) to express the primitive computations directly supported by
    /// the MBrace runtime.
    type Cloud =
        class
            /// Returns a cloud computation that will execute the given computations
            /// possibly in parallel and will return when any of the supplied computations
            /// have returned a successful value or if all of them fail to succeed. 
            /// If a computation succeeds the rest of them are canceled.
            /// The success of a computation is encoded as an option type.
            /// This operator may create distribution.
            static member Choice : computations:ICloud<'T option> [] -> ICloud<'T option>
                
            /// Returns the ProcessId of the current process.
            static member GetProcessId : unit -> ICloud<ProcessId>

            /// Returns the taskId of the current executing context.
            static member GetTaskId : unit -> ICloud<string>
                
            /// Returns the number of worker nodes in the current runtime.
            /// This operator may create distribution.
            static member GetWorkerCount : unit -> ICloud<int>
                
            /// Writes a string to the user logs.
            static member Log : msg:string -> ICloud<unit>

            /// Writes a string to the user logs using the specified format.
            static member Logf : fmt:Printf.StringFormat<'T,ICloud<unit>> -> 'T

            /// <summary>Converts an asynchronous computation to a cloud computation.</summary>
            /// <param name="asyncComputation">The computation to be converted.</param>
            static member OfAsync : asyncComputation:Async<'T> -> ICloud<'T>
                
            /// <summary>Returns a cloud computation that will execute the given computations
            /// possibly in parallel and returns the array of their results.
            /// This operator may create distribution.
            /// If any exceptions are thrown all the results will be aggregated in an exception.</summary>
            /// <param name="computations">The computations to be executed in parallel.</param>  
            static member Parallel : computations:ICloud<'T> [] -> ICloud<'T []>

            /// <summary>Converts a cloud computation to a computation that will 
            /// be executed locally (on the same node).
            /// </summary>
            /// <param name="cloudComputation">The computation to be converted.</param>
            static member ToLocal : cloudComputation:ICloud<'T> -> ICloud<'T>
                
            /// <summary>Wraps a cloud computation in a computation that will return the
            /// same result but will also write trace information in the user logs.
            /// </summary>
            /// <param name="cloudComputation">The computation to be traced.</param>
            static member Trace : cloudComputation:ICloud<'T> -> ICloud<'T>
        end

    /// The monadic builder.
    and CloudBuilder =
        class
            new : unit -> CloudBuilder
            member Bind : computation:ICloud<'T> * bindF:('T -> ICloud<'U>) -> ICloud<'U>
            member Combine : first:ICloud<unit> * second:ICloud<'T> -> ICloud<'T>
            member Delay : f:(unit -> ICloud<'T>) -> ICloud<'T>
            member For : values:'T [] * bindF:('T -> ICloud<unit>) -> ICloud<unit>
            member Return : value:'T -> ICloud<'T>
            member ReturnFrom : computation:ICloud<'T> -> ICloud<'T>
            member TryFinally : computation:ICloud<'T> * compensation:(unit -> unit) -> ICloud<'T>
            member TryWith : computation:ICloud<'T> * exceptionF:(exn -> ICloud<'T>) -> ICloud<'T>
            member While : guardF:(unit -> bool) * body:ICloud<unit> -> ICloud<unit>
            member Zero : unit -> ICloud<unit>
            member Using<'T, 'U when 'T :> ICloudDisposable> : 'T * ('T -> ICloud<'U>) -> ICloud<'U> 
        end


    [<AutoOpen>]
    /// The module containing all the primitive operations directly supported by
    /// the MBrace runtime.
    module CloudModule = begin

        val cloud : CloudBuilder
        val internal mkTry<'Exc,'T when 'Exc :> exn> :
            expr:ICloud<'T> -> ICloud<'T option>
    end

