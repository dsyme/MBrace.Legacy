namespace Nessos.MBrace.Client
    
    open System.Threading.Tasks

    open Microsoft.FSharp.Quotations

    open Nessos.MBrace
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Logging
    open Nessos.MBrace.Runtime.Interpreter
    open Nessos.MBrace.Client.Reporting

    // type abbreviations

    /// Provides a handle and administration API for remote MBrace nodes.
    type Node = Nessos.MBrace.Client.MBraceNode

    /// Provides a handle and administration API for a running MBrace cluster.
    type MBrace = Nessos.MBrace.Client.MBraceRuntime

    /// Cloud Process Identifier
    type ProcessId = Nessos.MBrace.CloudExpr.ProcessId

    /// Store configuration descriptor (Implementation/Connection string)
    type StoreDefinition = Nessos.MBrace.Store.StoreDefinition

    /// Cloud Computation + metadata
    type CloudComputation<'T> = Nessos.MBrace.Runtime.Compiler.CloudComputation<'T>


    /// MBrace Client extension methods

    [<AutoOpen>]
    module ClientExtensions =

        /// MBrace static method API
        type MBrace =
            
            /// <summary>
            ///     Compiles a cloud workflow to a cloud computation.
            /// </summary>
            /// <param name="expr">Quoted cloud computation.</param>
            /// <param name="name">Assigned name to cloud computation.</param>
            static member Compile (expr : Expr<Cloud<'T>>, ?name) : CloudComputation<'T> = 
                CloudComputation.Compile(expr, ?name = name)

            /// <summary>
            ///     Compiles a cloud workflow to a cloud computation.
            /// </summary>
            /// <param name="expr">Cloud computation.</param>
            /// <param name="name">Assigned name to cloud computation.</param>        
            [<CompilerMessage("Cloud blocks should be wrapped in quotation literals for better debug support.", 44)>]
            static member Compile (block : Cloud<'T>, ?name) : CloudComputation<'T> = 
                CloudComputation.Compile(block, ?name = name)

            /// <summary>
            ///     Runs a computation at given runtime.
            /// </summary>
            /// <param name="runtime">Runtime to execute the computation.</param>
            /// <param name="expr">Quoted cloud computation to be executed.</param>
            static member RunRemote (runtime: MBraceRuntime) (expr : Expr<Cloud<'T>>) : 'T = runtime.Run expr

            /// <summary>
            ///   Creates a new process at the given runtime.  
            /// </summary>
            /// <param name="runtime">Runtime to execute the computation.</param>
            /// <param name="expr">Cloud computation to be executed.</param>
            static member CreateProcess (runtime : MBraceRuntime) (expr : Expr<Cloud<'T>>) : Process<'T> = runtime.CreateProcess expr

            /// <summary>
            ///     Asynchronously executes a cloud workflow inside the local process with thread-parallelism semantics.
            /// </summary>
            /// <param name="computation">Cloud computation to be executed.</param>
            /// <param name="showLogs">Print user logs to StdOut.</param>
            static member RunLocalAsync (computation : CloudComputation<'T>, ?showLogs) : Async<'T> = async {
                let processId = 0

                let logger =
                    if defaultArg showLogs false then
                        let console = Logger.createConsoleLogger()
                        new InMemoryCloudProcessLogger(console, processId) :> ICloudLogger
                    else
                        new NullCloudProcessLogger() :> ICloudLogger

                let storeInfo = StoreRegistry.DefaultStoreInfo

                return! Interpreter.evaluateLocalWrapped storeInfo logger processId computation.Value
            }

            /// <summary>
            ///     Asynchronously executes a cloud workflow inside the local process with thread-parallelism semantics.
            /// </summary>
            /// <param name="computation">Cloud computation to be executed.</param>
            /// <param name="showLogs">Print user logs to StdOut.</param>
            static member RunLocalAsync (computation : Cloud<'T>, ?showLogs) : Async<'T> =
                let cc = CloudComputation.Compile computation in
                MBrace.RunLocalAsync(cc, ?showLogs = showLogs)

            /// <summary>
            ///     Asynchronously executes a cloud workflow inside the local process with thread-parallelism semantics.
            /// </summary>
            /// <param name="computation">Cloud computation to be executed.</param>
            /// <param name="showLogs">Print user logs to StdOut.</param>
            static member RunLocalAsync (expr : Expr<Cloud<'T>>, ?showLogs) : Async<'T> =
                let cc = CloudComputation.Compile expr in
                MBrace.RunLocalAsync(cc, ?showLogs = showLogs)

            /// <summary>
            ///     Executes a cloud workflow inside the local process with thread-parallelism semantics.
            /// </summary>
            /// <param name="computation">Cloud computation to be executed.</param>
            /// <param name="showLogs">Print user logs to StdOut.</param>
            static member RunLocal(computation : CloudComputation<'T>, ?showLogs) : 'T =
                MBrace.RunLocalAsync(computation, ?showLogs = showLogs) |> Async.RunSynchronously

            /// <summary>
            ///     Executes a cloud workflow inside the local process with thread-parallelism semantics.
            /// </summary>
            /// <param name="computation">Cloud computation to be executed.</param>
            /// <param name="showLogs">Print user logs to StdOut.</param>
            static member RunLocal(computation : Cloud<'T>, ?showLogs) : 'T =
                MBrace.RunLocalAsync(computation, ?showLogs = showLogs) |> Async.RunSynchronously

            /// <summary>
            ///     Executes a cloud workflow inside the local process with thread-parallelism semantics.
            /// </summary>
            /// <param name="computation">Cloud computation to be executed.</param>
            /// <param name="showLogs">Print user logs to StdOut.</param>
            static member RunLocal(computation : Expr<Cloud<'T>>, ?showLogs) : 'T =
                MBrace.RunLocalAsync(computation, ?showLogs = showLogs) |> Async.RunSynchronously