namespace Nessos.MBrace.Client
    
    open System.Threading.Tasks

    open Microsoft.FSharp.Quotations

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Logging
    open Nessos.MBrace.Client.Reporting

    // type abbreviations

    type Node = Nessos.MBrace.Client.MBraceNode
    type MBrace = Nessos.MBrace.Client.MBraceRuntime
    type CloudComputation = Nessos.MBrace.Core.CloudComputation
    type CloudComputation<'T> = Nessos.MBrace.Core.CloudComputation<'T>

    [<AutoOpen>]
    module ClientExtensions =

        type MBrace =
            static member Compile (expr : Expr<Cloud<'T>>, ?name) : CloudComputation<'T> = 
                CloudComputation.Compile(expr, ?name = name)
        
            [<CompilerMessage("Cloud blocks should be wrapped in quotation literals for better debug support.", 44)>]
            static member Compile (block : Cloud<'T>, ?name) : CloudComputation<'T> = 
                CloudComputation.Compile(block, ?name = name)

            static member RunRemoteAsTask (runtime: MBraceRuntime) (expr : Expr<Cloud<'T>>) : Task<'T> =
                let computation = MBrace.Compile expr
                Async.StartAsTask (runtime.RunAsync computation)

            /// Runs a computation at the given runtime.
            static member RunRemote (runtime: MBraceRuntime) (expr : Expr<Cloud<'T>>) : 'T = runtime.Run expr

            /// Creates a new process at the given runtime.
            static member CreateProcess (runtime : MBraceRuntime) (expr : Expr<Cloud<'T>>) : Process<'T> = runtime.CreateProcess expr

            /// Runs the given computation locally without the need of a runtime.
            static member RunLocalAsync (computation : CloudComputation<'T>, ?showLogs) : Async<'T> = async {
                let processId = 0

                let logger =
                    if defaultArg showLogs false then
                        let console = Logger.createConsoleLogger()
                        new InMemoryCloudProcessLogger(console, processId) :> ICloudLogger
                    else
                        new NullCloudProcessLogger() :> ICloudLogger

                return! 
                    Interpreter.evaluateLocalWrapped 
                        MBraceSettings.DefaultStoreInfo.Primitives Serialization.DeepClone 
                        logger processId computation.Value
            }

            static member RunLocalAsync (computation : Cloud<'T>, ?showLogs) : Async<'T> =
                let cc = CloudComputation.Compile computation in
                MBrace.RunLocalAsync(cc, ?showLogs = showLogs)

            static member RunLocalAsync (expr : Expr<Cloud<'T>>, ?showLogs) : Async<'T> =
                let cc = CloudComputation.Compile expr in
                MBrace.RunLocalAsync(cc, ?showLogs = showLogs)

            static member RunLocal(computation : CloudComputation<'T>, ?showLogs) : 'T =
                MBrace.RunLocalAsync(computation, ?showLogs = showLogs) |> Async.RunSynchronously

            static member RunLocal(computation : Cloud<'T>, ?showLogs) : 'T =
                MBrace.RunLocalAsync(computation, ?showLogs = showLogs) |> Async.RunSynchronously

            static member RunLocal(computation : Expr<Cloud<'T>>, ?showLogs) : 'T =
                MBrace.RunLocalAsync(computation, ?showLogs = showLogs) |> Async.RunSynchronously