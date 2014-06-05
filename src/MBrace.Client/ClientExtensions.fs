namespace Nessos.MBrace.Client

open Microsoft.FSharp.Quotations

open Nessos.MBrace
open Nessos.MBrace.Core
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Logging

[<AutoOpen>]
module ClientExtensions =
    type MBrace =
        static member Compile (expr : Expr<Cloud<'R>>, ?name) = CloudComputation.Compile(expr, ?name = name)
        
        [<CompilerMessage("Cloud blocks should be wrapped in quotation literals for better debug support.", 44)>]
        static member Compile (block : Cloud<'R>, ?name) = CloudComputation.Compile(block, ?name = name)

        static member RunRemoteAsTask (runtime: MBraceRuntime) (expr : Expr<Cloud<'T>>) =
            let computation = MBrace.Compile expr
            Async.StartAsTask (runtime.RunAsync computation)

        /// Runs a computation at the given runtime.
        static member RunRemote (runtime: MBraceRuntime) (expr : Expr<Cloud<'T>>) : 'T =
            runtime.Run expr

        /// Creates a new process at the given runtime.
        static member CreateProcess (runtime : MBraceRuntime) (expr : Expr<Cloud<'T>>) : Process<'T> =
            runtime.CreateProcess expr

        /// Runs the given computation locally without the need of a runtime.
        static member RunLocalAsync (computation : Cloud<'T>, ?showLogs) : Async<'T> = async {
            // force dependency compilation
            let cc = CloudComputation.Compile(computation)

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
                    logger processId cc.Value
        }

        /// Runs the given computation locally without the need of a runtime.
        static member RunLocal (computation : Cloud<'T>, ?showLogs) : 'T = 
            MBrace.RunLocalAsync(computation, ?showLogs = showLogs) |> Async.RunSynchronously 

        static member RunLocal (expr : Expr<Cloud<'T>>, ?showLogs) : 'T =
            let computation = MBrace.Compile expr
            MBrace.RunLocal(computation.Value, ?showLogs = showLogs)