namespace Nessos.MBrace.Client

open Microsoft.FSharp.Quotations

open Nessos.MBrace
open Nessos.MBrace.Core
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Logging

[<AutoOpen>]
module ClientExtensions =
    type MBrace =
        static member internal Compile (expr : Expr<Cloud<'R>>, ?name) = CloudComputation<_>(expr, ?name = name)
//        static member internal Compile (f : Expr<'I -> Cloud<'R>>, ?name) = CloudComputation<_>.Compile(f, ?name = name)
//        static member internal Compile (f : Expr<'I1 -> 'I2 -> Cloud<'R>>, ?name) = CloudComputation<_>.Compile(f, ?name = name)
//        static member internal Compile (f : Expr<'I1 -> 'I2 -> 'I3 -> Cloud<'R>>, ?name) = CloudComputation<_>.Compile(f, ?name = name)

        static member internal RunRemoteTask (runtime: MBraceRuntime) (expr : Expr<Cloud<'T>>) =
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
            // force vagrant compilation if dependencies require it ;
            // this is since the local interpreter uses FsPickler internally
            // for deep object cloning
            MBraceSettings.Vagrant.ComputeObjectDependencies(computation, permitCompilation = true) |> ignore

            let processId = 0

            let logger =
                if defaultArg showLogs false then
                    let console = Logger.createConsoleLogger()
                    new InMemoryCloudProcessLogger(console, processId) :> ICloudLogger
                else
                    new NullCloudProcessLogger() :> ICloudLogger

            return! 
                Interpreter.evaluateLocalWrapped 
                    MBraceSettings.DefaultPrimitiveConfiguration Serialization.DeepClone 
                    logger processId computation
        }

        /// Runs the given computation locally without the need of a runtime.
        static member RunLocal (computation : Cloud<'T>, ?showLogs) : 'T = 
            MBrace.RunLocalAsync(computation, ?showLogs = showLogs) |> Async.RunSynchronously 