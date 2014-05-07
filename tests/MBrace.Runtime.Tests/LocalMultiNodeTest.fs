namespace Nessos.MBrace.Runtime.Tests

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Core
    open Nessos.MBrace.Client

    open System
    open System.IO
    open System.Collections.Generic
    open Microsoft.FSharp.Quotations
    open NUnit.Framework
    open System.Net
    open System.Diagnostics

    open Nessos.Thespian
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Remote

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Store
    open Nessos.MBrace.Client
    open Nessos.MBrace.Client.ClientExtensions

    [<TestFixture>]
    type ``Cloud scenaria`` =
        inherit TestBed

        val runtimeInfo: MBraceRuntime option ref

        new() = {
            inherit TestBed()

            runtimeInfo = ref None
        }

        member private test.GetRuntime () =
            match test.runtimeInfo.Value with
            | None -> test.Init(); test.GetRuntime()
            | Some runtime -> if runtime.Active then runtime else test.Init(); test.GetRuntime()
            
        override test.Name = "MultiNode"
        override test.Runtime = test.GetRuntime () 

        override test.InitRuntime() =

//            do Defaults.setCachingDirectory()
//
////            MBrace.ClientInit(
////                MBracedPath = Defaults.mbracedExe,
////                ILoggerFactory = (Logger.createConsoleLogger >> Logger.wrapAsync),
////                IStoreFactory = (fun () -> new FileSystemStore("teststore", System.Configuration.ConfigurationManager.AppSettings.["StorePath"]) :> IStore),
////                ClientSideExprCheck = false,
////                CompressSerialization = false,
////                DefaultSerializer = Defaults.defaultSerializer
////            )
//            MBraceSettings.MBracedExecutablePath <- Defaults.mbracedExe
//            MBraceSettings.StoreProvider <- LocalFS
//            MBraceSettings.ClientSideExpressionCheck <- false

            IoC.Register<ILogger>(Logger.createConsoleLogger, behaviour = Override)

            match test.runtimeInfo.Value with
            | Some runtime -> runtime.Kill(); ConnectionPool.TcpConnectionPool.Fini()
            | _ -> ConnectionPool.TcpConnectionPool.Init()

            let runtime = MBraceRuntime.InitLocal(3, debug = true)

            test.runtimeInfo := Some runtime
    
        override test.FiniRuntime() =
            let runtime = test.GetRuntime()
            runtime.Shutdown()
            ()

        override test.ExecuteExpression<'T>(expr: Expr<ICloud<'T>>): 'T =
            let runtime = test.GetRuntime()

            MBrace.RunRemote runtime expr


        override test.SerializationTest () =
            shouldFailwith<MBraceException> (fun () -> <@ cloud { return new System.Net.HttpListener() } @> |> test.ExecuteExpression |> ignore)


