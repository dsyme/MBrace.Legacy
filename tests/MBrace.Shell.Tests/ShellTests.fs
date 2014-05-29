namespace MBrace.Shell.Tests

    open System
    open System.Reflection
    open System.IO

    open NUnit.Framework

    open Microsoft.FSharp.Compiler.Interactive.Shell
    open Microsoft.FSharp.Compiler.SimpleSourceCodeServices

    [<TestFixture>]
    module FsiTests =

        // by default, NUnit copies test assemblies to a temp directory
        // use Directory.GetCurrentDirectory to gain access to the original build directory
        let private buildDirectory = Directory.GetCurrentDirectory()
        let getPathLiteral (path : string) =
            let fullPath =
                if Path.IsPathRooted path then path
                else Path.Combine(buildDirectory, path)

            sprintf "@\"%s\"" fullPath

        type FsiEvaluationSession with
        
            member fsi.AddReferences (paths : string list) =
                let directives = 
                    paths 
                    |> Seq.map (fun p -> sprintf "#r %s" <| getPathLiteral p)
                    |> String.concat "\n"

                fsi.EvalInteraction directives

            member fsi.LoadScript (path : string) =
                let directive = sprintf "#load %s" <| getPathLiteral path
                fsi.EvalInteraction directive

            member fsi.TryEvalExpression(code : string) =
                try fsi.EvalExpression(code)
                with _ -> None

        let shouldEqual (expected : 'T) (result : FsiValue option) =
            match result with
            | None -> raise <| new AssertionException(sprintf "expected %A, got exception." expected)
            | Some value ->
                if not <| typeof<'T>.IsAssignableFrom value.ReflectionType then
                    raise <| new AssertionException(sprintf "expected type %O, got %O." typeof<'T> value.ReflectionType)

                match value.ReflectionValue with
                | :? 'T as result when result = expected -> ()
                | result -> raise <| new AssertionException(sprintf "expected %A, got %A." expected result)
            
        type FsiSession private () =
            static let container = ref None

            static member Start () =
                lock container (fun () ->
                    match !container with
                    | Some _ -> invalidOp "an fsi session is already running."
                    | None ->
                        let dummy = new StringReader("")
                        let fsiConfig = FsiEvaluationSession.GetDefaultConfiguration()
                        let fsi = FsiEvaluationSession.Create(fsiConfig, [| "fsi.exe" ; "--noninteractive" |], dummy, Console.Out, Console.Error)
                        container := Some fsi; fsi)

            static member Stop () =
                lock container (fun () ->
                    match !container with
                    | None -> invalidOp "No fsi sessions are running"
                    | Some fsi ->
                        // need a 'stop' operation here
                        container := None)


            static member Value =
                match !container with
                | None -> invalidOp "No fsi session is running."
                | Some fsi -> fsi

        let eval (expr : string) = FsiSession.Value.TryEvalExpression expr


        [<TestFixtureSetUp>]
        let initFsiSession () =
            
            let fsi = FsiSession.Start()

            fsi.EvalInteraction <| sprintf """#cd %s """ (getPathLiteral "")

            fsi.AddReferences 
                [
                    "Thespian.dll"
                    "Vagrant.dll"
                    "MBrace.Core.dll"
                    "MBrace.Lib.dll"
                    "MBrace.Runtime.Base.dll"
                    "MBrace.Client.dll"
                ]

            fsi.EvalInteraction "open Nessos.MBrace"
            fsi.EvalInteraction "open Nessos.MBrace.Client"
            fsi.EvalInteraction <| """MBraceSettings.MBracedExecutablePath <- "mbraced.exe" """
            fsi.EvalInteraction <| "MBraceNode.SpawnMultiple(1)" 

        [<TestFixtureTearDown>]
        let stopFsiSession () =
            FsiSession.Value.Interrupt()
            //FsiSession.Value.EvalInteraction "client.Kill()"
            FsiSession.Stop()

        [<Test>]
        let ``01. Simple execution`` () =

            let fsi = FsiSession.Value

            "42" |> fsi.TryEvalExpression |> shouldEqual 42