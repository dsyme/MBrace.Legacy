namespace Nessos.MBrace.Runtime.Tests

    open System
    open System.Reflection
    open System.IO

    open NUnit.Framework

    open Microsoft.FSharp.Compiler.Interactive.Shell
    open Microsoft.FSharp.Compiler.SimpleSourceCodeServices

    [<TestFixture; FSharpInteractiveCategory>]
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
                    "MBrace.Store.dll"
                    "MBrace.Runtime.Base.dll"
                    "MBrace.Client.dll"
                ]

            fsi.EvalInteraction "open Nessos.MBrace"
            fsi.EvalInteraction "open Nessos.MBrace.Client"
            fsi.EvalInteraction <| """MBraceSettings.MBracedExecutablePath <- "mbraced.exe" """
            fsi.EvalInteraction <| "MBraceSettings.DefaultTimeout <- 120 * 1000"
            fsi.EvalInteraction <| "let runtime = MBrace.InitLocal(3)" 

        [<TestFixtureTearDown>]
        let stopFsiSession () =
            FsiSession.Value.Interrupt()
            FsiSession.Value.EvalInteraction "runtime.Kill()"
            FsiSession.Stop()

        [<Test>]
        let ``Simple execution - inlined quotation`` () =
            let fsi = FsiSession.Value
            "runtime.Run <@ cloud { return 42 } @>" |> fsi.TryEvalExpression |> shouldEqual 42

        [<Test;>]
        let ``Simple execution - let binding`` () =
            let fsi = FsiSession.Value
            "[<Cloud>]let f = cloud { return 42 }" |> fsi.EvalInteraction
            "runtime.Run <@ f @>" |> fsi.EvalExpression |> shouldEqual 42

        [<Test;>]
        let ``Custom type`` () =
            let fsi = FsiSession.Value
            "type 'a tree = Leaf | Node of 'a * 'a tree * 'a tree" |> fsi.EvalInteraction
            "let t = Node(2, Leaf, Node(40, Leaf, Leaf))" |> fsi.EvalInteraction
            "[<Cloud>]let rec g x = cloud { match x with Leaf -> return 0 | Node(v,l,r) -> let! (l,r) = g l <||> g r in return v + l + r}" |> fsi.EvalInteraction
            "runtime.Run <@ g t @>" |> fsi.EvalExpression |> shouldEqual 42

        [<Test;>]
        let ``Mutable ref captured`` () =
            let fsi = FsiSession.Value
            "let x = ref 0" |> fsi.EvalInteraction
            "for i in 1 .. 3 do x := runtime.Run <@ cloud { return !x + 1 } @>" |> fsi.EvalInteraction
            "x.Value" |> fsi.EvalExpression |> shouldEqual 3

        [<Test;>]
        let ``Reference to external library`` () =
            let code = """
            
            module StaticAssemblyTest

                type Test<'T> = TestCtor of 'T

                let value = TestCtor (42, "42")
            """

            let scs = new SimpleSourceCodeServices()

            let workDir = Path.GetTempPath()
            let name = Path.GetRandomFileName()
            let sourcePath = Path.Combine(workDir, Path.ChangeExtension(name, ".fs"))
            let assemblyPath = Path.Combine(workDir, Path.ChangeExtension(name, ".dll"))
            
            do File.WriteAllText(sourcePath, code)
            let errors,code = scs.Compile [| "" ; "--target:library" ; sourcePath ; "-o" ; assemblyPath |]
            if code <> 0 then failwithf "Compiler error: %A" errors

            let fsi = FsiSession.Value

            fsi.AddReferences [assemblyPath]
            fsi.EvalInteraction "open StaticAssemblyTest"
            fsi.EvalExpression "runtime.Run <@ cloud { let (TestCtor (v,_)) = value in return v } @>" |> shouldEqual 42
