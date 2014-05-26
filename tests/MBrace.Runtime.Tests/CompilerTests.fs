namespace Nessos.MBrace.Runtime.Tests

    open Nessos.MBrace
    open Nessos.MBrace.Client

    open NUnit.Framework

    open FsUnit

    [<TestFixture>]
    module ``Cloud Compiler Tests`` =

        let compile expr = 
            try 
                let c = MBrace.Compile expr
                match c.Warnings with
                | [] -> printfn "compilation successful."
                | ws -> printfn "compilation with warnings:\n%s" <| String.concat "\n" ws
                c

            with e -> printfn "%O" e ; reraise ()

        let shouldFailCompilation expr = shouldFailwith<CompilerException> (fun () -> compile expr |> ignore)
        let shouldSucceedCompilation expr = let comp = compile expr in comp.Warnings |> should equal []

        let valueWithoutAttribute = cloud { return 42 }
        let functionWithoutAttribute x = cloud { return x + 1 }

        [<Cloud>]
        let blockWithCloudAttributeCallingBlockWithCloudAttr () = cloud {
            try
                let! x = Cloud.Parallel [| cloud { return! functionWithoutAttribute 31 } |]

                return x.[0]

            with e ->
                return -1
        }

        [<Cloud>]
        let blockWithNonSerializableBinding () = cloud {
            let! value = cloud { return [|1uy|] }
            let m = new System.IO.MemoryStream(value)
            return m.Length
        }

        [<Cloud>]
        let blockThatContainsNonMonadicNonSerializableBinding () = cloud {
            let! value = cloud { return [| 1uy |] }
            let! length = Cloud.OfAsync <| async { let m = new System.IO.MemoryStream(value) in return m.Length }
            return length
        }

        type CloudObject () =
            let x = ref 0

            [<Cloud>]
            member __.Compute () = cloud { incr x ; return !x }

        [<Cloud>]
        let rec blockThatCallsClientApi () = cloud {
            return
                let runtime = MBrace.InitLocal 4 in
                runtime.Run <@ blockThatCallsClientApi () @>
        }

        [<Cloud>]
        module Module =

            module NestedModule =
                let nestedWorkflowThatInheritsCloudAttributeFromContainers () = cloud { return 42 }

            let workflowThatInheritsCloudAttributeFromContainers () = cloud { 
                return! NestedModule.nestedWorkflowThatInheritsCloudAttributeFromContainers ()
            }


        [<Test>]
        let ``Cloud value missing [<Cloud>] attribute`` () =
            shouldFailCompilation <@ valueWithoutAttribute @>

        [<Test>]
        let ``Cloud function missing [<Cloud>] attribute`` () =
            shouldFailCompilation <@ functionWithoutAttribute 41 @>

        [<Test>]
        let ``Nested cloud block missing [<Cloud>] attribute`` () =
            shouldFailCompilation <@ blockWithCloudAttributeCallingBlockWithCloudAttr () @>

        [<Test>]
        let ``Workflow that inherits [<Cloud>] attribute from containing modules`` () =
            shouldSucceedCompilation <@ Module.workflowThatInheritsCloudAttributeFromContainers () @>

        [<Test>]
        let ``Cloud block with non-serializable binding`` () =
            shouldFailCompilation <@ blockWithNonSerializableBinding () @>

        [<Test>]
        let ``Cloud block with non-serializable binding which is non-monadic`` () =
            shouldSucceedCompilation <@ blockThatContainsNonMonadicNonSerializableBinding () @>

        [<Test>]
        let ``Cloud block that is object member`` () =
            shouldFailCompilation <@ cloud { let obj = new CloudObject () in return! obj.Compute () } @>

        [<Test>]
        let ``Cloud block that calls the MBrace client API`` () =
            shouldFailCompilation <@ blockThatCallsClientApi () @>

        [<Test>]
        let ``Cloud block that captures MBrace.Client object`` () =
            let computation = box <| MBrace.Compile <@ cloud { return 42 } @>

            shouldFailCompilation <@ cloud { let x = computation.GetHashCode() in return x } @>

        [<Test>]
        let ``Cloud block that references MBrace.Client type`` () =
            shouldFailCompilation <@ cloud { return typeof<Nessos.MBrace.Client.MBraceRuntime> } @>