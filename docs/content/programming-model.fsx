(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../bin/"
#I "../../src/MBrace.Client/"

#load "preamble.fsx"
open Nessos.MBrace
open Nessos.MBrace.Lib
open Nessos.MBrace.Client

(**

# The MBrace Programming model

The MBrace programming model is based on F# 
[computation expressions](http://msdn.microsoft.com/en-us/library/dd233182.aspx),
a feature that allows user-defined, language-integrated DSLs.
A notable application of computation expressions in F# is
[asynchronous workflows](http://msdn.microsoft.com/en-us/library/dd233250.aspx),
a core library implementation that offers a concise and elegant asynchronous programming model.
MBrace draws heavy inspiration from asynchronous workflows and extends it to the domain of
distributed computation.

## Cloud workflows

In MBrace, the unit of computation is a *cloud workflow*:
*)

let myFirstCloudWorkflow = cloud { return 42 }

(**

Cloud workflows generate objects of type `Cloud<'T>`, which denote a delayed computation
that once executed will yield a result of type `'T`. Cloud workflows are language-integrated
and can freely intertwine with native code.

*)

let mySecondCloudWorkflow = cloud {
    let now = System.DateTime.Now
    printfn "Current time: %O" now
    return ()
}

(**

Note that the specific example introduces a side-effect to the computation.
Due to the distributed nature of cloud workflows, it is unclear where this might
take place once executed.

Simple cloud computations can be composed into larger ones using the `let!` keyword:

*)

let first = cloud { return 15 }
let second = cloud { return 27 }

cloud {
    let! x = first
    let! y = second
    return x + y
}

(** 

This creates bindings to the first and second workflows respectively,
to be consumed by the continuations of the main computation.
Once executed, this will sequentually perform the computations in `first`
and `second`, resuming once the latter has completed.

Recursion and higher-order computations are possible:

*)

let rec foldl f s ts = cloud {
    match ts with
    | [] -> return s
    | t :: ts' ->
        let! s' = f s t
        return! foldl f s' ts'
}

(**

and so are for loops and while loops.

*)

cloud {
    for i in [| 1 .. 100 |] do
        do! Cloud.Logf "Logging entry %d of 100" i

    while true do
        do! Cloud.Sleep 200
        do! Cloud.Log "Logging forever..."
}

(**

MBrace workflows also integrate with exception handling:

*)

cloud {
    try
        let! x = cloud { return 1 / 0 }
        return true

    with :? System.DivideByZeroException -> return false
}

(**

Asynchronous workflows can be embedded into cloud workflows:

*)

let downloadLines (url : string) = async {
    use http = new System.Net.WebClient()
    let! html = http.AsyncDownloadString(System.Uri url) 
    return html.Split('\n')
}

cloud {
    let download u = Cloud.OfAsync(downloadLines u)
    let! t1 = download "http://www.nessos.gr/"
    let! t2 = download "http://www.m-brace.net/"
    return t1.Length + t2.Length
}

(**

## Parallelism Combinators

Cloud workflows as discussed so far enable asynchronous
computation but do not suffice in describing parallelism and distribution.
To control this, MBrace uses a collection of primitive combinators that act on
the distribution/parallelism semantics of execution in cloud workflows.

The previous example could be altered so that downloading happens in parallel:

*)

cloud {
    let download = Cloud.OfAsync << downloadLines
    let! t1,t2 = download "http://www.m-brace.net" <||> download "http://www.nessos.gr/"
    return t1.Length + t2.Length
}

(**

The `<||>` operator defines the *binary parallel combinator*, which combines two workflows
into one where both will executed in fork/join parallelism.
It should be noted that parallelism in this case means that each of the workflows
will be scheduled for execution in remote and potentially separete worker machines
in the MBrace cluster.

The `Cloud.Parallel` combinator is the variadic version of `<||>`,
which can be used for running arbitrarily many cloud workflows in parallel:

*) 

cloud {
    let sqr x = cloud { return x * x }

    let N = System.Random().Next(50,100) // number of parallel jobs determined at runtime
    let! results = Cloud.Parallel(List.map sqr [1.. N])
    return Array.sum results
}

(**

A point of interest here is exception handling. Consider the workflow

*)

cloud {
    let download = Cloud.OfAsync << downloadLines

    try
        let! results =
            [
                "http://www.m-brace.net/"
                "http://www.nessos.gr/"
                "http://non.existent.domain/" ]
            |> List.map download
            |> Cloud.Parallel

        return results |> Array.sumBy(fun r -> r.Length)

    with :? System.Net.WebException as e ->
        // log and reraise
        do! Cloud.Logf "Encountered error %O" e
        return raise e
}

(**

Clearly, one of the child computations will fail on account of
an invalid url, creating an exception. In general, uncaught exceptions
bubble up through `Cloud.Parallel` triggering cancellation of all
outstanding child computations (just like `Async.Parallel`).

The interesting bit here is that the exception continuation
will almost certainly be executed in a different machine than the one
in which it was originally thrown. This is due to the interpreted
nature of the monadic skeleton of cloud workflows, which allows
exceptions, environments, closures to be passed around worker machines
in a seemingly transparent manner.

*)