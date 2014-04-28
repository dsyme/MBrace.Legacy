#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Utils.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Actors.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Base.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Store.dll"
#r "../Nessos.MBrace.Client/bin/debug/Nessos.MBrace.Client.dll"
#r "../Nessos.MBrace.Lib/bin/debug/Nessos.MBrace.Lib.dll"


// Rerversible monad tests

open Nessos.MBrace.Utils
open Nessos.MBrace.Utils.Reversible

open System

// thread-safe printfn
let sprintfn fmt = Printf.ksprintf Console.WriteLine fmt

let verbose msg = 
    RevAsync.FromComponents(
        async { sprintfn "Doing %A" msg },
        async { sprintfn "Undoing %A" msg },
        async { sprintfn "Finalizing %A" msg })

let fail msg = 
    RevAsync.FromComponents( 
        async { failwith msg },
        async { printfn "Undoing %A" msg },
        async { printfn "Finalizing %A" msg })

let fail2 msg =
    revasync {
        do! verbose msg
        do! fail msg
        do! verbose "THIS SHOULD NOT APPEAR"
    }

let funny n = 
    RevAsync.FromComponents( 
        async { if n = 42 then failwith "KABOOM" else printfn "doing %d" n },
        async { printfn "Undoing %d" n },
        async { printfn "Finalizing %d" n })


let testNoFail = revasync {
    printfn "delayed side effect"
    do! verbose 1
    do! verbose 2
    printfn "continuation side effect"
    return 3
} 

RevAsync.RunWithRecovery testNoFail

let testFault = revasync {
    do! verbose 1
    do! verbose 2
    do! fail "KABOOM"
    do! verbose 3
}

RevAsync.RunWithRecovery testFault

let testFault = revasync {
    do! verbose 1
    do! verbose 2
    do! revasync {
        do! verbose "2.1"
        do! fail "KABOOM"
        do! verbose "2.2"
    }
    do! verbose 3
}

RevAsync.RunWithRecovery testFault

let testTryWith = revasync {
    do! verbose 1
    try
        do! 
            revasync { 
                do! verbose 2 
                do! verbose "2.1"
            }
        do! fail "BOOM"
        
        do! verbose 3
    with e ->
        do! verbose "recovering"

    //do! fail "BAM"
    do! verbose 5
}

RevAsync.RunWithRecovery testTryWith

let testTryWithFinal =
    revasync {
        do! verbose 1
        try
            try
                do! verbose 2
                do! verbose 3
                do! fail "WOWZAH"
            finally
                printfn "yep, i'm finalizing"
        with _ ->
            do! verbose "handling"

        do! verbose 4
    }

RevAsync.RunWithRecovery testTryWithFinal

let forTest =
    revasync {
        for i in seq { for i in 35 .. 45 -> printfn "%d" i ; i } do
            do! funny i

        do! verbose "done"
    }

RevAsync.RunWithRecovery forTest

let whileTest =
    revasync {
        let x = ref 35
        while !x < 45 do
            do! funny (!x)
            do incr x
    }

RevAsync.RunWithRecovery whileTest

open System

type Test(n : int) =
    let x = ref (Some n)
    member __.Value = x.Value.Value
    interface IDisposable with member __.Dispose() = printfn "Disposing test object %d" __.Value ; x := None
    override __.ToString() = sprintf "%d" __.Value

let disposableTest =
    revasync {
        do! verbose 1
        use! x = revasync { return new Test(42) }
        use! y = revasync { return new Test(43) }

        do! verbose x

        do! fail "Boom"

        do! verbose y

        y.Value |> ignore
    }

RevAsync.RunWithRecovery disposableTest


open System.IO

let delete (file : string) =
    let comp =
        async {
            printfn "deleting %s" file
            let tmp = Path.GetTempFileName()
            try
                File.Copy(file, tmp, true)
                File.Delete file
            with e -> File.Delete tmp ; return raise e

            return (), tmp
        }
    RevAsync.FromComponents( comp,
            (fun tmp -> async { printfn "recovering %s" file ; File.Copy(tmp, file) }),
            (fun tmp -> async { printfn "finalizing %s" file ; File.Delete tmp }))


let files = [1..10] |> List.map (fun i -> let file = Path.GetTempFileName() in File.WriteAllText(file, sprintf "file %d" i) ; file)

let test =
    revasync {
        for file in files do 
            do! delete file

//        do! fail "whatever"
    }

RevAsync.RunWithRecovery test

files |> List.iter (fun f -> File.ReadAllText f |> printfn "%s")

//
//  Parallelism
//

open System.Threading

let ofAsync(f : Async<'T>) = RevAsync.FromComponents(f, (Async.zero()), (Async.zero()))

let child  n  =
    revasync {
        do! verbose (n + "1")
        do! ofAsync (Async.Sleep 1000)
        do! verbose (n + "2")
        do! ofAsync (Async.Sleep 1000)

        do! verbose (n + "3")
        do! ofAsync (Async.Sleep 1000)

        if n = "D" then do! fail "I'm a problem child."

        sprintfn "1st Side effect from %s" n

        do! verbose (n + "4")

        sprintfn "2nd Side effect from %s" n

        return n
    }

let parallelTest =
    revasync {
        do! verbose "startup"

        let! results =
            [ "A" ; "B" ; "C" ]  @ [ "D" ]
            |> Seq.map child
            |> RevAsync.Parallel

        //do! fail "last minute failure"

        return results
    }

RevAsync.RunWithRecovery parallelTest


let child (cts : CancellationTokenSource) cancel n  =
    revasync {
        do! verbose (n + "1")
        do! ofAsync (Async.Sleep 1000)
        do! verbose (n + "2")
        do! ofAsync (Async.Sleep 1000)

        if cancel && n = "A" then printfn "cancelling external cts" ; cts.Cancel()

        do! verbose (n + "3")
        do! ofAsync (Async.Sleep 1000)

        if n = "D" then do! fail "I'm a problem child."

        do! verbose (n + "4")

        return n
    }

let parallelTest cts cancel =
    revasync {
        do! verbose "startup"

        let! results =
            [ "A" ; "B" ; "C" ] // @ [ "D" ]
            |> Seq.map (child cts cancel)
            |> RevAsync.Parallel

        //do! fail "last minute failure"

        return results
    }

RevAsync.RunWithRecovery parallelTest

//Cancellation
open System.Threading

let slowWhileTest =
    revasync {
        let x = ref 35
        while !x < 45 do
            do! funny (!x)
            do incr x
            do! RevAsync.FromAsync(Async.Sleep 1000)
    }

let cts = new CancellationTokenSource()

Async.Start(RevAsync.ToAsync slowWhileTest, cts.Token)

cts.Cancel()

RevAsync.RunWithRecovery(slowWhileTest, timeout = 1000)


testCancellation true
testCancellation false


let foo =
    revasync {
        try
            do! verbose 3
        with _ -> return ()
    }


RevAsync.RunWithRecovery foo


// nested exception
let nested () =
    revasync {
        do! verbose "nested1"
        do! verbose "nested2"

//        if true then failwith "poutses!"
//        else ()
    }

let outer = 
    revasync {
        do! verbose "outer1"
        do! verbose "outer2"

        if true then
            do! nested ()
    }

open System.Diagnostics

let startProcessAndAwaitTerminationAsync (psi : ProcessStartInfo) =
    async {
        let proc = new Process()
        proc.StartInfo <- psi
        proc.EnableRaisingEvents <- true
        if proc.Start() then
            let! _ = Async.AwaitObservable proc.Exited
            return proc
        else
            return failwith "error starting process"
    }

let psi = new ProcessStartInfo("cmd.exe", "")

startProcessAndAwaitTerminationAsync(psi) |> Async.RunSynchronously


open System
open System.IO

Environment.SpecialFolder.CommonStartMenu

let foobar = Environment.GetFolderPath Environment.SpecialFolder.DesktopDirectory

type IconDescriptor =
    {
        Name : string
        Source : string
        Icon : string option
    }

let createIcon (source : IconDescriptor) (target : string) =
    if File.Exists target then File.Delete target

    use sw = new StreamWriter(Path.Combine(target, source.Name + ".url"))
    sw.WriteLine("[InternetShortcut]")
    sw.WriteLine("URL=file:///" + source.Source)
    let icon = defaultArg source.Icon source.Source
    sw.WriteLine("IconIndex=0")
    sw.WriteLine("IconFile=" + icon.Replace('\\','/'))

    sw.Flush()

let icon = { Name = "{m}brace interactive" ; Source = @"C:\Program Files\Nessos\MBrace\bin\mbi.exe" ; Icon = None }

createIcon icon foobar