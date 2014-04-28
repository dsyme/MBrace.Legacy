#I "bin/Debug"
#r "Nessos.MBrace.Utils"
#r "Nessos.MBrace.Actors"
#r "Nessos.MBrace.Actors.Remote"
#r "Nessos.MBrace"
#r "Nessos.MBrace.Common"
#r "Nessos.MBrace.Store"
#r "Nessos.MBrace.Client"

open Nessos.MBrace
open Nessos.MBrace.Client

open System.Reflection

let r = MBrace.InitLocal 3

[<Cloud>]
let foo (f : int -> int) = cloud { return f 42 }

[<Cloud>]
let test1 () = 
    cloud {
        return! foo (fun y -> new System.IO.FileStream("C:\temp", System.IO.FileMode.Create) |> ignore ; 32)
    }

[<Cloud>]
let test2 () =
    cloud {
        let f =
            let m = new System.IO.MemoryStream()
            fun () -> m
        return f
    }

[<Cloud>]
let test3 () =
    cloud {
        try
            if true then
                match Some 42 with
                | Some 1 when 1 > 0 ->
                    for i in [| 1 .. 1000 |] do
                        while true do
                            let client = new System.Net.WebClient()
                            return ()
                | _ -> return ()
        with _ -> ()
    }
                    

r.Run <@ test1 () @>
r.Run <@ test2 () @>
r.Run <@ test3 () @>