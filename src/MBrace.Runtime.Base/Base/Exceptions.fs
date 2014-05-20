namespace Nessos.MBrace.Runtime

    open Nessos.MBrace
    
    [<AutoOpen>]
    module MBraceException =

        let mkMBraceExn inner msg = 
            match inner with
            | Some (inner : exn) -> MBraceException (msg, inner)
            | None -> MBraceException msg

        let inline mfailwith msg = mkMBraceExn None msg |> raise
        let inline mfailwithInner exn msg = mkMBraceExn (Some exn) msg |> raise
        let inline mfailwithf fmt = Printf.ksprintf(mfailwith) fmt
        let inline mfailwithfInner exn fmt = Printf.ksprintf (mfailwithInner exn) fmt

        let rec (|MBraceExn|_|) (e : exn) =
            match e with
            | :? MBraceException as mbe -> Some mbe
            | e when e.InnerException <> null -> (|MBraceExn|_|) e.InnerException
            | _ -> None