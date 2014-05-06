namespace Nessos.MBrace.Runtime

    open System

    open Microsoft.FSharp.Quotations

    open Nessos.MBrace

    // TODO: replace with this

//    type CloudPackage<'T> =
//        | Raw of ICloud<'T>
//        | Quoted of Expr<ICloud<'T>>
//    with
//        member __.ReturnType = typeof<'T>
//        member pkg.Value =
//            match pkg with
//            | Raw c -> c
//            | Quoted e -> Swensen.Unquote.Operators.eval e

    type CloudPackage private (expr : Expr, t : Type) =
        member __.Expr = expr
        member __.ReturnType = t
        member __.Eval () = Swensen.Unquote.Operators.evalRaw expr : ICloud
        static member Create (expr : Expr<ICloud<'T>>) =
            CloudPackage(expr, typeof<'T>)