namespace Nessos.MBrace.Runtime

    module Maybe =
        type Maybe<'T> = 'T option

        let succeed x = Some x
        let fail = None

        type MaybeBuilder() =
            member m.Bind(maybe: Maybe<'T>, bindF: 'T -> Maybe<'Y>): Maybe<'Y> =
                match maybe with
                | Some x -> bindF x
                | None -> fail

            member m.Return(v: 'T): Maybe<'T> = succeed v
            member m.ReturnFrom(maybe: Maybe<'T>): Maybe<'T> =
                maybe

            member m.Delay(f: unit -> Maybe<'T>): Maybe<'T> = f()
            member m.For(vs: seq<'T>, bindF: 'T -> Maybe<unit>): Maybe<unit> =
                succeed (for v in vs do bindF v |> ignore)

            member m.Zero(): Maybe<unit> = Some()

        let maybe = MaybeBuilder()