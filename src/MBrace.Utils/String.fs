module Nessos.MBrace.Utils.String

    open System
    open System.Text
    open System.Text.RegularExpressions

    //
    // string builder
    //

    type StringBuilderM = StringBuilder -> unit

    type StringExprBuilder () =
        member __.Zero () : StringBuilderM = ignore
        member __.Yield (txt : string) : StringBuilderM = fun b -> b.Append txt |> ignore
        member __.Yield (c : char) : StringBuilderM = fun b -> b.Append c |> ignore
        member __.Yield (o : obj) : StringBuilderM = fun b -> b.Append o |> ignore
        member __.YieldFrom f = f : StringBuilderM

        member __.Combine(f : StringBuilderM, g : StringBuilderM) = fun b -> f b; g b
        member __.Delay (f : unit -> StringBuilderM) = fun b -> f () b
        
        member __.For (xs : 'a seq, f : 'a -> StringBuilderM) =
            fun b ->
                let e = xs.GetEnumerator ()
                while e.MoveNext() do f e.Current b

        member __.While (p : unit -> bool, f : StringBuilderM) =
            fun b -> while p () do f b



    let string = new StringExprBuilder ()

    [<RequireQualifiedAccess>]
    module String =
        let build (f : StringBuilderM) = 
            let b = new StringBuilder ()
            do f b
            b.ToString()

        /// quick 'n' dirty indentation insertion
        let indentWith (prefix : string) (input : string) =
            (prefix + input.Replace("\n","\n" + prefix))



    type StringBuilder with
        member b.Printf fmt = Printf.ksprintf (b.Append >> ignore) fmt
        member b.Printfn fmt = Printf.ksprintf (b.AppendLine >> ignore) fmt

    let mprintf fmt = Printf.ksprintf (fun txt (b : StringBuilder) -> b.Append txt |> ignore) fmt
    let mprintfn fmt = Printf.ksprintf (fun txt (b : StringBuilder) -> b.AppendLine txt |> ignore) fmt


    //
    //  a base32 encoding scheme: suitable for case-insensitive filesystems
    //

    [<RequireQualifiedAccess>]
    module Convert =
        
        open System.IO
        open System.Collections.Generic

        // taken from : http://www.atrevido.net/blog/PermaLink.aspx?guid=debdd47c-9d15-4a2f-a796-99b0449aa8af
        let private encodingIndex = "qaz2wsx3edc4rfv5tgb6yhn7ujm8k9lp"
        let private inverseIndex = encodingIndex |> Seq.mapi (fun i c -> c,i) |> Map.ofSeq

        let toBase32String(bytes : byte []) =
            let b = new StringBuilder()
            let mutable hi = 5
            let mutable idx = 0uy
            let mutable i = 0
                
            while i < bytes.Length do
                // do we need to use the next byte?
                if hi > 8 then
                    // get the last piece from the current byte, shift it to the right
                    // and increment the byte counter
                    idx <- bytes.[i] >>> (hi - 5)
                    i <- i + 1
                    if i <> bytes.Length then
                        // if we are not at the end, get the first piece from
                        // the next byte, clear it and shift it to the left
                        idx <- ((bytes.[i] <<< (16 - hi)) >>> 3) ||| idx

                    hi <- hi - 3
                elif hi = 8 then
                    idx <- bytes.[i] >>> 3
                    i <- i + 1
                    hi <- hi - 3
                else
                    // simply get the stuff from the current byte
                    idx <- (bytes.[i] <<< (8 - hi)) >>> 3
                    hi <- hi + 5

                b.Append (encodingIndex.[int idx]) |> ignore

            b.ToString ()

        let ofBase32String(encoded : string) =
            let encoded = encoded.ToLower ()
            let numBytes = encoded.Length * 5 / 8
            let bytes = Array.zeroCreate<byte> numBytes

            let inline get i = 
                try inverseIndex.[encoded.[i]]
                with :? KeyNotFoundException -> raise <| new InvalidDataException()

            if encoded.Length < 3 then
                bytes.[0] <- byte (get 0 ||| (get 1 <<< 5))
            else
                let mutable bit_buffer = get 0 ||| (get 1 <<< 5)
                let mutable bits_in_buffer = 10
                let mutable currentCharIndex = 2

                for i = 0 to numBytes - 1 do
                    bytes.[i] <- byte bit_buffer
                    bit_buffer <- bit_buffer >>> 8
                    bits_in_buffer <- bits_in_buffer - 8
                    while bits_in_buffer < 8 && currentCharIndex < encoded.Length do
                        bit_buffer <- bit_buffer ||| (get currentCharIndex <<< bits_in_buffer)
                        bits_in_buffer <- bits_in_buffer + 5
                        currentCharIndex <- currentCharIndex + 1

            bytes


    //
    // pretty printer for records
    //

    type internal UntypedRecord = (string * string * Align) list // label * entry * alignment

    and Align = Left | Center | Right

    and Field<'Record> =
        {
            Label : string
            Align : Align
            Getter : 'Record -> string
        }

    [<RequireQualifiedAccess>]
    module Field =
        let create (label : string) alignment (projection : 'Record -> 'Field) =
            { Label = label ; Align = alignment ; Getter = fun r -> (projection r).ToString() }

    /// pretty print with grouped entries
    /// input is given as a collection of lists to group together wrt horizontal separators
    let prettyPrintTable (template : Field<'Record> list) (title : string option) useBorders (table : 'Record list list) =
        let header = template |> List.map (fun f -> f.Label, f.Label, f.Align) : UntypedRecord
        let getLine (r : 'Record) = template |> List.map (fun f -> f.Label, f.Getter r, f.Align)
        let untypedTable = List.map (List.map getLine) table : UntypedRecord list list

        let padding = 2
        let margin = 1

        let rec traverseEntryLengths (map : Map<string,int>) (line : UntypedRecord) =
            match line with
            | [] -> map
            | (label, value, _) :: rest ->
                let length = defaultArg (map.TryFind label) 0
                let map' = map.Add (label, max length <| value.Length + padding)
                traverseEntryLengths map' rest

        let lengthMap = List.fold traverseEntryLengths Map.empty (header :: (List.concat untypedTable))

        let repeat (times : int) (c : char) = String(c,times)

        let printHorizontalBorder (template : int list) =
            string {
                yield '+'

                // ------+
                for length in template do
                    yield repeat length '-'
                    yield '+'

                yield '\n'
            }

        let printEntry length align (field : string) =
            let whitespace = length - field.Length // it is given that > 0
            let lPadding, rPadding =
                match align with
                | Left -> margin, whitespace - margin
                | Right -> whitespace - margin, margin
                | Center -> let div = whitespace / 2 in div, whitespace - div

            string { 
                yield repeat lPadding ' '
                yield field 
                yield repeat rPadding ' '
                if useBorders then yield '|'
            }

        let printRecord (record : UntypedRecord) =
            string {
                if useBorders then yield '|'

                for (label, value, align) in record do
                    yield! printEntry lengthMap.[label] align value

                yield '\n'
            }

        let printSeparator (record : UntypedRecord) =
            let record' = record |> List.map (fun (label,value,align) -> (label, repeat value.Length '-', align))
            printRecord record'

        let printTitle =
            string {
                if not useBorders then yield '\n'

                match title with
                | None -> ()
                | Some title ->
                    let totalLength = (lengthMap.Count - 1) + (Map.toSeq lengthMap |> Seq.map snd |> Seq.sum)

                    // print top level separator
                    if useBorders then
                        yield! printHorizontalBorder [totalLength]
                        yield '|'

                    yield! printEntry (max totalLength (title.Length + 1)) Left title
                    yield '\n'
                    
                    if not useBorders then yield '\n'
            }

        string {
            if useBorders then
                let horizontalBorder = 
                    header 
                    |> List.map (fun (l,_,_) -> lengthMap.[l]) 
                    |> printHorizontalBorder 
                    |> String.build

                yield! printTitle

                yield horizontalBorder
                yield! printRecord header
                yield horizontalBorder

                for group in untypedTable do
                    for entry in group do
                        yield! printRecord entry
                    if group.Length > 0 then yield horizontalBorder
            else
                yield! printTitle
                yield! printRecord header
                yield! printSeparator header

                for entry in List.concat untypedTable do
                    yield! printRecord entry

        } |> String.build

    /// pretty print with discrete groupings
    let prettyPrintTable2 template title (input : 'Record list) =
        prettyPrintTable template title true (List.map (fun r -> [r]) input)

    /// pretty print with no grouping
    let prettyPrintTable3 template title useBorders (input : 'Record list) =
        prettyPrintTable template title useBorders [input]