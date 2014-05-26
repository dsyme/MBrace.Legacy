namespace Nessos.MBrace.Utils.PrettyPrinters

    open System

    open Nessos.MBrace.Utils.String

    //
    // pretty printer for records
    //

    type private UntypedRecord = (string * string * Align) list // label * entry * alignment

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

    [<RequireQualifiedAccess>]
    module Record =
        /// pretty print with grouped entries
        /// input is given as a collection of lists to group together wrt horizontal separators
        let prettyPrint (template : Field<'Record> list) (title : string option) useBorders (table : 'Record list list) =
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
                stringB {
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

                stringB { 
                    yield repeat lPadding ' '
                    yield field 
                    yield repeat rPadding ' '
                    if useBorders then yield '|'
                }

            let printRecord (record : UntypedRecord) =
                stringB {
                    if useBorders then yield '|'

                    for (label, value, align) in record do
                        yield! printEntry lengthMap.[label] align value

                    yield '\n'
                }

            let printSeparator (record : UntypedRecord) =
                let record' = record |> List.map (fun (label,value,align) -> (label, repeat value.Length '-', align))
                printRecord record'

            let printTitle =
                stringB {
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

            stringB {
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
        let prettyPrint2 template title (input : 'Record list) =
            prettyPrint template title true (List.map (fun r -> [r]) input)

        /// pretty print with no grouping
        let prettyPrint3 template title useBorders (input : 'Record list) =
            prettyPrint template title useBorders [input]