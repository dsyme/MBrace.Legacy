namespace Nessos.MBrace.Utils.Json

    open System
    open System.IO
    open System.Text
    open System.Collections
    open System.Collections.Generic

    open Newtonsoft.Json

    type JsonSequenceSerializer<'T> (writer : TextWriter, ?newLine) =
        static let jsonSerializer = JsonSerializer.Create()

        let newLine = defaultArg newLine true
        let jwriter = new JsonTextWriter(writer)

        member __.WriteNext(t : 'T) =
            jsonSerializer.Serialize(jwriter, t)
            if newLine then
                jwriter.Flush()
                writer.WriteLine()

        member __.Close() = writer.Flush() ; writer.Close()

        interface IDisposable with member __.Dispose () = __.Close ()


    type JsonSequenceDeserializer<'T> (reader : TextReader) =
        
        static let jsonSerializer = JsonSerializer.Create()

        let jreader = new JsonTextReader(reader)
        do jreader.SupportMultipleContent <- true

        let getEnumerator () =
            let current = ref Unchecked.defaultof<'T>
            {
                new IEnumerator<'T> with
                    member __.Current = current.Value
                    member __.Current : obj = box current.Value
                    member __.Dispose () = ()
                    member __.MoveNext () =
                        try
                            match jreader.TokenType with
                            | JsonToken.EndObject when not <| jreader.Read() -> false
                            | _ ->
                                current := jsonSerializer.Deserialize<'T>(jreader)
                                true

                        // protect from partially written log files
                        with _ -> false
                            
                    member __.Reset () = raise <| new NotSupportedException()
            }

        interface IEnumerable<'T> with
            member d.GetEnumerator() = getEnumerator ()
            member d.GetEnumerator() = getEnumerator () :> IEnumerator


    type JsonSequence =
        
        static member CreateSerializer<'T>(writer : TextWriter, ?newLine) = new JsonSequenceSerializer<'T>(writer, ?newLine = newLine)
        static member CreateDeserializer<'T>(reader : TextReader) = new JsonSequenceDeserializer<'T>(reader)

        static member CreateSerializer<'T>(stream : Stream, ?encoding : Encoding, ?newLine) =
            let writer = 
                match encoding with
                | Some e -> new StreamWriter(stream, e)
                | None -> new StreamWriter(stream)

            new JsonSequenceSerializer<'T>(writer, ?newLine = newLine)

        static member CreateDeserializer<'T>(stream : Stream, ?encoding : Encoding) =
            let reader =
                match encoding with
                | Some e -> new StreamReader(stream, e)
                | None -> new StreamReader(stream)

            new JsonSequenceDeserializer<'T>(reader)