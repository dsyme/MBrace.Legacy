namespace Nessos.MBrace.Runtime.Tests

    open System
    open System.IO

    open Nessos.MBrace

    [<AutoOpenAttribute>]
    module Helpers =

        type MVar<'T> = IMutableCloudRef<'T option>

        [<Cloud>]
        module MVar =
            let newEmpty<'T> : ICloud<MVar<'T>> = MutableCloudRef.New(None)
            let newValue<'T> value : ICloud<MVar<'T>> = MutableCloudRef.New(Some value)
            let rec put (mvar : MVar<'T>) value = 
                cloud {
                    let! v = MutableCloudRef.Read(mvar)
                    match v with
                    | None -> 
                        let! ok = MutableCloudRef.Set(mvar, Some value)
                        if not ok then return! put mvar value
                    | Some _ ->
                        return! put mvar value
                }
            let rec take (mvar : MVar<'T>) =
                cloud {
                    let! v = MutableCloudRef.Read(mvar)
                    match v with
                    | None -> 
                        return! take mvar
                    | Some v -> 
                        let! ok = MutableCloudRef.Set(mvar, None)
                        if not ok then return! take mvar
                        else return v
                }
    

    [<AutoOpen>]
    module CloudFileExtensions =
        open System
        open System.IO
        open System.Text
        open System.Collections
        open System.Collections.Generic
        open System.Runtime.Serialization

        open Nessos.MBrace.Client

        type CloudFile with
        
            [<Cloud>]
            static member ReadLines(file : ICloudFile, ?encoding : Encoding) =
                cloud {
                    let reader (stream : Stream) = async {
                        let s = seq {
                            use sr = 
                                match encoding with
                                | None -> new StreamReader(stream)
                                | Some e -> new StreamReader(stream, e)
                            while not sr.EndOfStream do
                                yield sr.ReadLine()
                        }
                        return s
                    }
                    return! CloudFile.ReadSeq(file, reader)
                }

            [<Cloud>]
            static member WriteLines(container : string, name : string, lines : seq<string>, ?encoding : Encoding) =
                cloud {
                    let writer (stream : Stream) = async {
                        use sw = 
                            match encoding with
                            | None -> new StreamWriter(stream)
                            | Some e -> new StreamWriter(stream, e)
                        for line in lines do
                            do! Async.AwaitTask(sw.WriteLineAsync(line).ContinueWith(ignore))
                    }
                    return! CloudFile.Create(container, name, writer)
                }

            [<Cloud>]
            static member ReadAllText(file : ICloudFile, ?encoding : Encoding) =
                cloud {
                    let reader (stream : Stream) = async {
                        use sr = 
                            match encoding with
                            | None -> new StreamReader(stream)
                            | Some e -> new StreamReader(stream, e)
                        return sr.ReadToEnd()
                    }
                    return! CloudFile.Read(file, reader)
                }

            [<Cloud>]
            static member WriteAllText(container : string, name : string, text : string, ?encoding : Encoding) =
                cloud {
                    let writer (stream : Stream) = async {
                        use sw = 
                            match encoding with
                            | None -> new StreamWriter(stream)
                            | Some e -> new StreamWriter(stream, e)
                        do! Async.AwaitTask(sw.WriteAsync(text).ContinueWith(ignore))
                    }
                    return! CloudFile.Create(container, name, writer)
                }
        
            [<Cloud>]
            static member ReadAllBytes(file : ICloudFile) =
                cloud {
                    let reader (stream : Stream) = async {
                        use ms = new MemoryStream()
                        do! Async.AwaitTask(stream.CopyToAsync(ms).ContinueWith(ignore))
                        return ms.ToArray() :> seq<byte>
                    }
                    return! CloudFile.ReadSeq(file, reader)
                }

            [<Cloud>]
            static member WriteAllBytes(container : string, name : string, buffer : byte []) =
                cloud {
                    let writer (stream : Stream) = async {
                        do! Async.AwaitTask(stream.WriteAsync(buffer, 0, buffer.Length).ContinueWith(ignore))
                    }
                
                    return! CloudFile.Create(container, name, writer)
                }
