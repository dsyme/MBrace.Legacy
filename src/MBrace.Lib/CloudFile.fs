namespace Nessos.MBrace.Lib

    open System
    open System.IO
    open System.Text
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.MBrace

    /// CloudFile extension methods

    [<AutoOpen>]
    module CloudFileExtensions =

        let private asyncWriteLine (source : StreamWriter, line : string) : Async<unit> =
            Async.AwaitTask(source.WriteLineAsync(line))

        let private asyncWriteText (source : StreamWriter, text : string) : Async<unit> =
            Async.AwaitTask(source.WriteAsync(text))

        let private asyncWriteBytes (source : Stream, buffer : byte[], offset : int, count : int) : Async<unit> =
            Async.AwaitTask(source.WriteAsync(buffer, offset, count))


        [<StructuredFormatDisplay("{StructuredFormatDisplay}")>] 
        type internal CloudFileSequence<'T>(file : ICloudFile, reader : Stream -> Async<seq<'T>>) =
            let enumerate () = 
                async {
                    let! stream = file.Read()
                    let! seq = reader stream
                    return seq.GetEnumerator()
                } |> Async.RunSynchronously
        
            override self.ToString() = file.ToString()
            member private this.StructuredFormatDisplay = this.ToString()

            interface ICloudSeq<'T> with
                member __.Name = file.Name
                member __.Container = file.Container
                member __.Type = typeof<'T>
                member __.Count = raise <| new NotSupportedException("Count not supported for CloudSeqs created from CloudFiles.")
                member __.Size = file.Size
                member __.Dispose() = async.Zero()

            interface IEnumerable<'T> with
                member __.GetEnumerator() : IEnumerator = enumerate() :> _
                member __.GetEnumerator() = enumerate()

            interface ISerializable with
                member __.GetObjectData(sI : SerializationInfo, _ : StreamingContext) =
                    sI.AddValue("file", file)
                    sI.AddValue("reader", reader)

            new (sI : SerializationInfo, _ : StreamingContext) =
                let file = sI.GetValue("file", typeof<ICloudFile>) :?> ICloudFile
                let deserializer = sI.GetValue("reader", typeof<Stream -> Async<seq<'T>>>) :?> Stream -> Async<seq<'T>>
                new CloudFileSequence<'T>(file, deserializer)

        /// CloudFile extension methods 
        type CloudFile with
        
            /// <summary> Read the contents of a CloudFile as a sequence of objects using the given deserialize/reader function.</summary>
            /// <param name="cloudFile">The CloudFile to read.</param>
            /// <param name="deserializer">The function that reads data from the underlying stream as a sequence.</param>
            [<Cloud>]
            static member ReadAsSeq(cloudFile:ICloudFile, deserializer :(Stream -> Async<seq<'T>>)) : Cloud<ICloudSeq<'T>> =
                cloud { return new CloudFileSequence<'T>(cloudFile, deserializer) :> ICloudSeq<'T> }

            /// <summary>
            ///     Reads a CloudFile as a sequence of lines.
            /// </summary>
            /// <param name="file">Input CloudFile.</param>
            /// <param name="encoding">Text encoding.</param>
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
                    return! CloudFile.ReadAsSeq(file, reader)
                }

            /// <summary>
            ///     Writes a sequence of lines to a given CloudFile uri.
            /// </summary>
            /// <param name="container">Container id.</param>
            /// <param name="name">File name.</param>
            /// <param name="lines">Lines to be written.</param>
            /// <param name="encoding">Text encoding.</param>
            [<Cloud>]
            static member WriteLines(container : string, name : string, lines : seq<string>, ?encoding : Encoding) =
                cloud {
                    let writer (stream : Stream) = async {
                        use sw = 
                            match encoding with
                            | None -> new StreamWriter(stream)
                            | Some e -> new StreamWriter(stream, e)
                        for line in lines do
                            do! asyncWriteLine(sw, line)
                    }
                    return! CloudFile.New(container, name, writer)
                }

            /// <summary>
            ///     Dump all file contents to a single string.
            /// </summary>
            /// <param name="file">Input CloudFile.</param>
            /// <param name="encoding">Text encoding.</param>
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

            /// <summary>
            ///     Writes string contents to given CloudFile.
            /// </summary>
            /// <param name="container">Container for CloudFile.</param>
            /// <param name="name">Name of CloudFile.</param>
            /// <param name="text">Input text.</param>
            /// <param name="encoding">Output encoding.</param>
            [<Cloud>]
            static member WriteAllText(container : string, name : string, text : string, ?encoding : Encoding) =
                cloud {
                    let writer (stream : Stream) = async {
                        use sw = 
                            match encoding with
                            | None -> new StreamWriter(stream)
                            | Some e -> new StreamWriter(stream, e)
                        do! asyncWriteText(sw, text)
                    }
                    return! CloudFile.New(container, name, writer)
                }
        
            /// <summary>
            ///     Dump the contents of given CloudFile as byte[].
            /// </summary>
            /// <param name="file">Input CloudFile.</param>
            [<Cloud>]
            static member ReadAllBytes(file : ICloudFile) =
                cloud {
                    let reader (stream : Stream) = async {
                        use ms = new MemoryStream()
                        do! Stream.AsyncCopy(stream, ms)
                        return ms.ToArray()
                    }
                    return! CloudFile.Read(file, reader)
                }

            /// <summary>
            ///     Write buffer contents to CloudFile.
            /// </summary>
            /// <param name="container">Container Id.</param>
            /// <param name="name">Container name.</param>
            /// <param name="buffer">Source buffer.</param>
            [<Cloud>]
            static member WriteAllBytes(container : string, name : string, buffer : byte []) =
                cloud {
                    let writer (stream : Stream) = async {
                        do! asyncWriteBytes(stream, buffer, 0, buffer.Length)
                    }
                
                    return! CloudFile.New(container, name, writer)
                }
