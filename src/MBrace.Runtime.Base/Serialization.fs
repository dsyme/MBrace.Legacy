namespace Nessos.MBrace.Runtime

    open System.IO
    open System.Runtime.Serialization

    open Nessos.FsPickler
    open Nessos.FsPickler.Json

    open Nessos.Thespian.Serialization

    open Nessos.MBrace.Core

    type Serialization private () =

        static let lockObj = obj()
        static let mutable picklerSingleton = None

        static member internal Register(pickler : FsPicklerBase) =
            lock lockObj (fun () ->
                if picklerSingleton.IsSome then
                    invalidOp "An instance of FsPickler has been registered."

                let actorSerializer = 
                    new FsPicklerSerializer(pickler) :> IMessageSerializer

                SerializerRegistry.Register(actorSerializer)
                SerializerRegistry.SetDefault actorSerializer.Name

                picklerSingleton <- Some pickler)

        static member DefaultPickler = 
            match picklerSingleton with
            | None -> invalidOp "No instance of FsPickler has been registered."
            | Some p -> p


        static member Serialize (x : 'T) = Serialization.DefaultPickler.Pickle<obj>(x)
        static member Deserialize<'T> (data : byte []) = Serialization.DefaultPickler.UnPickle<obj>(data) :?> 'T

//        static member DeepClone =
//            {
//                new IObjectCloner with
//                    member __.Clone (t : 'T) = FsPickler.Clone t
//            }


    type JsonLogPickler private () =

        static let jsonLogPickler = FsPickler.CreateJson(omitHeader = true, indent = false)
        static do
            jsonLogPickler.UseCustomTopLevelSequenceSeparator <- true
            jsonLogPickler.SequenceSeparator <- System.Environment.NewLine

        static member WriteSingleEntry<'Entry>(w : TextWriter, e : 'Entry) = 
            jsonLogPickler.Serialize(w, e, leaveOpen = true) ; w.WriteLine() ; w.Flush()

        static member WriteEntries<'Entry>(w : TextWriter, es : seq<'Entry>, ?leaveOpen) = 
            jsonLogPickler.SerializeSequence(w, es, ?leaveOpen = leaveOpen)

        static member ReadEntries<'Entry>(r : TextReader) = jsonLogPickler.DeserializeSequence<'Entry>(r)