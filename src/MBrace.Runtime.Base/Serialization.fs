namespace Nessos.MBrace.Runtime

    open System.IO
    open System.Runtime.Serialization

    open Nessos.FsPickler

    open Nessos.Thespian.Serialization

    open Nessos.MBrace.Core

    type Serialization private () =

        static let lockObj = obj()
        static let mutable picklerSingleton = None

        static member Register(pickler : FsPickler) =
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

        static member DeepClone =
            {
                new IObjectCloner with
                    member __.Clone (t : 'T) =
                        use mem = new MemoryStream()
                        Serialization.DefaultPickler.Serialize(mem, t)
                        mem.Position <- 0L
                        Serialization.DefaultPickler.Deserialize<'T>(mem)
            }
