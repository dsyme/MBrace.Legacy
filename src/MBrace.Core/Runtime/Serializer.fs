namespace Nessos.MBrace.Runtime

    open System.IO
    open System.Runtime.Serialization

    open Nessos.FsPickler

    open Nessos.Thespian.Serialization

    open Nessos.MBrace.Utils
    

//    type PicklerSerializer(pickler : FsPickler) =
//        interface IMessageSerializer with
//            member __.Name = "FsPickler"
//
//            member __.Serialize(context:obj, value:obj) =
//                use mem = new MemoryStream()
//                let sc = new StreamingContext(StreamingContextStates.All, context)
//                pickler.Serialize<obj>(mem, value, streamingContext = sc)
//                mem.ToArray()
//
//            member __.Deserialize(context:obj, data:byte[]) =
//                use mem = new MemoryStream(data)
//                let sc = new StreamingContext(StreamingContextStates.All, context)
//                pickler.Deserialize<obj>(mem, streamingContext = sc)


    type Serializer private () =

        static let picklerSingleton = ref None

        static member Register(pickler : FsPickler) =
            lock picklerSingleton (fun () ->
                if picklerSingleton.Value.IsSome then
                    invalidOp "An instance of FsPickler has been registered."

                let actorSerializer = 
                    new FsPicklerSerializer(pickler) :> IMessageSerializer

                SerializerRegistry.Register(actorSerializer)
                SerializerRegistry.SetDefault actorSerializer.Name
                IoC.RegisterValue(pickler, behaviour = OverrideBehaviour.Override)

                picklerSingleton := Some pickler)

        static member Pickler = 
            match picklerSingleton.Value with
            | None -> invalidOp "No instance of FsPickler has been registered."
            | Some p -> p


        static member Serialize (x : 'T) = Serializer.Pickler.Pickle<obj>(x)
        static member Deserialize<'T> (data : byte []) = Serializer.Pickler.UnPickle<obj>(data) :?> 'T
        static member Clone(x : 'T) =
            let pickler = Serializer.Pickler
            use mem = new MemoryStream()
            pickler.Serialize(mem, x)
            mem.Position <- 0L
            pickler.Deserialize<'T>(mem)