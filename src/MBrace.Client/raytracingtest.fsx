//Prelude
//Evaluate in fsi before all else
//#r "FSharp.PowerPack"
//#r "FSharp.PowerPack.Linq"
#I "../Installer/bin/debug"

#r "System.Runtime.Serialization"
#r "System.Xml"
#r "System.Xml.Linq"
#r "Nessos.MBrace.Actors.dll"
#r "Nessos.MBrace.Actors.Remote.dll"
#r "Nessos.MBrace.Utils.dll"
#r "Nessos.MBrace.Serialization.dll"

#I @"C:\Users\krontogiannis\Desktop\Raytracer\Raytracing\bin\Release"
#I @"C:\Users\Administrator\Desktop\"
#r "Raytracing.dll"

open Nessos.MBrace.Actors
open Nessos.MBrace.Actors.Serialization
open Nessos.MBrace.Actors.Remote
open Nessos.MBrace.Actors.Remote.TcpProtocol
open Nessos.MBrace.Actors.Remote.TcpProtocol.Unidirectional
open Nessos.MBrace.Serialization
open System.Net
//open Raytracing

//INIT
//common

let ip =
    let host = Dns.GetHostEntry(Dns.GetHostName())
    host.AddressList
    |> Seq.find (fun ip -> ip.AddressFamily = Sockets.AddressFamily.InterNetwork)
    |> string

ConnectionPool.TcpConnectionPool.Init()
SerializerRegistry.Register(new FsCoreSerializer() |> plug, true)
TcpListenerPool.RegisterListener(Address.Parse(ip + ":3242"))

let testActor : Actor<IReplyChannel<int []>> =
    Actor.bind (fun actor ->
        let rec loop () =
            async {
                let! (ch : IReplyChannel<int []>) = actor.Receive()
                let out = Harness.StaticRayTracer.renderParallel()
                ch.Reply(Value out)
                return! loop ()
            }
        loop ())
    |> Actor.rename "testActor"
    |> Actor.publish [UTcp()]
    |> Actor.start


////IN MEMORY REF
//let testActorRef = !testActor
////REMOTE REF
//let testActorRef = (!testActor).["utcp"]

let testActorRef : ActorRef<IReplyChannel<int []>> = ActorRef.fromUri "utcp://10.61.180.142:3242/*/testActor/fscs"

//testActorRef <!= id

Async.RunSynchronously <| testActorRef.PostWithReply id


let rec render (ms : int64) (frames : int) = async {
    let sw = System.Diagnostics.Stopwatch()
    sw.Start()

    let! rgb = testActorRef.PostWithReply id
    sw.Stop()

    let ms = ms + sw.ElapsedMilliseconds
    let frames = frames + 1

    if ms > 1000L then
        printfn "FPS %A" <| (float frames) / (float ms)
        return! render 0L 0
    else
        return! render ms frames
}

