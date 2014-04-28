#I @"../Installer/bin/Debug"
#r "Nessos.MBrace.Utils.dll"
#r "Nessos.MBrace.Actors.dll"
#r "Nessos.MBrace.Common.dll"
#r "Nessos.MBrace.dll"
#r "Nessos.MBrace.Store.dll"
#r "Nessos.MBrace.Client.dll"
#r "Nessos.MBrace.Core.dll"

open Nessos.MBrace
open Nessos.MBrace.Client
// end of init

open System
open System.IO


let rt = MBrace.InitLocal 4

[<Sealed; AbstractClass>]
type UID private () =
    static let id = System.Guid.NewGuid()
    static member GetWorkerId () = id.ToString()

[<Cloud>]
let run () = 
    cloud {
        return!
            Array.create 10
                (cloud { return System.Diagnostics.Process.GetCurrentProcess().Id, UID.GetWorkerId() })
            |> Cloud.Parallel
    }


rt.Run <@ run () @>
|> Array.sort

rt.Run <@ Cloud.GetWorkerCount() @>




[<Cloud>]
let run1 () = 
    cloud {
        let ms = new MemoryStream([|42uy..50uy|]) :> obj

        let! cf = CloudFile.Create(fun stream -> async { 
            return (ms :?> Stream).CopyTo(stream); stream.Flush() })
        return! CloudFile.Read(cf, fun stream -> async { use sr = new StreamReader(stream)
                                                         return sr.Read() } )
    }

type TheWrapper = TheWrapper of MemoryStream with
    member this.Unwrap = let (TheWrapper ms) = this in ms

[<Cloud>]
let run2 () = cloud {
    
    let ms = TheWrapper(new MemoryStream([|42uy..50uy|]) )

    let! cf = CloudFile.Create(fun stream -> async { 
        return ms.Unwrap.CopyTo(stream); stream.Flush() })
    return! CloudFile.Read(cf, fun stream -> async { use sr = new StreamReader(stream)
                                                     return sr.Read() } )
}




let rt = MBrace.InitLocal 4
rt.Run <@ run1 () @>




































type NodeState = White | Gray | Black

type Distance = Distance of int | Inf // declaring Inf last means that Inf > Distance _
    with static member (+) (l : Distance, r : int) =
            match l with
            | Distance l -> Distance (l + r)
            | Inf -> Inf

type ParentType = Source | None | Parent of int

type Node = { 
        Id : int
        Nodes : int []
        mutable State : NodeState
        mutable Distance : Distance
        mutable Parent : ParentType 
    }

let partition n (a : _ array) =
    [| for i in 0 .. n - 1 ->
        let i, j = a.Length * i / n, a.Length * (i + 1) / n
        Array.sub a i (j - i) |]

let parse filename =
    let parse_line (line : string) =
        let [|key; value|] = line.Split [|'\t'|]
        let [|nodes; dist; state; parent|] = value.Split [|'|'|]
        let nodes = nodes.Split [|','|] |> Array.map int
        let dist  = if dist = "Integer.MAX_VALUE" then Inf else Distance (int dist)
        let state = match state with
                    | "WHITE" -> White
                    | "GRAY"  -> Gray
                    | "BLACK" -> Black
        let parent = match parent with
                     | "null"    -> None
                     | "source"  -> Source
                     | id        -> Parent (int id)
        { Id = int key; Nodes = nodes; State = state; Distance = dist; Parent = parent }

    File.ReadLines(filename)
    |> Seq.map parse_line
    |> Seq.toArray

let create_input filename nodes =
    let lines =
        Seq.init nodes (fun id ->
            seq {
                let id = id + 1
                if id % 1000 = 0 then printfn "%d" id
                yield string id 
                yield "\t"
                let rnd = Random(id)
                let childs = 1000 //rnd.Next(1, nodes + 1)
                yield { 1..childs }
                      |> Seq.map (fun _ -> string (rnd.Next(1, nodes + 1)))
                      //|> Seq.map (fun _ -> string (rnd.Next(id, nodes + 1)))
                      |> Seq.distinct
                      |> Seq.reduce (fun a b -> a + "," + b)
                yield "|"
                if id = 1 then yield "0" else yield "Integer.MAX_VALUE"
                yield "|"
                if id = 1 then yield "GRAY" else yield "WHITE"
                yield "|"
                if id = 1 then yield "source" else yield "null"
            }
            |> String.concat "")
    File.WriteAllLines(filename, lines)

[<Cloud>]
let write_nodes (nodes : Node []) = cloud {
        let! pid = Cloud.GetProcessId()
        for node in nodes do
            do! MutableCloudRef.New("nodetest" , string node.Id, node)
                |> Cloud.Ignore
    }

[<Cloud>]
let iterate (id : int) (flag : IMutableCloudRef<bool>) = cloud {
    let! nodeRef = MutableCloudRef.Get<Node>("nodetest", string id)
    let! node = MutableCloudRef.Read(nodeRef)

    match node.State with
    | Gray ->
        let hasGray = ref false
        for neighborId in node.Nodes do

            let! neighborRef = MutableCloudRef.Get<Node>("nodetest", string neighborId)

            let! child = MutableCloudRef.Read(neighborRef)

            do! MutableCloudRef.SpinSet(neighborRef,
                    fun neighbor -> 
                        match neighbor.State with
                        | Black -> ()
                        | Gray  ->
                            if neighbor.Distance > node.Distance + 1 then
                                neighbor.Distance <- node.Distance + 1
                                neighbor.Parent <- Parent node.Id
                            hasGray := true
                        | White ->
                            neighbor.Distance <- node.Distance + 1
                            neighbor.Parent   <- Parent node.Id
                            neighbor.State    <-  Gray
                            hasGray := true
                        neighbor
                    )
            
        node.State <- Black
        do! MutableCloudRef.SpinSet(nodeRef, fun _ -> node)

        if !hasGray then do! MutableCloudRef.SpinSet(flag, fun _ -> true)
    | _ -> ()
}

[<Cloud>]
let bfs (nodes : int) = cloud {
    let! workers = Cloud.GetWorkerCount()
    let workers = workers * 4

    let! flagRef = MutableCloudRef.New(true)
   
    let rec loop flag = cloud {
        if flag then
            do! MutableCloudRef.Force(flagRef, false)

            do! [|1..nodes|]
                |> partition workers
                |> Array.map (fun nodes -> cloud { 
                        for n in nodes do
                            do! iterate n flagRef
                    })
                |> Cloud.Parallel
                |> Cloud.Ignore

            let! flagVal = MutableCloudRef.Read(flagRef)

            return! loop flagVal
    }
    
    do! loop true
}

let rt = MBrace.InitLocal 8

let nodes = [1..5] |> List.map (fun n -> MBraceNode("virtual" + string n, 2675))
nodes |> List.iter (fun n -> printfn "n %A %A" n (n.Ping()))
let rt = MBrace.Boot nodes
let n = parse (@"c:\users\krontogiannis\desktop\input10000.txt")


let g = 500
create_input  (@"c:\users\krontogiannis\desktop\input" + string g + ".txt") g
let n = parse (@"c:\users\krontogiannis\desktop\input" + string g + ".txt")

rt.DeleteContainer("nodetest")
rt.Run <@ write_nodes n @>
let ps = rt.CreateProcess <@ bfs n.Length @>
ps

rt.ShowUserLogs(ps.ProcessId)
ps.AwaitResult()
ps.Kill()
rt.Reboot()


let r = 
    rt.Run <@ cloud { 
            let! refs = MutableCloudRef.Get("nodetest") 
            return! refs 
                    |> Seq.cast<IMutableCloudRef<Node>> 
                    |> Seq.toArray
                    |> Array.map MutableCloudRef.Read
                    |> Cloud.Parallel
        } @>

r   |> Seq.sortBy (fun n -> n.Id)
    |> Seq.iter (fun n -> printfn "%A %A %A %A %A" n.Id n.Nodes n.Distance n.State n.Parent)

let parse_hadoop filename =
    seq {
        for line in File.ReadLines(filename) do
            let [|node; rest|] = line.Split([|'\t'|])
            let rest = rest.Split([|'|'|])
            yield node, rest.[1], rest.[2]
    }
    |> Seq.toArray
    |> Array.sort
   
let parse_mbrace (nodes : Node []) =
    [| for n in nodes do
        yield string n.Id, (match n.Distance with Inf -> "Integer.MAX_VALUE" | Distance d -> string d), (match n.State with White -> "WHITE" | Black -> "BLACK")
    |]
    |> Array.sort
    
let a1 = parse_hadoop @"c:\users\krontogiannis\desktop\outspace\out-hadoop-1000.txt"
let a2 = parse_mbrace r
a1 = a2

Seq.zip a1 a2
|> Seq.map (fun ((n1,d1,s1),(n2,d2,s2)) ->  (n1,n2), (d1,d2), (s1,s2))
|> Seq.iter (fun ((n1,n2), (d1,d2), (s1,s2)) -> if d1 <> d2 then printfn "%A" (n1,n2,d1,d2,s1,s2))


//            match child.State with
//            | Black -> ()
//            | Gray  ->
//                if child.Distance > node.Distance + 1 then
//                    child.Distance <- node.Distance + 1
//                    child.Parent <- Parent node.Id
//                hasGray := true
//            | White ->
//                child.Distance <- node.Distance + 1
//                child.Parent   <- Parent node.Id
//                child.State    <-  Gray
//                hasGray := true
//
//            do! MutableCloudRef.Force(neighborRef, child)

            //let! child = MutableCloudRef.Read(neighborRef)

//
//    let flag = ref true
//
//    while !flag do
//        do! MutableCloudRef.Force(flagRef, false)
////        do! [|1..nodes|]
////            |> partition workers
////            |> Array.map (fun nodes -> cloud { 
////                    for n in nodes do
////                        do! iterate n flagRef
////                })
////            |> Cloud.Parallel
////            |> Cloud.Ignore
//        
//        do! [|1..nodes|] 
//            |> Array.map (fun n -> iterate n flagRef)
//            |> Cloud.Parallel
//            |> Cloud.Ignore
//
////        for n in [|1..nodes|] do
////            do! iterate n flagRef
//
//        let! flagVal = MutableCloudRef.Read(flagRef)
//
//        flag := flagVal
//
//
//
////            do! [|1..nodes|] 
////                |> Array.map (fun n -> iterate n flagRef)
////                |> Cloud.Parallel
////                |> Cloud.Ignore






//
//
//    let flag = ref true
//
//    while !flag do
//        do! MutableCloudRef.Force(flagRef, false)
//
//        do! Cloud.Logf "flag before : %A" flag
//
//        do! [|1..nodes|] 
//            |> Array.map (fun n -> iterate n flagRef)
//            |> Cloud.Parallel
//            |> Cloud.Ignore
//
//        do! Cloud.Logf "flag after : %A" flag
//
//        let! flagVal = MutableCloudRef.Read(flagRef)
//
//        do! Cloud.Logf "flagVal : %A" flagVal
//
//        flag := flagVal




let rt = MBrace.InitLocal 10

[<Cloud>]
let rec bin (depth : int)  = cloud {
    if depth = 0 then 
        return 1
    else 
        let! (l,r) = bin (depth-1) <||> bin (depth-1) 
        return l + r
}

let ps = rt.CreateProcess <@ bin 10 @>

ps.AwaitResult()


rt.ShowInfo()
rt.ShowProcessInfo()

rt.Reboot()







