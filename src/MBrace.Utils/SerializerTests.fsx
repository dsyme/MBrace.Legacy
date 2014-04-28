#r "bin/Debug/Nessos.MBrace.Utils.dll"

open System
open System.IO
open System.Reflection
open System.Runtime.Serialization
open System.Collections.Generic
open SerializerExperiments
    

type Foo = 
    | Bar1
    | Bar2 of int

type Peano =
    | Zero
    | Succ of Peano

let rec int2Peano n = match n with 0 -> Zero | n -> Succ(int2Peano(n-1))

serialize (int2Peano 10) |> deserialize
serialize 1 |> deserialize
serialize (1, Some 3, [1..10]) |> deserialize
serialize (Bar2 2) |> deserialize
serialize (1, (1,2,3,4) , [5..10]) |> deserialize
serialize [|1..10|] |> deserialize
serialize typeof<int> |> deserialize
serialize (Exception("skata"), Exception("fraoules")) |> deserialize

let foo = System.Func<int,int>(fun x -> x + 1)

serialize foo |> deserialize

let recArray = let x = [| null |] : obj [] in x.[0] <- x :> obj ; x

serialize recArray |> deserialize

let d = [1..100] |> Seq.map (fun i -> i, string i) |> dict
let m = [1..100] |> Seq.map (fun i -> i, string i) |> Map.ofSeq

serialize d |> deserialize
serialize m |> deserialize

let objs =
    typeof<int>.Assembly.GetTypes()
        |> Array.filter (fun t -> t.IsSerializable && not t.IsAbstract)
        |> Array.choose (fun t -> try Activator.CreateInstance t |> Some with _ -> None)

let test (o : obj) = try serialize o |> deserialize |> ignore ; None with _ -> Some (o.GetType())

let failures = objs |> Array.choose test

let foo = failures.[3]

bfs o

let o = new System.TimeZoneInfo.TransitionTime()

let o = Activator.CreateInstance (typeof<System.TimeZoneInfo.TransitionTime>) :?> System.TimeZoneInfo.TransitionTime

let o = Activator.CreateInstance foo
let fs = 
    failures 
    |> Array.choose (fun t -> let o = Activator.CreateInstance t in try serialize o |> deserialize |> ignore ; None with e -> Some e)
    |> Array.filter (function :? SerializationException -> false | _ -> true)

fs.Length

let x = System.NotFiniteNumberException(3.14)
let x = System.ArithmeticException()
let ctor = typeof<System.NotFiniteNumberException>.GetConstructors(ctorBindings).[6]
let ctor' = typeof<System.ArithmeticException>.GetConstructors(ctorBindings).[3]

let sI = new SerializationInfo(typeof<System.NotFiniteNumberException>, new FormatterConverter())
let sC = new StreamingContext()

let fc = new FormatterConverter()


ctor.DeclaringType

x.GetObjectData(sI, sC)
sI.MemberCount
ctor.Invoke [| sI :> obj ; sC :> obj |]

ctor'.Invoke [| sI :> obj ; sC :> obj |]


let x = 3.1415926
sI.AddValue("foo", x, typeof<int>)
sI.GetInt32("foo")

let bsI = ref null : SerializationInfo ref

type Foo(x : float) =

    new (sI : SerializationInfo, _ : StreamingContext) =
        let o = sI.GetValue("test", typeof<obj>)
        printfn "read %A : %s" o <| o.GetType().Name
        new Foo(float <| sI.GetInt32("test"))

    member __.Value = x

    interface ISerializable with
        member __.GetObjectData(sI : SerializationInfo, _ : StreamingContext) =
            printfn "writing"
            sI.AddValue("test", x, typeof<int>)
    
let s = Foo(42.0)

serialize s |> deserialize
bfs s

type ITest =
    abstract Foo : obj -> unit

and Test<'T> = { Foo : 'T -> unit }
with
    interface ITest with
        member __.Foo (o : obj) = __.Foo(o :?> 'T)

type Test2 = { Foo2 : obj -> unit }

let mkTest (f : 'T -> unit) = { Foo2 = fun o -> f (o :?> 'T) }

let rec factorial n = if n = 0 then 1 else n * factorial(n-1)

let a = { Foo = (factorial >> ignore) } :> ITest
let b = mkTest (factorial >> ignore)

let foo =
    {
        new ITest with
            member __.Foo o = factorial (o :?> int) |> ignore
    }

let test (a : ITest) =
    for i = 1 to 100000000 do
        a.Foo (box 10)

let test2 (b : Test2) =
    for i = 1 to 100000000 do
        b.Foo2 (box 10)

test a
test foo
test2 b

open Nessos.MBrace.Utils
open System.Collections.Generic
open System.Collections.Concurrent

let inps = [1..1000] |> Seq.map(fun i -> i, string i)
let cd = new ConcurrentDictionary<int, string>(inps |> Seq.map (fun (k,v) -> KeyValuePair(k,v)))
let am = Atom.atom (Map.ofSeq inps)


#time
for i = 1 to 10000000 do
    cd.TryFind(i % 1000) |> ignore

for i = 1 to 10000000 do
    am.Value.TryFind(i % 1000) |> ignore


let x = Array.zeroCreate<int option> 10000000

let y = x :> System.Array

x.GetUpperBound(0)

let inline incr (x : ^T ref) = x := !x + LanguagePrimitives.GenericOne

let inline range x y =
    seq {
        let i = ref x
        while !i <= y do
            yield !i
            incr i
    }


open System
open System.IO
open System.Threading


[<RequireQualifiedAccess>]
module Array =

    let internal bufferSize = 256    
    let internal buffer = new ThreadLocal<byte []>(fun () -> Array.zeroCreate<byte> bufferSize)

    let internal write (stream : Stream, array : Array) =
        do stream.Flush()

        let buf = buffer.Value
        let totalBytes = Buffer.ByteLength array

        let d = totalBytes / bufferSize
        let r = totalBytes % bufferSize

        for i = 0 to d - 1 do
            Buffer.BlockCopy(array, i * bufferSize, buf, 0, bufferSize)
            stream.Write(buf, 0, bufferSize)

        if r > 0 then
            Buffer.BlockCopy(array, d * bufferSize, buf, 0, r)
            stream.Write(buf, 0, r)

    let internal read (stream : Stream, array : Array) =
        let buf = buffer.Value
        let inline readBytes (n : int) =
            if stream.Read(buf, 0, n) < n then
                raise <| new EndOfStreamException()
        
        let totalBytes = Buffer.ByteLength array

        let d = totalBytes / bufferSize
        let r = totalBytes % bufferSize

        for i = 0 to d - 1 do
            do readBytes bufferSize
            Buffer.BlockCopy(buf, 0, array, i * bufferSize, bufferSize)

        if r > 0 then
            do readBytes r
            Buffer.BlockCopy(buf, 0, array, d * bufferSize, r)


    let write1D<'T when 'T : struct>(bw : BinaryWriter, xs : 'T []) =
        bw.Write xs.Length
        write(bw.BaseStream, xs)

    let write2D<'T when 'T : struct>(bw : BinaryWriter, xs : 'T [,]) =
        bw.Write(xs.GetLength 0)
        bw.Write(xs.GetLength 1)
        bw.BaseStream.Flush()
        write(bw.BaseStream, xs)

    let write3D<'T when 'T : struct>(bw : BinaryWriter, xs : 'T [,,]) =
        bw.Write(xs.GetLength 0)
        bw.Write(xs.GetLength 1)
        bw.Write(xs.GetLength 2)
        write(bw.BaseStream, xs)

    let write4D<'T when 'T : struct>(bw : BinaryWriter, xs : 'T [,,,]) =
        bw.Write(xs.GetLength 0)
        bw.Write(xs.GetLength 1)
        bw.Write(xs.GetLength 2)
        bw.Write(xs.GetLength 3)
        write(bw.BaseStream, xs)

    let read1D<'T when 'T : struct>(br : BinaryReader) =
        let n = br.ReadInt32()
        let arr = Array.zeroCreate<'T> n
        read(br.BaseStream, arr)
        arr

    let read2D<'T when 'T : struct>(br : BinaryReader) =
        let n1 = br.ReadInt32()
        let n2 = br.ReadInt32()
        let arr = Array2D.zeroCreate<'T> n1 n2
        read(br.BaseStream, arr)
        arr

    let read3D<'T when 'T : struct>(br : BinaryReader) =
        let n1 = br.ReadInt32()
        let n2 = br.ReadInt32()
        let n3 = br.ReadInt32()
        let arr = Array3D.zeroCreate<'T> n1 n2 n3
        read(br.BaseStream, arr)
        arr

    let read4D<'T when 'T : struct>(br : BinaryReader) =
        let n1 = br.ReadInt32()
        let n2 = br.ReadInt32()
        let n3 = br.ReadInt32()
        let n4 = br.ReadInt32()
        let arr = Array4D.zeroCreate<'T> n1 n2 n3 n4
        read(br.BaseStream, arr)
        arr


let mem = new MemoryStream()
let bw = new BinaryWriter(mem)
let br = new BinaryReader(mem)

let x = Array2D.init 10 10 (fun i j -> i * j + 1 |> float)
let z = Array.init 100 (fun i -> float i)

Array.write2D(bw, x)
Array.write1D(bw, z)
mem.Position <- 0L

let x0 = Array.read2D<float>(br)
let z0 = Array.read1D<float>(br)

x.GetLength 0


open System

let x = new IntPtr(1821) :> ISerializable

let si = new SerializationInfo(typeof<IntPtr>, FormatterConverter())

x.GetObjectData(si, new StreamingContext())

let e = si.GetEnumerator()

e.MoveNext()
e.Current