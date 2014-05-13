#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Client.dll"

open Nessos.MBrace
open Nessos.MBrace.Client

let runtime = MBrace.InitLocal 3

runtime.Ping()

runtime.Nodes

type CloudList<'T> = Nil | Cons of 'T * ICloudRef<CloudList<'T>>

[<Cloud>]
let rec testBuildCloudList a = 
    cloud {
        if a = 0 then 
            return! newRef Nil
        else
            let! tail = testBuildCloudList (a - 1)
            return! newRef <| Cons (1, tail)
    }

[<Cloud>]
let rec testReduceCloudList (cloudRefList : ICloudRef<CloudList<_>>) = 
    cloud {
        let cloudList = cloudRefList.Value
        match cloudList with
        | Cons (v, cloudRefList') ->
            let! result = testReduceCloudList cloudRefList'
            return 1 + result
        | Nil -> return 0
    }

let c = runtime.Run <@ cloud { return! testBuildCloudList 2 } @>

c.Value

runtime.Run <@ cloud { return! testReduceCloudList c } @>