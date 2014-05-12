#r "../../bin/FsPickler.dll"
#r "../../bin/Mono.Cecil.dll"
#r "../../bin/Vagrant.Cecil.dll"
#r "../../bin/Vagrant.dll"
#r "../../bin/Thespian.dll"
#r "../../bin/MBrace.Core.dll"
#r "../../bin/MBrace.Utils.dll"
#r "../../bin/MBrace.Runtime.Base.dll"
#r "../../bin/MBrace.Client.dll"

open Nessos.MBrace
open Nessos.MBrace.Client

MBraceSettings.MBracedExecutablePath <- __SOURCE_DIRECTORY__ + "/../../bin/mbraced.exe"

let hello =
    cloud {
        return! CloudRef.New 42
    }

let t = MBrace.RunLocal hello
t.Value


let runtime = MBrace.InitLocal 3

runtime.Ping()

runtime.Nodes

runtime.Run <@ cloud { return 42 } @>