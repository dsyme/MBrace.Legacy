#I "bin/Debug"
#r "bin/debug/MBrace.Core.dll"
#r "bin/debug/MBrace.Runtime.Base.dll"
#r "bin/debug/MBrace.Client.dll"

open Nessos.MBrace
open Nessos.MBrace.Client

MBraceSettings.MBracedExecutablePath <- __SOURCE_DIRECTORY__ + "/../MBrace.Daemon/bin/Debug/mbraced.exe"

let nodes = MBraceNode.SpawnMultiple(3)