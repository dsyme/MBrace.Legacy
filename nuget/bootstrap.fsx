//  Loads the minimal set of assemblies required to initialize MBrace
//  in an F# interactive session.

#I __SOURCE_DIRECTORY__
#r "tools/Thespian.dll"
#r "tools/Vagrant.dll"
#r "tools/MBrace.Core.dll"
#r "tools/MBrace.Lib.dll"
#r "tools/MBrace.Utils.dll"
#r "tools/MBrace.Store.dll"
#r "tools/MBrace.Runtime.Base.dll"
#r "tools/MBrace.Client.dll"

open System.IO
open Nessos.MBrace.Store
open Nessos.MBrace.Client

// set printers for Client Objects
fsi.AddPrinter prettyPrintStore

// set local MBrace executable location
MBraceSettings.MBracedExecutablePath <- System.IO.Path.Combine(__SOURCE_DIRECTORY__, "./tools/mbraced.exe")