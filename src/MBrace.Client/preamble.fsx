//  Loads the minimal set of assemblies required to initialize an MBrace session
#I "../../bin/"

#r "Thespian.dll"
#r "Vagrant.dll"
#r "MBrace.Core.dll"
#r "MBrace.Lib.dll"
#r "MBrace.Store.dll"
#r "MBrace.Runtime.Base.dll"
#r "MBrace.Client.dll"

open System.IO
open Nessos.MBrace.Client

MBraceSettings.MBracedExecutablePath <- Path.Combine(__SOURCE_DIRECTORY__, "../../bin/mbraced.exe")