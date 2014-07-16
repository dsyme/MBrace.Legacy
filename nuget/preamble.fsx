//  Loads the minimal set of assemblies required to initialize an MBrace session
#I "./bin/Debug/" // EDIT this according to your build path

#r "Thespian.dll"
#r "Vagrant.dll"
#r "MBrace.Core.dll"
#r "MBrace.Lib.dll"
#r "MBrace.Store.dll"
#r "MBrace.Runtime.Base.dll"
#r "MBrace.Client.dll"

open System.IO
open Nessos.MBrace.Client

MBraceSettings.MBracedExecutablePath <- Path.Combine(__SOURCE_DIRECTORY__, "./bin/Debug/mbraced.exe")