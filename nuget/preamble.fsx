//  Loads the minimal set of assemblies required to initialize an MBrace session

#I __SOURCE_DIRECTORY__
#r "tools/Thespian.dll"
#r "tools/Vagrant.dll"
#r "tools/MBrace.Core.dll"
#r "tools/MBrace.Lib.dll"
#r "tools/MBrace.Store.dll"
#r "tools/MBrace.Runtime.Base.dll"
#r "tools/MBrace.Client.dll"

open Nessos.MBrace.Client

MBraceSettings.MBracedExecutablePath <- System.IO.Path.Combine(__SOURCE_DIRECTORY__, "./tools/mbraced.exe")