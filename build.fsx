// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I "packages/FAKE/tools"
#r "packages/FAKE/tools/FakeLib.dll"

open Fake
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper

open System
open System.IO


let project = "MBrace"
let authors = [ "Jan Dzik" ; "Nick Palladinos" ; "Kostas Rontogiannis" ; "Eirik Tsarpalis" ]

let description = """
    An open source framework for large-scale distributed computation and data processing written in F#.
"""

let tags = "F# cloud mapreduce distributed"

// --------------------------------------------------------------------------------------
// Read release notes & version info from RELEASE_NOTES.md
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let release = parseReleaseNotes (IO.File.ReadAllLines "RELEASE_NOTES.md") 
let nugetVersion = release.NugetVersion

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
    let attributes =
        [ 
            Attribute.Title project
            Attribute.Product project
            Attribute.Company "Nessos Information Technologies"
            Attribute.Copyright "\169 Nessos Information Technologies."
            Attribute.Trademark "{m}brace"
            Attribute.Version release.AssemblyVersion
            Attribute.FileVersion release.AssemblyVersion
        ]

    !! "./src/**/AssemblyInfo.fs"
    |> Seq.iter (fun info -> CreateFSharpAssemblyInfo info attributes)
)


// --------------------------------------------------------------------------------------
// Clean and restore packages


Target "RestorePackages" (fun _ ->
    !! "./**/packages.config"
    |> Seq.iter (RestorePackage (fun p -> { p with ToolPath = "./.nuget/NuGet.exe" }))
)

Target "Clean" (fun _ ->
    CleanDirs (!! "**/bin/Release/")
    CleanDir "bin/"
)

// --------------------------------------------------------------------------------------
// Build


let configuration = environVarOrDefault "Configuration" "Release"

Target "Build" (fun _ ->
    // Build the rest of the project
    { BaseDirectory = __SOURCE_DIRECTORY__
      Includes = [ project + ".sln" ]
      Excludes = [] } 
    |> MSBuild "" "Build" ["Configuration", configuration]
    |> Log "AppBuild-Output: "
)

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete


let testAssemblies = 
    [
        "bin/MBrace.Runtime.Tests.dll"
        "bin/MBrace.Shell.Tests.dll"
        "bin/MBrace.Store.Tests.dll"
    ]

let excludedStoresCategory = "CustomStores"

Target "RunTestsExcludingCustomStores" (fun _ ->
    let nunitVersion = GetPackageVersion "packages" "NUnit.Runners"
    let nunitPath = sprintf "packages/NUnit.Runners.%s/tools" nunitVersion
    ActivateFinalTarget "CloseTestRunner"
        
    testAssemblies
    |> NUnit (fun p -> 
        { p with
            Framework = "v4.0.30319"
            ToolPath = nunitPath
            DisableShadowCopy = true
            TimeOut = TimeSpan.FromMinutes 20.
            ExcludeCategory = excludedStoresCategory
            OutputFile = "TestResults.xml" })
)

Target "RunTests" (fun _ ->
    let nunitVersion = GetPackageVersion "packages" "NUnit.Runners"
    let nunitPath = sprintf "packages/NUnit.Runners.%s/tools" nunitVersion
    ActivateFinalTarget "CloseTestRunner"
        
    testAssemblies
    |> NUnit (fun p -> 
        { p with
            Framework = "v4.0.30319"
            ToolPath = nunitPath
            DisableShadowCopy = true
            TimeOut = TimeSpan.FromMinutes 20.
            OutputFile = "TestResults.xml" })
)

FinalTarget "CloseTestRunner" (fun _ ->  
    ProcessHelper.killProcess "nunit-agent.exe"
)

// Nuget packages

let addFile (target : string) (file : string) =
    if File.Exists (Path.Combine("nuget", file)) then (file, Some target, None)
    else raise <| new FileNotFoundException(file)

let addAssembly (target : string) assembly =
    let includeFile force file =
        let file = file
        if File.Exists (Path.Combine("nuget", file)) then [(file, Some target, None)]
        elif force then raise <| new FileNotFoundException(file)
        else []

    seq {
        yield! includeFile true assembly
        yield! includeFile false <| Path.ChangeExtension(assembly, "pdb")
        yield! includeFile false <| Path.ChangeExtension(assembly, "xml")
        yield! includeFile false <| assembly + ".config"
    }

Target "NuGet -- MBrace.Core" (fun _ ->
    let nugetPath = ".nuget/NuGet.exe"
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = "MBrace.Core"
            Summary = "Core libraries for the MBrace programming model."
            Description = description
            Version = nugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            ToolPath = nugetPath
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Publish = hasBuildParam "nugetkey"
            Dependencies = []
            Files =
                [
                    yield! addAssembly @"lib\net45" @"..\bin\MBrace.Core.dll"
                    yield! addAssembly @"lib\net45" @"..\bin\MBrace.Lib.dll"
                ]
        })
        ("nuget/MBrace.nuspec")
)

Target "MBraceIntro" (fun _ ->
    let newFile = 
        File.ReadLines("./nuget/mbrace-intro.fsx")
        |> Seq.map (fun line -> 
            if line.StartsWith """#load "../packages/MBrace.Runtime""" then 
                sprintf """#load "../packages/MBrace.Runtime.%s/preamble.fsx" """ release.NugetVersion
            else line)
        |> Seq.toArray
    File.WriteAllLines("./nuget/mbrace-intro.fsx", newFile)
)

Target "NuGet -- MBrace.Runtime" (fun _ ->
    let nugetPath = ".nuget/NuGet.exe"
    NuGet (fun p -> 
        { p with   
            Authors = authors
            Project = "MBrace.Runtime"
            Summary = "Libraries and tools for setting up an MBrace runtime."
            Description = description
            Version = nugetVersion
            ReleaseNotes = String.concat " " release.Notes
            Tags = tags
            OutputPath = "bin"
            ToolPath = nugetPath
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Publish = hasBuildParam "nugetkey"
            Files =
                [  
                    yield! addAssembly @"tools" @"..\bin\Newtonsoft.Json.dll"
                    yield! addAssembly @"tools" @"..\bin\FSharp.Compiler.Service.dll"
                    yield! addAssembly @"tools" @"..\bin\Mono.Cecil.dll"
                    yield! addAssembly @"tools" @"..\bin\Mono.Cecil.Mdb.dll"
                    yield! addAssembly @"tools" @"..\bin\Mono.Cecil.Pdb.dll"
                    yield! addAssembly @"tools" @"..\bin\Mono.Cecil.Rocks.dll"
                    yield! addAssembly @"tools" @"..\bin\FsPickler.dll"
                    yield! addAssembly @"tools" @"..\bin\FsPickler.Json.dll"
                    yield! addAssembly @"tools" @"..\bin\Thespian.dll"
                    yield! addAssembly @"tools" @"..\bin\Thespian.Cluster.dll"
                    yield! addAssembly @"tools" @"..\bin\UnionArgParser.dll"
                    yield! addAssembly @"tools" @"..\bin\Unquote.dll"
                    yield! addAssembly @"tools" @"..\bin\Vagrant.dll"
                    yield! addAssembly @"tools" @"..\bin\Vagrant.Cecil.dll"
                    yield! addAssembly @"tools" @"..\bin\MBrace.Core.dll"
                    yield! addAssembly @"tools" @"..\bin\MBrace.Lib.dll"
                    yield! addAssembly @"tools" @"..\bin\MBrace.Utils.dll"
                    yield! addAssembly @"tools" @"..\bin\MBrace.Runtime.Base.dll"
                    yield! addAssembly @"tools" @"..\bin\MBrace.Runtime.Cluster.dll"
                    yield! addAssembly @"tools" @"..\bin\MBrace.Client.dll"
                    yield! addAssembly @"tools" @"..\bin\MBrace.Store.dll"
                    yield! addAssembly @"tools" @"..\bin\mbraced.exe"
                    yield! addAssembly @"tools" @"..\bin\mbrace.worker.exe"
                    yield! addAssembly @"tools" @"..\bin\mbracesvc.exe"
                    yield! addAssembly @"tools" @"..\bin\mbracectl.exe"
                    yield  addFile     @"tools" @"..\lib\fsharp\FSharp.Core.dll"
                    yield  addFile     @"tools" @"..\lib\fsharp\FSharp.Core.xml"
                    yield  addFile     @"tools" @"..\lib\fsharp\FSharp.Core.sigdata"
                    yield  addFile     @"tools" @"..\lib\fsharp\FSharp.Core.optdata"
                    yield  addFile     @"" @"preamble.fsx"
                    yield  addFile     @"tools" @"install.ps1"  
                    yield  addFile     @"content" "mbrace-intro.fsx"
                ]                   
        })                          
        ("nuget/MBrace.nuspec")
)

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override
Target "Default" DoNothing
Target "DefaultWithStores" DoNothing
Target "Release" DoNothing
Target "PrepareRelease" DoNothing

"Clean"
  ==> "RestorePackages"
  ==> "AssemblyInfo"
  ==> "Build"
//  ==> "RunTestsExcludingCustomStores"
  ==> "Default"

"Clean"
  ==> "RestorePackages"
  ==> "AssemblyInfo"
  ==> "Build"
  ==> "RunTests"
  ==> "DefaultWithStores"

"Default"
  ==> "PrepareRelease"
  ==> "NuGet -- MBrace.Core"
  ==> "MBraceIntro"
  ==> "NuGet -- MBrace.Runtime"
  ==> "Release"

// start build
RunTargetOrDefault "Default"