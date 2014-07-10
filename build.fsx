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
        yield! includeFile true <| Path.ChangeExtension(assembly, "pdb")
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
            Dependencies = 
                [
                    "FsPickler", "0.9.6"
                    "FsPickler.Json", "0.9.6"
                    "Thespian", "0.0.7"
                    "UnionArgParser", "0.7.0"
                    "Unquote", "2.2.2"
                    "Vagrant", "0.2.1"
                    "MBrace.Core", RequireExactly nugetVersion
                ]
            Files =
                [   
                    yield! addAssembly @"lib\net45" @"..\bin\MBrace.Utils.dll"
                    yield! addAssembly @"lib\net45" @"..\bin\MBrace.Runtime.Base.dll"
                    yield! addAssembly @"lib\net45" @"..\bin\MBrace.Runtime.Cluster.dll"
                    yield! addAssembly @"lib\net45" @"..\bin\MBrace.Client.dll"
                    yield! addAssembly @"lib\net45" @"..\bin\mbraced.exe"
                    yield! addAssembly @"lib\net45" @"..\bin\mbrace.worker.exe"
                    yield! addAssembly @"lib\net45" @"..\bin\mbracesvc.exe"
                    yield! addAssembly @"lib\net45" @"..\bin\mbracectl.exe"
                    
                    //yield  addFile "tools" "init.ps1"     
                    yield  addFile "tools" "install.ps1"  
                    //yield  addFile "tools" "uninstall.ps1"
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
  ==> "NuGet -- MBrace.Runtime"
  ==> "Release"

// start build
RunTargetOrDefault "Default"