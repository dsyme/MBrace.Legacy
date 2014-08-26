namespace Nessos.MBrace.Runtime.Service

    open System
    open System.IO

    open Nessos.UnionArgParser

    open Nessos.MBrace.Utils

    type Configuration =
        | [<Mandatory>][<NoCommandLine>] MBraced_Path of string
    with
        interface IArgParserTemplate with
            member s.Usage =
                match s with
                | MBraced_Path _ -> "sets the mbraced path."

    [<AutoOpen>]
    module internal Config =

        let config = UnionArgParser.Create<Configuration>()

        let exiter = new ConsoleProcessExiter() :> IExiter

        let selfExe = System.Reflection.Assembly.GetEntryAssembly().Location
        let selfPath = Path.GetDirectoryName selfExe

        let resolvePath (path : string) =
            if Path.IsPathRooted path then path
            else Path.Combine(selfPath, path) |> Path.GetFullPath
        
        let parseMBracedPath (path : string) =
            let path = resolvePath path
            if File.Exists path then path
            else failwith "supplied mbraced.exe location not found."