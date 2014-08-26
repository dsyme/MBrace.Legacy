namespace Nessos.MBrace.Runtime.Daemon

    open System

    open Nessos.UnionArgParser

    open Nessos.MBrace.Utils

    module Configuration =

        type MBracedConfig =
            | [<NoAppSettings>][<AltCommandLine("-V")>] Version
            | Hostname of string
            | Listen_Ips of string
            | [<Mandatory>] Primary_Port of int
            | [<ParseCSV>][<CustomAppSettings("worker ports")>] Worker_Port of int
            | Worker_Port_Range of int * int
            | Working_Directory of string
            | [<NoAppSettings>] Use_Temp_WorkDir
            | [<Mandatory>] Log_Level of int
            | Log_File of string
            | [<Hidden>][<NoAppSettings>] Parent_Receiver_Id of int * string // pid * name
            | Debug
            | [<NoAppSettings>] Detach
            | [<NoAppSettings>] Spawn_Window
            | [<Mandatory>] Permissions of int
            | [<NoCommandLine>] MBrace_ProcessDomain_Executable of string
        with
            interface IArgParserTemplate with
                member s.Usage =
                    match s with
                    | Version -> "display version number and exit."
                    | Hostname _ -> "hostname used by the daemon."
                    | Listen_Ips _ -> "IP addresses the daemon will listen on. Separate with commas. Use empty string for all interfaces."
                    | Primary_Port _ -> "specifies the TCP port used by the daemon."
                    | Worker_Port _ -> "available port for use by mbrace workers."
                    | Worker_Port_Range _ -> "available port range for use by mbrace workers."
                    | Working_Directory _ -> "specifies the working directory."
                    | Use_Temp_WorkDir -> "executes in a temp working directory. Useful for local multi-process setups."
                    | Log_Level _ -> "specifies the log level (Info = 0| Warning = 1| Error = 2)."
                    | Log_File _ -> "specifies a log file for the daemon."
                    | Detach -> "detach daemon to background process."
                    | Spawn_Window -> "detach daemon to windowed process."
                    | Parent_Receiver_Id _ -> "specify parent receiver id."
                    | Debug -> "enables debug mode."
                    | Permissions _ -> "override the default runtime permissions (All)."
                    | MBrace_ProcessDomain_Executable _ -> "sets the location of the mbraces.process executable."


        let mbracedParser = UnionArgParser.Create<MBracedConfig>("USAGE: mbraced.exe [options]")           

        // Node spawning infrastructure

        open Nessos.Thespian
        open Nessos.MBrace.Runtime

        type MBracedSpawningServer =
            | StartupError of int * string option // errorcode * optional message
            | StartupSuccessful of Uri * IReplyChannel<string option> // listener uri * R<message displayed by logFile>