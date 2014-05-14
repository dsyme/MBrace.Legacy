module Nessos.MBrace.Runtime.Daemon.Controller.MainModule

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Client
    open System.Diagnostics

    open Nessos.MBrace.Runtime.Daemon.Controller.Implementations

    [<EntryPoint>]
    let main _ =
        
        if runsOnMono then exiter.Exit("mono not supported... yet!", 1)

        // arg parser

        let results = argParser.Parse(errorHandler = plugExiter exiter)

        let sessionFile = results.GetResult(<@ Session_File @>, defaultSessionFile) |> resolveRelativePath
        let mbracedExe = results.PostProcessResult(<@ MBraced_Executable @>, parseMBracedExecutable)

        let startMode = results.Contains <@ Start @>
        let stopMode = results.Contains <@ Stop @>
        let statusMode = results.Contains <@ Status @>

        if not <| List.exists id [startMode ; stopMode ; statusMode ] then results.Raise("mbracectl: need to specify an operation mode.")

        let nodes = results.TryPostProcessResult(<@ Nodes @>, parseNodes startMode) |> fun n -> defaultArg n 1
        let spawnWindow = results.Contains <@ Spawn_Window @>
        let boot = results.Contains <@ Boot @>

        if boot && nodes < 3 then results.Raise("error: need at least 3 nodes to boot.", 2, showUsage = true)

        // init
        
        do lockSession sessionFile

        let state = tryParseSession sessionFile

        // parser guarantees that the 3 booleans are mutually exclusive
        if startMode then
            let spawnOptions = if nodes = 1 then None else Some(nodes, boot)    
            let session', nodes =
                match state with
                | None -> 
                    printf "starting {m}brace session... "
                    MBraceSettings.MBracedExecutablePath <- mbracedExe
                    let r = spawnNodes spawnWindow spawnOptions
                    printfn "done"
                    r
                | Some _ -> exiter.Exit("mbracectl: an mbrace session is already active.", 0)

            do saveSession sessionFile session'
            prettyPrint session' nodes

        elif stopMode then
            match state with
            | None -> exiter.Exit("mbracectl: no active mbrace session found.")
            | Some(session, nodes) ->
                try
                    printf "stopping {m}brace session... "
                    // TODO: make cooperative
                    for n in nodes do n.Kill ()
                    printfn "done"
                with e -> exiter.Exit(sprintf "\nmbracectl error: %s" e.Message, id = 3)

        else
            match state with
            | None -> printfn "no active {m}brace session."
            | Some (s, nodes) -> prettyPrint s nodes

        // use exiter to trigger exit events
        exiter.Exit(id = 0)