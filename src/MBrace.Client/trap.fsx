//open System
//open System.IO
//open System.Security.AccessControl

//let rec trap path (mode : FileMode) (access : FileAccess) (share : FileShare) : Async<Stream> = async {
////    let fs = try Some(new FileStream(path, mode, access, share))
////                with (think) IOException as e when File.Exists(path) -> None
////            
////    match fs with
////    | Some fs -> return fs :D _
////    | None -> return! trap path mode access share
//    //return new FileStream(path, mode, FileSystemRights.Modify, share, 4096, FileOptions.None) :> _
//    return File.Open(path, mode, access, share) :> _
//}
//
//let trapsync path mode access share = File.Open(path, mode, access, share)
//
//let createLockFile path =
//    try 
//        use f = File.Open(path, FileMode.CreateNew)
//        true
//    with :? IOException -> false
//
//let lockFile path = async {
//    while not (createLockFile path) do
//        do! Async.Sleep 100
//}
//
//let unlockFile path = File.Delete path
//
//let foo i =
//    async {
//        let fs = ref null
//        try
//            try
//                do! lockFile @"C:\Users\nessos\Desktop\foo.txt.lock"
//                let! xs = trap @"C:\Users\nessos\Desktop\foo.txt" FileMode.Open FileAccess.Read FileShare.None
//                //let xs = trapsync @"C:\Users\nessos\Desktop\foo.txt" FileMode.Open FileAccess.Read FileShare.None
//                fs := xs
//                fs.Value.ReadByte() |> ignore
//                //fs.Value.WriteByte(byte i)
//                return true
//            with _ -> 
//                return false
//        finally
//                if !fs = null then
//                    ()
//                else
//                    fs.Value.Flush()
//                    fs.Value.Dispose()
//                    unlockFile @"C:\Users\nessos\Desktop\foo.txt.lock"
//    }

open System
open System.IO
open System.Security.AccessControl

type Tag = string

let path = @"C:\Users\nessos\Desktop\foo"

let readTag (s : Stream) : Tag =
    let br = new BinaryReader(s)
    br.ReadString()

let writeTag (stream : Stream) =
    let bw = new BinaryWriter(stream)
    let tag = Guid.NewGuid().ToString("N")
    bw.Write(tag)
    tag

let rec trap path (mode : FileMode) (access : FileAccess) (share : FileShare) : Async<Stream> = async {
    //let fs = try Some(new FileStream(path, mode, FileSystemRights.Modify, share, 4096, FileOptions.None))
    let fs = try Some(new FileStream(path, mode, access, share))
                with :? IOException as e when File.Exists(path) -> None
    match fs with
    | Some fs -> return fs :> _
    | None -> printfn "RACE DETECTED"; return! trap path mode access share
}

let create path =
    use s = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None)

    writeTag s

let update path oldTag = 
    async {
        //let path = Path.Combine(path, folder, file)
                
        let! fs = trap path FileMode.Open FileAccess.ReadWrite FileShare.None
                
        let tag = readTag fs

        try
            if tag = oldTag then
                fs.Position <- 0L
                fs.SetLength(0L)
                let newTag = writeTag fs
                //do! serialize fs

                fs.Position <- 0L

                return true, newTag
            else
                return false, oldTag
        finally
            fs.Flush()
            fs.Dispose()
    }

let initialTag = create path

let currentTags = [| initialTag; initialTag |]

let updateTag i = async {
    let! isUpdated, newTag = update path currentTags.[i]
    currentTags.[i] <- newTag
    return isUpdated
}

for i = 0 to 1000 do
    Async.Parallel [| updateTag 0; updateTag 1 |]
    |> Async.RunSynchronously
    |> fun [|a;b|] -> if a = b then printfn "%A" (a,b)
    





    let fs = 
        trap @"C:\Users\nessos\Desktop\foo.txt" FileMode.Open FileAccess.ReadWrite FileShare.None
        |> Async.RunSynchronously
    printfn "Value : %A" <| fs.ReadByte()
    fs.Dispose()




