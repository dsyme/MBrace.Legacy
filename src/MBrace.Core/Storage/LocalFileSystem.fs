namespace Nessos.MBrace.Core

    open System
    open System.IO
    open System.Security.AccessControl

    open Nessos.MBrace
    open Nessos.MBrace.Core

    type LocalFileSystem(location : string) =

        static let rec retry (interval : TimeSpan) retries f =
            let result = 
                try Some <| f () 
                with 
                | _ when retries = 0 -> reraise()
                | _ -> None

            match result with
            | Some r -> r
            | None -> System.Threading.Thread.Sleep interval ; retry interval (retries - 1) f

        let location = Path.GetFullPath location
        do
            fun () ->
                if Directory.Exists location then ()
                else
                    Directory.CreateDirectory location |> ignore

            |> retry (TimeSpan.FromMilliseconds 100.) 3

        let uuid =
            let uri = Uri(location)
            if uri.IsUnc then uri.ToString()
            else sprintf "file://%s/%s" (System.Net.Dns.GetHostName()) uri.AbsolutePath

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
            | None -> return! trap path mode access share
        }

        interface ICloudFileSystem with

            member __.Name = "FileSystem"
            member __.UUID = uuid

            member __.GetContainer(path : string) = Path.GetDirectoryName path

            member __.GetAllFiles(container : string) : Async<string []> =
                async {
                    let path = Path.Combine(location, container)
                    return 
                        Directory.GetFiles(path)
                        |> Array.map (fun file -> Path.Combine(container, Path.GetFileName(file)))
                }

            member __.GetAllContainers () : Async<string []> =
                async {
                    return 
                        Directory.GetDirectories(location)
                        |> Array.map Path.GetFileName
                }

            member __.FileExists(path : string) : Async<bool> = 
                async {
                    let fullPath = Path.Combine(location, path)
                    return File.Exists fullPath
                }

            member __.ContainerExists(container : string) : Async<bool> = 
                async {
                    let fullPath = Path.Combine(location, container)
                    return Directory.Exists(fullPath)
                }

            member __.CreateImmutable(path : string, writer : Stream -> Async<unit>, asFile : bool) = 
                async {
                    let fullPath = Path.Combine(location, path)
                    let container = Path.GetDirectoryName fullPath

                    if not <| Directory.Exists container then
                        Directory.CreateDirectory container |> ignore

                    use fs = new FileStream(fullPath, FileMode.Create, FileAccess.Write, FileShare.None)

                    return! writer fs
                }
             
            member __.ReadImmutable(path : string, reader : Stream -> Async<'T>) : Async<'T> = 
                async {
                    let fullPath = Path.Combine(location, path)

                    use fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.Read)

                    return! reader fs
                }

            member __.CreateImmutable(path : string, source : Stream, asFile : bool) =
                let copyAsync (target : Stream) =
                    let copyTask = source.CopyToAsync(target).ContinueWith ignore
                    Async.AwaitTask copyTask

                (__ :> ICloudFileSystem).CreateImmutable(path, copyAsync, asFile)

            member __.ReadImmutable(path : string, target : Stream) = 
                let copyAsync (source : Stream) =
                    let copyTask = source.CopyToAsync(target).ContinueWith ignore
                    Async.AwaitTask copyTask

                (__ :> ICloudFileSystem).ReadImmutable(path, copyAsync)

            member __.DeleteFile(path : string) =
                async {
                    let fullPath = Path.Combine(location, path)
                    File.Delete(path)
                }

            member __.DeleteContainer(path : string) =
                async {
                    let fullPath = Path.Combine(location, path)
                    Directory.Delete(path, true)
                }

            member __.CreateMutable(path : string, writer : Stream -> Async<unit>) : Async<Tag> = 
                async {
                    let fullPath = Path.Combine(location, path)
                    let container = Path.GetDirectoryName fullPath

                    if not <| Directory.Exists container then
                        Directory.CreateDirectory container |> ignore

                    use fs = new FileStream(fullPath, FileMode.Create, FileAccess.Write, FileShare.None) :> Stream
                    let tag = writeTag fs
                    do! writer fs
                    fs.Flush()
                    return tag
                }

            member __.ReadMutable(path : string, reader : Stream -> Async<'T>) : Async<Tag * 'T> = 
                async {
                    let fullPath = Path.Combine(location, path)
                    let! fs = trap fullPath FileMode.Open FileAccess.Read FileShare.Read
                    let tag = readTag fs
                    let! result = reader fs
                    return tag, result
                }

            member __.TryUpdateMutable(path : string, writer : Stream -> Async<unit>, oldTag) =
                async {
                    let fullPath = Path.Combine(location, path)
                
                    let! fs = trap fullPath FileMode.Open FileAccess.ReadWrite FileShare.None
                
                    let tag = readTag fs

                    try
                        if tag = oldTag then
                            fs.Position <- 0L
                            fs.SetLength(0L)
                            let newTag = writeTag fs
                            do! writer fs

                            fs.Position <- 0L
                            
                            return true, newTag
                        else
                            return false, oldTag
                    finally
                        fs.Flush()
                        fs.Dispose()
                }

            member __.ForceUpdateMutable(path : string, writer : Stream -> Async<unit>) : Async<Tag> =
                async {
                    let fullPath = Path.Combine(location, path)
                
                    use! fs = trap fullPath FileMode.Open FileAccess.Write FileShare.None
                    fs.Position <- 0L
                    fs.SetLength(0L)
                    let newTag = writeTag fs
                    do! writer fs
                    fs.Flush()
                    return newTag
                }

    type LocalFileSystemFactory () =
        interface ICloudFileSystemFactory with
            member this.CreateFromConnectionString (path : string) = 
                LocalFileSystem(path) :> ICloudFileSystem