namespace Nessos.MBrace.Store

    open System
    open System.IO
    open System.Security.AccessControl
    open System.Runtime.CompilerServices

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry

    /// An ICloudStore implementation that uses the local filesystem as a backend.
    type FileSystemStore(path : string, ?name) =
        let path = Path.GetFullPath path

        do if not <| Directory.Exists path then
            retry (RetryPolicy.Retry(2, 0.5<sec>)) (fun () -> Directory.CreateDirectory path |> ignore)

        let name = defaultArg name "FileSystem"

        let fullPath =
            let uri = Uri(path)
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
            let fs = try Some(new FileStream(path, mode, access, share))
                     with :? IOException as e when File.Exists(path) -> None
            match fs with
            | Some fs -> return fs :> _
            | None -> return! trap path mode access share
        }

        interface ICloudStore with
            override self.Name = name
            override self.EndpointId = fullPath

            override self.CreateImmutable(folder : string, file : string, serialize : Stream -> Async<unit>, asFile : bool) =
                async {
                    let path = Path.Combine(path, folder)
                    if not <| Directory.Exists(path) then
                        Directory.CreateDirectory(path) |> ignore


                    use fs = new FileStream(Path.Combine(path, file), FileMode.Create, FileAccess.Write, FileShare.None)
                    return! serialize fs
                }
             
            override self.ReadImmutable(folder : string, file : string) : Async<Stream> = 
                async {
                    let path = Path.Combine(path, folder, file)
                    return new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read) :> _
                }
                  
            override self.GetAllFiles(folder : string) : Async<string []> =
                async {
                    let path = Path.Combine(path, folder)
                    return Directory.GetFiles(path) |> Array.map Path.GetFileName
                }

            override self.GetAllContainers () : Async<string []> =
                async {
                    return Directory.GetDirectories(path) |> Array.map Path.GetFileName
                }

            override self.Exists(folder : string, file : string) : Async<bool> = 
                async {
                    let path = Path.Combine(Path.Combine(path, folder), file)
                    return File.Exists(path)
                }

            override self.ContainerExists(folder : string) : Async<bool> = 
                async {
                    let path = Path.Combine(path, folder)
                    return Directory.Exists(path)
                }

            override self.CopyTo(folder : string, file : string, target : Stream) = 
                async {
                    use! source = (self :> ICloudStore).ReadImmutable(folder,file)
                    do! source.CopyToAsync(target)
                }

            override self.CopyFrom(folder : string, file : string, source : Stream, asFile : bool) =
                async {
                    let path = Path.Combine(path, folder)
                    if not <| Directory.Exists(path) then
                        Directory.CreateDirectory(path) |> ignore
                    use target = new FileStream(Path.Combine(path,file), FileMode.CreateNew, FileAccess.Write) :> Stream
                    do! source.CopyToAsync(target)
                }

            override self.Delete(folder : string, file : string) =
                async {
                    let path = Path.Combine(Path.Combine(path, folder), file)
                    File.Delete(path)
                }

            override self.DeleteContainer(folder : string) =
                async {
                    let path = Path.Combine(path, folder)
                    Directory.Delete(path, true)
                }

            override self.TryUpdateMutable(folder, file, serialize, oldTag) =
                async {
                    let path = Path.Combine(path, folder, file)
                
                    let! fs = trap path FileMode.Open FileAccess.ReadWrite FileShare.None
                
                    let tag = readTag fs

                    try
                        if tag = oldTag then
                            fs.Position <- 0L
                            fs.SetLength(0L)
                            let newTag = writeTag fs
                            do! serialize fs

                            fs.Position <- 0L
                            
                            return true, newTag
                        else
                            return false, oldTag
                    finally
                        fs.Flush()
                        fs.Dispose()
                }

            override self.ForceUpdateMutable(folder, file, serialize) : Async<Tag> =
                async {
                    let path = Path.Combine(path, folder, file)
                
                    use! fs = trap path FileMode.Open FileAccess.Write FileShare.None
                    fs.Position <- 0L
                    fs.SetLength(0L)
                    let newTag = writeTag fs
                    do! serialize fs
                    fs.Flush()
                    return newTag
                }

            override self.CreateMutable(folder : string, file : string, serialize : Stream -> Async<unit>) : Async<Tag> = 
                async {
                    let path = Path.Combine(path, folder)
                    if not <| Directory.Exists(path) then
                        Directory.CreateDirectory(path) |> ignore
                    use fs = new FileStream(Path.Combine(path, file), FileMode.Create, FileAccess.Write, FileShare.None) :> Stream
                    let tag = writeTag fs
                    do! serialize fs
                    fs.Flush()
                    return tag
                }

            override self.ReadMutable(folder : string, file : string) : Async<Stream * Tag> = 
                async {
                    let path = Path.Combine(path, folder, file)
                    let! fs = trap path FileMode.Open FileAccess.Read FileShare.Read
                    let tag = readTag fs
                    return fs, tag
                }

    type FileSystemStoreFactory () =
        interface ICloudStoreFactory with
            member this.CreateStoreFromConnectionString (path : string) = 
                FileSystemStore(path) :> ICloudStore