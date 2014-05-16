namespace Nessos.MBrace.Utils.AssemblyCache

    open System
    open System.IO
    open System.Reflection
    open System.Security.Cryptography
    open System.Text

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Reflection

    type AssemblyHash = byte []

    type AssemblyId = { FullName : string ; Hash : AssemblyHash }
    with
        static member TryLoad (id : AssemblyId) : Assembly option =
            match Assembly.TryFind id.FullName with
            | Some a -> Some a
            | None ->
                match AssemblyCache.TryGetImage id with
                | None -> None
                | Some img -> AssemblyImage.Load img |> Some

    and AssemblyImage = { IL : byte [] ; Symbols : byte [] option }
    with
        static member Load (img : AssemblyImage) : Assembly =
            match img.Symbols with
            | None -> Assembly.Load img.IL
            | Some pdb -> Assembly.Load(img.IL, pdb)

        static member Create (path : string) =
            let image = File.ReadAllBytes path
            let symbols =
                let pdbFile = Path.ChangeExtension(path, ".pdb")
                if File.Exists pdbFile then Some (File.ReadAllBytes pdbFile)
                else None

            { IL = image ; Symbols = symbols }
    
    and AssemblyPacket = { Header : AssemblyId ; Image : AssemblyImage option } 
    with 
        member p.IL =  p.Image |> Option.map (fun im -> im.IL)
        member p.Symbols = p.Image |> Option.bind (fun im -> im.Symbols)
        static member OfAssembly (assembly : Assembly, ?includeImage) =
            AssemblyCache.CreatePacket (assembly, ?includeImage = includeImage)

        static member TryLoad (p : AssemblyPacket) =
            match AssemblyId.TryLoad p.Header with
            | Some a -> Some a
            | None when p.Image.IsSome -> Some <| AssemblyImage.Load p.Image.Value
            | None -> None

    and AssemblyCache private () =

        static let algorithm = ref (SHA1Managed.Create() :> HashAlgorithm)
        static let cacheDir = ref <| Path.GetTempPath()

        static let computeHash (assembly : byte []) =
            let length = BitConverter.GetBytes(assembly.Length)
            (!algorithm).ComputeHash(assembly) |> Array.append length

        static let uniqueFilename (hash : AssemblyHash) =
            let fileName = String.Convert.BytesToBase32(hash)
            Path.Combine(!cacheDir, fileName)

        static let (|CacheContains|_|) hash =
            let path = uniqueFilename hash
            let dll = path + ".dll"
            if File.Exists dll then
                let pdb = path + ".pdb"
                if File.Exists pdb then Some(dll, Some pdb)
                else Some(dll, None)
            else None

        static let (|Image|) (dll : string, pdb : string option) =
            let il = File.ReadAllBytes dll
            let symbols = pdb |> Option.map File.ReadAllBytes
            { IL = il ; Symbols = symbols }

        static let save (assembly : AssemblyImage) =
            let hash = computeHash assembly.IL
            let path = uniqueFilename hash
            File.WriteAllBytes(path + ".dll", assembly.IL)
            match assembly.Symbols with
            | None -> ()
            | Some pdb ->
                File.WriteAllBytes(path + ".pdb", pdb)

        static member SetCacheDir (dir : string) =
            if Directory.Exists dir then cacheDir := dir 
            else raise <| new ArgumentException("AssemblyCache: provided cache directory does not exist.")

        static member SetHashingAlgorithm (alg : HashAlgorithm) = algorithm := alg

        static member CreatePacket (assembly : Assembly, ?includeImage) =
            let includeImage = defaultArg includeImage true
            if assembly.IsDynamic then 
                raise <| new ArgumentException("AssemblyCache: cannot read from dynamic assemly!")
            let image = AssemblyImage.Create assembly.Location
            let header = { FullName = assembly.FullName ; Hash = computeHash image.IL }
            { Header = header ; Image = if includeImage then Some image else None }

        static member ComputeHash (assembly : AssemblyImage) = computeHash assembly.IL
        static member ComputeHash (assembly : Assembly) =
            let bytes = File.ReadAllBytes assembly.Location
            computeHash bytes
        static member ComputeHash (path : string) =
            let bytes = File.ReadAllBytes path
            computeHash bytes

        static member TryGetImage (id : AssemblyId) : AssemblyImage option =
            match id.Hash with
            | CacheContains (Image img) -> Some img
            | _ -> None

        static member TryGetPacket (id : AssemblyId) : AssemblyPacket option =
            AssemblyCache.TryGetImage id
            |> Option.map (fun img -> { Header = id ; Image = Some img })

        static member Save (assembly : AssemblyImage) = save assembly
        static member Save (assembly : AssemblyPacket) = 
            if assembly.Image.IsSome then
                save assembly.Image.Value
            else
                raise <| new ArgumentException "AssemblyCache: packet does not contain assembly image"

        static member Contains (hash : AssemblyHash) = match hash with CacheContains _ -> true | _ -> false
        static member Contains (id : AssemblyId) = AssemblyCache.Contains id.Hash

        static member CachePath = !cacheDir


namespace Nessos.MBrace.Utils

    module Assembly =

        open System
        open System.Reflection

        open Nessos.MBrace.Utils.Reflection
        open Nessos.MBrace.Utils.AssemblyCache
        
        // assembly name -> location
        let getAssemblyByName =
            let memo = tryMemoize id Assembly.TryFind
            fun name -> match memo name with None -> null | Some a -> a
                    
        
        let RegisterAssemblyResolutionHandler =
            fun () ->
                AppDomain.CurrentDomain.add_AssemblyResolve 
                <| new ResolveEventHandler (fun _ args -> getAssemblyByName args.Name)
            |> runOnce