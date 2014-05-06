namespace Nessos.MBrace.Store
    open System
    open System.IO
    open System.Collections.Generic
    open System.Collections.Concurrent

    [<Sealed;AbstractClass>]
    type private InMemoryRegistry private () =
        static let inmem = new Dictionary<Folder * Nessos.MBrace.Store.File, byte [] * Tag>()
        static let guid  = Guid.NewGuid().ToString()
        static let sync  = new Object()
        
        static member Registry  with get () = inmem
        static member Guid      with get () = guid
        static member Sync      with get () = sync

    type InMemoryStore() =

        let inMem = InMemoryRegistry.Registry
        let newTag () = Guid.NewGuid().ToString()
        let tag = Guid.Empty.ToString()

        interface IStore with
            override self.Name = "InMem"
            override self.UUID = InMemoryRegistry.Guid

            override self.Create(folder : string, file : string, serialize : Stream -> Async<unit>, ?asFile : bool) = 
                async {
                    ignore asFile 
                    use ms = new MemoryStream()
                    do! serialize(ms) 
                    inMem.Add((folder, file), (ms.ToArray(), tag)) 
                }
             
            override self.Read(folder : string, file : string) : Async<Stream> = 
                async {
                    let array = fst inMem.[folder, file]
                    return new MemoryStream(array) :> _
                }
                  
            override self.GetFiles(folder : string) : Async<Nessos.MBrace.Store.File []> =
                async {
                    let keys = inMem.Keys |> Seq.filter(fun kv -> fst kv = folder) |> Seq.map snd |> Seq.toArray
                    return keys
                }

            override self.GetFolders () : Async<Nessos.MBrace.Store.Folder []> =
                async {
                    let keys = inMem.Keys |> Seq.map fst |> Seq.distinct |> Seq.toArray
                    return keys
                }

            override self.Exists(folder : string, file : string) : Async<bool> = 
                async {
                    return inMem.ContainsKey(folder, file)
                }

            override self.Exists(folder : string) : Async<bool> = 
                async {
                    return inMem.Keys |> Seq.exists (fun (f, _) -> f = folder)
                }

            override self.CopyTo(folder : string, file : string, target : Stream) = 
                async {
                    use! source = (self :> IStore).Read(folder,file)
                    source.CopyTo(target)
                }

            override self.CopyFrom(folder : string, file : string, source : Stream, ?asFile : bool) =
                async {
                    ignore asFile
                    do! (self :> IStore).Create(folder, file, fun target -> async { return source.CopyTo(target) })
                }

            override self.Delete(folder : string, file : string) =
                async { 
                    inMem.Remove(folder, file) |> ignore
                }

            override self.Delete(folder : string) =
                async {
                    inMem.Keys |> Seq.filter (fun kv -> fst kv = folder)
                               |> Seq.iter (fun kv -> inMem.Remove(kv) |> ignore)
                }

            override self.CreateMutable(folder : string, file : string, serialize : Stream -> Async<unit>) : Async<Tag> = 
                async {
                    let t = newTag()
                    use ms = new MemoryStream()
                    do! serialize(ms)
                    lock InMemoryRegistry.Sync (fun () -> inMem.Add((folder, file), (ms.ToArray(), t)) )
                    return t
                }

            override self.ReadMutable(folder : string, file : string) : Async<Stream * Tag> = 
                async {
                    let array, tag = lock InMemoryRegistry.Sync (fun () -> inMem.[folder, file])
                    return new MemoryStream(array) :> _, tag
                }

            override self.UpdateMutable(folder, file, serialize, oldTag) =
                async {
                    let t = newTag()
                    use ms = new MemoryStream()
                    do! serialize(ms)
                    return
                        lock InMemoryRegistry.Sync (fun () -> 
                            let _, tag = inMem.[folder, file]
                            if oldTag = tag then
                                inMem.Add((folder,file), (ms.ToArray() ,t))
                                true, t
                            else
                                false, oldTag)
                }
                
            override self.ForceUpdateMutable(folder, file, serialize) : Async<Tag> =
                async {
                    let t = newTag()
                    use ms = new MemoryStream()
                    do! serialize(ms)
                    return
                        lock InMemoryRegistry.Sync (fun () -> 
                            let _, tag = inMem.[folder, file]
                            inMem.Add((folder,file), (ms.ToArray() ,t))
                            t)
                }


    type InMemoryStoreFactory () =
        interface IStoreFactory with
            member this.CreateStoreFromConnectionString (path : string) = 
                InMemoryStore() :> IStore