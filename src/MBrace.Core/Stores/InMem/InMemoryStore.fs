namespace Nessos.MBrace.Store
    open System
    open System.IO
    open System.Collections.Generic

    type InMemoryStore() =

        let inMem = Dictionary<string * string, byte []>()

        interface IStore with
            override self.Name = "In memory store"
            override self.UUID = Guid.NewGuid().ToString()

            override self.Create(folder : string, file : string, serialize : Stream -> Async<unit>, ?asFile : bool) = 
                async {
                    ignore asFile
                    use ms = new MemoryStream()
                    do! serialize(ms)
                    inMem.Add((folder, file), ms.ToArray())
                }
             
            override self.Read(folder : string, file : string) : Async<Stream> = 
                async {
                    let array = inMem.[folder, file]
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

            override self.UpdateMutable(folder, file, serialize, oldTag) =
                raise <| NotImplementedException()

            override self.ForceUpdateMutable(folder, file, serialize) : Async<Tag> =
                raise <| NotImplementedException()

            override self.CreateMutable(folder : string, file : string, serialize : Stream -> Async<unit>) : Async<Tag> = 
                raise <| NotImplementedException()

            override self.ReadMutable(folder : string, file : string) : Async<Stream * Tag> = 
                raise <| NotImplementedException()

    type InMemoryStoreFactory () =
        interface IStoreFactory with
            member this.CreateStoreFromConnectionString (path : string) = 
                InMemoryStore() :> IStore