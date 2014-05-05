namespace Nessos.MBrace.Store.Collections

//    open Nessos.MBrace.Store
//    open Nessos.MBrace.Utils
//    open Nessos.MBrace.Utils.Atom
//
//    open System
//    open System.Collections
//    open System.Collections.Generic
//
//    type PersistentAtomMap<'Key, 'Value when 'Key: comparison>(store: IBlobStore<'Value>, toStr: 'Key -> string, fromStr: string -> 'Key) =
//
//        let map = Atom.atom Map.empty<'Key, 'Value>
//
//        do Atom.swap map (fun _ -> store.GetAll() |> Seq.map (fun (k, v) -> fromStr k, v) |> Map.ofSeq)
//
//        let add k v = async { 
//                do! store.SetAsync(toStr k, v)
//                Atom.swap map (fun m -> Microsoft.FSharp.Collections.Map.add k v m)
//            }
//
//        let remove k = async {
//                do! store.RemoveAsync(toStr k)
//                Atom.swap map (fun m -> Microsoft.FSharp.Collections.Map.remove k m)
//            }
//
//        let clear () = async {
//                do! store.ClearAsync()
//                Atom.swap map (fun _ -> Map.empty)
//            }
//
//        let enumerable() = !map :> IEnumerable
//        let enumerable'() = !map :> IEnumerable<KeyValuePair<'Key, 'Value>>
//        let collection() = !map :> ICollection<KeyValuePair<'Key, 'Value>>
//        let dictionary() = !map :> IDictionary<'Key, 'Value>
//        let comparable() = !map :> IComparable
//
//        member m.AddAsync(key: 'Key, value: 'Value) = add key value
//
//        member m.Add(key: 'Key, value: 'Value) = add key value |> Async.RunSynchronously
//
//        member m.RemoveAsync(key: 'Key) = remove key
//
//        member m.Remove(key: 'Key) = remove key |> Async.RunSynchronously
//
//        member m.ClearAsync() = clear()
//
//        member m.Clear() = clear() |> Async.RunSynchronously
//
//        member m.Item with get (key: 'Key) = Map.find key !map
//
//        member m.ContainsKey(key: 'Key) = Map.containsKey key !map
//
//        member m.TryFind(key: 'Key) = Map.tryFind key !map
//
//        member m.AsMap with get() = !map
//
//        member m.Clone() = new PersistentAtomMap<'Key, 'Value>(store, toStr, fromStr)
//
//        member m.Store with get() = store
//
//        interface IEnumerable with
//            member m.GetEnumerator() = enumerable().GetEnumerator()
//
//        interface IEnumerable<KeyValuePair<'Key, 'Value>> with
//            member m.GetEnumerator() = enumerable'().GetEnumerator()
//
//        interface ICollection<KeyValuePair<'Key, 'Value>> with
//            member m.Count with get() = collection().Count
//
//            member m.IsReadOnly with get() = collection().IsReadOnly
//
//            member m.Add(item: KeyValuePair<'Key, 'Value>) = add (item.Key) (item.Value) |> Async.RunSynchronously
//
//            member m.Clear() = clear() |> Async.RunSynchronously
//
//            member m.Contains(item: KeyValuePair<'Key, 'Value>) = collection().Contains(item)
//
//            member m.CopyTo(array: KeyValuePair<'Key, 'Value>[], index: int) = collection().CopyTo(array, index)
//
//            member m.Remove(item: KeyValuePair<'Key, 'Value>) = if collection().Contains(item) then remove (item.Key) |> Async.RunSynchronously ; true else false
//
//        interface IDictionary<'Key, 'Value> with
//            member m.Item with get (key: 'Key) = dictionary().Item(key)
//                           and set (key: 'Key) (value: 'Value) = add key value |> Async.RunSynchronously
//            
//            member m.Keys with get () = dictionary().Keys
//
//            member m.Values with get () = dictionary().Values
//
//            member m.ContainsKey(key: 'Key) = dictionary().ContainsKey(key)
//
//            member m.Add(key: 'Key, value: 'Value) = add key value |> Async.RunSynchronously
//
//            member m.Remove(key: 'Key) = remove key |> Async.RunSynchronously ; true
//
//            member m.TryGetValue(key: 'Key, value: byref<'Value>): bool = dictionary().TryGetValue(key, ref value)
//
//
//    module PersistentAtomMap =
//
//        let toSeq (m: PersistentAtomMap<_, _>) = 
//            m.AsMap |> Map.toSeq
//
//        let add key value (m: PersistentAtomMap<_, _>) =
//            m.Add(key, value)
//            m
