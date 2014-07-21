namespace Nessos.MBrace.Store

    open System
    open System.Text
    open System.Security.Cryptography

    open Nessos.MBrace.Store

    module private Hashcode =
        
        let private hashAlgorithm = SHA256Managed.Create() :> HashAlgorithm

        let compute (txt : string) = hashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes txt)

    /// Provides a unique identification token for a given store instance

    [<AutoSerializable(true)>]
    type StoreId private (assemblyQualifiedName : string, hashcode : byte[]) =
        
        /// Assembly qualified name for the store implementation
        member __.AssemblyQualifiedName = assemblyQualifiedName

        /// Contains a cryptographic hash generated from the connection string
        member __.HashCode = hashcode

        override this.ToString () = sprintf "StoreId:%s" this.AssemblyQualifiedName

        /// Generates a store identifier from given instance.
        static member Generate(store : ICloudStore) =
            let aqn = store.GetType().AssemblyQualifiedName
            let hc = Hashcode.compute store.EndpointId
            new StoreId(aqn, hc)

        member private this.CompareTo (that : StoreId) =
            match compare this.AssemblyQualifiedName that.AssemblyQualifiedName with
            | 0 -> compare this.HashCode that.HashCode
            | c -> c

        override id.Equals(o:obj) =
            match o with
            | :? StoreId as id' -> id.CompareTo id = 0
            | _ -> false

        override id.GetHashCode() = hash (assemblyQualifiedName, hashcode)

        interface IComparable with
            member id.CompareTo that =
                match that with
                | :? StoreId as id' -> id.CompareTo id'
                | _ -> invalidArg "that" "invalid comparand."