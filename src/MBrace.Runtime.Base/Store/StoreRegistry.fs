namespace Nessos.MBrace.Client

    /// Represents the storage provider used by CloudRefs etc.
    /// This can be the the local filesystem (for local usage),
    /// a shared filesystem (like a UNC path)
    /// or any custom provider that implements the IStore interface.
    type StoreProvider =
        /// A store provider using the local file system (and a folder in the users temp path).
        /// Any endpoint given will be ignored.
        | LocalFS
        /// A store provider using the file system with an endpoint being either a
        /// path in the local file system, or a UNC path.
        | FileSystem of string
        /// A custom store provider and its endpoint (connection string).
        | Plugin of System.Type * string
    with
        /// Create a StoreProvider object from the storeProvider, storeEndpoint configuration.
        static member Parse (storeProvider : string, storeEndpoint : string) =
            match storeProvider with
            | "LocalFS" -> LocalFS
            | "FileSystem" -> FileSystem storeEndpoint
            | _ ->
                let t = System.Type.GetType(storeProvider, throwOnError = true)
                Plugin(t, storeEndpoint)

        /// The provider's endpoint (path, connection string, etc).
        member sp.EndPoint =
            match sp with
            | LocalFS -> " "
            | FileSystem uri -> uri
            | Plugin (_,cs) -> cs

        /// The provider's name. This is the Assembly Qualified Name for custom providers.
        member sp.Name =
            match sp with
            | LocalFS -> "LocalFS"
            | FileSystem _ -> "FileSystem"
            | Plugin(t,_) -> t.AssemblyQualifiedName

namespace Nessos.MBrace.Runtime.Store
    
    open System
    open System.Runtime
    open System.Reflection
    open System.Security.Cryptography
    open System.Text

    open Nessos.MBrace.Client
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.String
    open Nessos.MBrace.Utils.AssemblyCache

    module internal Crypto =
        
        let hasher = SHA256Managed.Create()
        let getHashCode (txt : string) = hasher.ComputeHash(Encoding.UTF8.GetBytes txt)


    type StoreActivator = internal StoreActivator of AssemblyPacket * string * string // Assembly id * factory QN * connection string

    and StoreInfo =
        {
            Id : StoreId
            FactoryType : Type
            ConnectionString : string
            Store : IStore
        }
    with
        member s.Assembly = s.FactoryType.Assembly

    // TODO : handle all dependent assemblies
    and StoreRegistry private () =

        static let defaultStore = ref None
        static let storeIndex = Atom.atom Map.empty<StoreId, StoreInfo>

        static let activate makeDefault (factoryType : Type) (connectionString : string) =
            let factory = Activator.CreateInstance(factoryType) :?> IStoreFactory
            let store = factory.CreateStoreFromConnectionString connectionString
            let id = StoreId (Crypto.getHashCode <| factoryType.AssemblyQualifiedName + ":" + store.UUID)

            match storeIndex.Value.TryFind id with
            | Some sI -> sI
            | None ->
                let storeInfo = { Id = id ; FactoryType = factoryType ; ConnectionString = connectionString ; Store = store }
            
                if (defaultArg makeDefault false) then defaultStore := Some storeInfo

                storeIndex.Swap(fun m -> m.Add(storeInfo.Id, storeInfo))
                storeInfo

        static member TryGetInstance (id : StoreId) = storeIndex.Value.TryFind id

        static member GetInstance (id : StoreId) =
            match storeIndex.Value.TryFind id with
            | Some store -> store
            | None -> invalidOp "Store: missing instance with id '%O'." id

        static member Activate<'StoreFactory when 'StoreFactory :> IStoreFactory> (connectionString, ?makeDefault) =
            activate makeDefault typeof<'StoreFactory> connectionString

        static member Activate(factoryType : Type, connectionString, ?makeDefault) =
            activate makeDefault factoryType connectionString

        static member Activate (provider : StoreProvider, ?makeDefault) =
            let factoryType, connectionString =
                match provider with
                | LocalFS -> typeof<LocalFileSystemStoreFactory>, ""
                | FileSystem path -> typeof<FileSystemStoreFactory>, path
                | Plugin (ft, cs) -> ft, cs

            activate makeDefault factoryType connectionString

        static member TryActivate (StoreActivator(packet, factoryQualifiedName, connectionString), ?makeDefault) =
            match AssemblyPacket.TryLoad packet with
            | None -> None
            | Some _ ->
                let factoryType = Type.GetType(factoryQualifiedName, throwOnError = true)
                Some <| activate makeDefault factoryType connectionString

        static member GetActivator(id : StoreId, ?includeImage) =
            let storeInfo = StoreRegistry.GetInstance id
            let packet = AssemblyPacket.OfAssembly(storeInfo.Assembly, ?includeImage = includeImage)
            StoreActivator(packet, storeInfo.FactoryType.AssemblyQualifiedName, storeInfo.ConnectionString)

        static member DefaultStore 
                with get () =
                    match defaultStore.Value with
                    | None -> invalidOp "Store: no default store has been registered."
                    | Some s -> s
                and set (s : StoreInfo) =
                    storeIndex.Swap(fun m -> m.Add(s.Id, s))
                    defaultStore := Some s