namespace Nessos.MBrace.Client

    open System
    open System.IO

    open Nessos.MBrace.Runtime.Store
    
    type IStore = Nessos.MBrace.Runtime.Store.IStore
    type ICloudStoreFactory = Nessos.MBrace.Runtime.Store.ICloudStoreFactory

    /// Represents the storage provider used by CloudRefs etc.
    /// This can be the the local filesystem (for local usage),
    /// a shared filesystem (like a UNC path)
    /// or any custom provider that implements the IStore interface.
    type StoreProvider private (factoryType : Type, connectionString : string) =

        member __.StoreFactoryQualifiedName = factoryType.FullName
        member __.StoreFactoryType = factoryType
        member __.ConnectionString = connectionString

        /// Defines a new store provider
        static member Define<'Factory when 'Factory :> ICloudStoreFactory>(connectionString : string) =
            new StoreProvider(typeof<'Factory>, connectionString)

        /// Create a StoreProvider object from the storeProvider, storeEndpoint configuration.
        static member Parse(storeFactoryQualifiedName : string, connectionString : string) =
            let factoryType = Type.GetType(storeFactoryQualifiedName, throwOnError = true)
            if typeof<ICloudStoreFactory>.IsAssignableFrom factoryType then
                new StoreProvider(factoryType, connectionString)
            else
                invalidArg "storeFactoryQualifiedName" "Type is not a store factory"

        /// A store provider using the file system with an endpoint being either a
        /// path in the local file system, or a UNC path.
        static member DefineFileSystem (path : string) = StoreProvider.Define<FileSystemStoreFactory>(path)

        /// A store provider using the local file system (and a folder in the users temp path).
        /// Any endpoint given will be ignored.
        static member LocalFS = StoreProvider.DefineFileSystem(Path.Combine(Path.GetTempPath(), "mbrace-localfs"))

////        static member LocalFS =
////            let path = 
////            StoreProvider.Define<FileSystemStoreFactory>()
//
//
//    type StoreProvider =
//
//        | LocalFS
//
//        | FileSystem of string
//        /// A custom store provider and its endpoint (connection string).
//        | Plugin of System.Type * string
//    with
//        
//        static member Parse (storeProvider : string, storeEndpoint : string) =
//            match storeProvider with
//            | "LocalFS" -> LocalFS
//            | "FileSystem" -> FileSystem storeEndpoint
//            | _ ->
//                let t = System.Type.GetType(storeProvider, throwOnError = true)
//                Plugin(t, storeEndpoint)
//
//        /// The provider's endpoint (path, connection string, etc).
//        member sp.EndPoint =
//            match sp with
//            | LocalFS -> " "
//            | FileSystem uri -> uri
//            | Plugin (_,cs) -> cs
//
//        /// The provider's name. This is the Assembly Qualified Name for custom providers.
//        member sp.Name =
//            match sp with
//            | LocalFS -> "LocalFS"
//            | FileSystem _ -> "FileSystem"
//            | Plugin(t,_) -> t.AssemblyQualifiedName

namespace Nessos.MBrace.Runtime.Store
    
    open System
    open System.Runtime
    open System.Reflection
    open System.Security.Cryptography
    open System.Text

    open Nessos.MBrace.Core
    open Nessos.MBrace.Client
    open Nessos.MBrace.Utils
//    open Nessos.MBrace.Utils.String
//    open Nessos.MBrace.Utils.AssemblyCache


//    module internal Crypto =        



    [<StructuralEquality ; StructuralComparison>]
    type StoreId = 
        {
            AssemblyQualifiedName : string
            UUID                  : byte []
        }

    with override this.ToString () = sprintf "StoreId:%s" this.AssemblyQualifiedName


//    type StoreActivator = 
//        internal {
//            Packet : AssemblyPacket
//            FactoryAQN : string
//            ConnectionString : string
//        }

    and StoreInfo =
        {
            Id : StoreId
            Provider : StoreProvider
//            FactoryType : Type
//            ConnectionString : string
            Store : IStore
        }
//    with
//        member s.Assembly = s.FactoryType.Assembly

    // TODO : handle all dependent assemblies
    and StoreRegistry private () =

        static let defaultStore = ref None
        static let storeIndex = Atom.atom Map.empty<StoreId, StoreInfo>
        static let coreConfigIndex = Atom.atom Map.empty<StoreId, CoreConfiguration>

        static let hashAlgorithm = SHA256Managed.Create() :> HashAlgorithm
        static let computeHash (txt : string) = hashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes txt)

        static member Activate (provider : StoreProvider, ?makeDefault) =
            let factory = Activator.CreateInstance(provider.StoreFactoryType) :?> ICloudStoreFactory
            let store = factory.CreateStoreFromConnectionString provider.ConnectionString
            let id = { AssemblyQualifiedName = store.GetType().FullName ; UUID = computeHash store.UUID } 

            match storeIndex.Value.TryFind id with
            | Some sI -> sI
            | None ->
                let storeInfo = { Id = id ; Provider = provider ; Store = store }
            
                if (defaultArg makeDefault false) then defaultStore := Some storeInfo

                storeIndex.Swap(fun m -> m.Add(storeInfo.Id, storeInfo))
                storeInfo


        static member RegisterCoreConfiguration (id : StoreId, cconfig : CoreConfiguration) =
            coreConfigIndex.Swap(fun m -> m.Add(id, cconfig))

        static member TryGetCoreConfiguration (id : StoreId) =
            coreConfigIndex.Value.TryFind id

        static member TryGetInstance (id : StoreId) = storeIndex.Value.TryFind id

        static member GetInstance (id : StoreId) =
            match storeIndex.Value.TryFind id with
            | Some store -> store
            | None -> invalidOp "Store: missing instance with id '%O'." id

//        static member Activate<'StoreFactory when 'StoreFactory :> ICloudStoreFactory> (connectionString, ?makeDefault) =
//            activate makeDefault typeof<'StoreFactory> connectionString
//
//        static member Activate(factoryType : Type, connectionString, ?makeDefault) =
//            activate makeDefault factoryType connectionString

//        
//            activate makeDefault provider
//            let factoryType, connectionString =
//                match provider with
//                | LocalFS -> typeof<FileSystemStoreFactory>, Path.Com
//                | FileSystem path -> typeof<FileSystemStoreFactory>, path
//                | Plugin (ft, cs) -> ft, cs

//            activate makeDefault factoryType connectionString

//        static member TryActivate (activator : StoreActivator, ?makeDefault) =
//            match AssemblyPacket.TryLoad activator.Packet with
//            | None -> None
//            | Some _ ->
//                let factoryType = Type.GetType(activator.FactoryAQN, throwOnError = true)
//                Some <| activate makeDefault factoryType activator.ConnectionString

        static member GetProvider(id : StoreId, ?includeImage) =
            let storeInfo = StoreRegistry.GetInstance id
            storeInfo.Provider
//            let packet = AssemblyPacket.OfAssembly(storeInfo.Assembly, ?includeImage = includeImage)
//            { Packet = packet; FactoryAQN =  storeInfo.FactoryType.AssemblyQualifiedName; ConnectionString = storeInfo.ConnectionString }

        static member DefaultStore 
                with get () =
                    match defaultStore.Value with
                    | None -> invalidOp "Store: no default store has been registered."
                    | Some s -> s
                and set (s : StoreInfo) =
                    storeIndex.Swap(fun m -> m.Add(s.Id, s))
                    defaultStore := Some s