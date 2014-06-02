namespace Nessos.MBrace.Client

    open System
    open System.IO

    open Nessos.MBrace.Runtime.Store
    
    type ICloudStore = Nessos.MBrace.Runtime.Store.ICloudStore
    type ICloudStoreFactory = Nessos.MBrace.Runtime.Store.ICloudStoreFactory

    /// Represents the storage provider used by CloudRefs etc.
    /// This can be the the local filesystem (for local usage),
    /// a shared filesystem (like a UNC path)
    /// or any custom provider that implements the ICloudStore interface.
    type StoreProvider private (factoryType : Type, connectionString : string) =

        member __.StoreFactoryQualifiedName = factoryType.FullName
        member __.StoreFactoryType = factoryType
        member __.ConnectionString = connectionString

        /// Defines a new store provider
        static member Define<'Factory when 'Factory :> ICloudStoreFactory>(connectionString : string) =
            new StoreProvider(typeof<'Factory>, connectionString)

        /// Create a StoreProvider object from the storeProvider, storeEndpoint configuration.
        static member Parse(storeFactoryQualifiedName : string, connectionString : string) =
            match storeFactoryQualifiedName with
            | "LocalFS" -> StoreProvider.LocalFS
            | "FileSystem" -> StoreProvider.FileSystem connectionString
            | _ ->
                let factoryType = Type.GetType(storeFactoryQualifiedName, throwOnError = true)
                if typeof<ICloudStoreFactory>.IsAssignableFrom factoryType then
                    new StoreProvider(factoryType, connectionString)
                else
                    invalidArg "storeFactoryQualifiedName" "Type is not a store factory"

        /// A store provider using the file system with an endpoint being either a
        /// path in the local file system, or a UNC path.
        static member FileSystem (path : string) = StoreProvider.Define<FileSystemStoreFactory>(path)

        /// A store provider using the local file system (and a folder in the users temp path).
        /// Any endpoint given will be ignored.
        static member LocalFS = StoreProvider.FileSystem(Path.Combine(Path.GetTempPath(), "mbrace-localfs"))



namespace Nessos.MBrace.Runtime.Store
    
    open System
    open System.Runtime
    open System.Reflection
    open System.Security.Cryptography
    open System.Text

    open Nessos.Thespian.ConcurrencyTools

    open Nessos.MBrace.Core
    open Nessos.MBrace.Client
    open Nessos.MBrace.Utils

    [<StructuralEquality ; StructuralComparison>]
    type StoreId = 
        {
            AssemblyQualifiedName : string
            UUID                  : byte []
        }

    with override this.ToString () = sprintf "StoreId:%s" this.AssemblyQualifiedName


    and StoreInfo =
        {
            Id : StoreId
            Provider : StoreProvider
            Store : ICloudStore
        }

    // TODO : handle all dependent assemblies
    and StoreRegistry private () =

        static let defaultStore = ref None
        static let storeIndex = Atom.atom Map.empty<StoreId, StoreInfo>
        static let coreConfigIndex = Atom.atom Map.empty<StoreId, PrimitiveConfiguration>

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


        static member RegisterCoreConfiguration (id : StoreId, cconfig : PrimitiveConfiguration) =
            coreConfigIndex.Swap(fun m -> m.Add(id, cconfig))

        static member TryGetCoreConfiguration (id : StoreId) =
            coreConfigIndex.Value.TryFind id

        static member TryGetInstance (id : StoreId) = storeIndex.Value.TryFind id

        static member GetInstance (id : StoreId) =
            match storeIndex.Value.TryFind id with
            | Some store -> store
            | None -> invalidOp "Store: missing instance with id '%O'." id

        static member GetProvider(id : StoreId, ?includeImage) =
            let storeInfo = StoreRegistry.GetInstance id
            storeInfo.Provider

        static member DefaultStore 
                with get () =
                    match defaultStore.Value with
                    | None -> invalidOp "Store: no default store has been registered."
                    | Some s -> s
                and set (s : StoreInfo) =
                    storeIndex.Swap(fun m -> m.Add(s.Id, s))
                    defaultStore := Some s

        static member DefaultPrimitiveConfiguration
            with get () =
                let storeId = StoreRegistry.DefaultStore.Id
                match StoreRegistry.TryGetCoreConfiguration(storeId) with
                | None -> invalidOp "Store: no configuration has been registered for %A." storeId
                | Some cc -> cc