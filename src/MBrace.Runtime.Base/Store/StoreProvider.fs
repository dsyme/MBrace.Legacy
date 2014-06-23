namespace Nessos.MBrace.Client

    open System
    open System.IO

    open Nessos.MBrace.Runtime.Store

    /// Represents the storage provider used by CloudRefs etc.
    /// This can be the the local filesystem (for local usage),
    /// a shared filesystem (like a UNC path)
    /// or any custom provider that implements the ICloudStore interface.
    [<AutoSerializable(false) ; NoEquality ; NoComparison>]
    type StoreProvider private (factoryType : Type, connectionString : string) =

        member val internal Id : StoreId option = None with get,set

        member __.StoreFactoryQualifiedName = factoryType.AssemblyQualifiedName
        member __.StoreFactoryType = factoryType
        member __.ConnectionString = connectionString

        member internal __.InitStore() =
            let factory = Activator.CreateInstance factoryType :?> ICloudStoreFactory
            factory.CreateStoreFromConnectionString(connectionString)

        /// Defines a new store provider
        static member Define<'Factory when 'Factory :> ICloudStoreFactory>(connectionString : string) =
            new StoreProvider(typeof<'Factory>, connectionString)

        static member Define(storeFactoryType : Type, connectionString : string) =
            if typeof<ICloudStoreFactory>.IsAssignableFrom storeFactoryType then
                new StoreProvider(storeFactoryType, connectionString)
            else
                invalidArg "storeFactoryQualifiedName" "Type is not a store factory."

        static member internal TryDefine(storeFactoryQualifiedName : string, connectionString : string, throwOnError : bool) =
            match Type.GetType(storeFactoryQualifiedName, throwOnError = throwOnError) with
            | null -> None
            | factoryType -> Some <| StoreProvider.Define(factoryType, connectionString)

        /// Create a StoreProvider object from the storeProvider, storeEndpoint configuration.
        static member Parse(name : string, connectionString : string) =
            match name with
            | "LocalFS" -> StoreProvider.LocalFS
            | "FileSystem" -> StoreProvider.FileSystem connectionString
            | _ -> StoreProvider.TryDefine(name, connectionString, throwOnError = true) |> Option.get

        /// A store provider using the file system with an endpoint being either a
        /// path in the local file system, or a UNC path.
        static member FileSystem (path : string) = StoreProvider.Define<FileSystemStoreFactory>(path)

        /// A store provider using the local file system (and a folder in the users temp path).
        /// Any endpoint given will be ignored.
        static member LocalFS = StoreProvider.FileSystem(Path.Combine(Path.GetTempPath(), "mbrace-localfs"))