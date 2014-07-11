namespace Nessos.MBrace.Store

    open System
    open System.IO

    /// Represents the storage provider used by CloudRefs etc.
    /// This can be the the local filesystem (for local usage),
    /// a shared filesystem (like a UNC path)
    /// or any custom provider that implements the ICloudStore interface.
    [<AutoSerializable(true); NoEquality ; NoComparison>]
    type StoreDefinition private (storeFactoryType : Type, connectionString : string) =

        member __.StoreFactoryType = storeFactoryType
        member __.ConnectionString = connectionString

        /// Defines a new store provider
        static member Create<'Factory when 'Factory :> ICloudStoreFactory>(connectionString : string) =
            new StoreDefinition(typeof<'Factory>, connectionString)

        static member Create(storeFactoryType : Type, connectionString : string) =
            if typeof<ICloudStoreFactory>.IsAssignableFrom storeFactoryType then
                new StoreDefinition(storeFactoryType, connectionString)
            else
                invalidArg "storeFactoryQualifiedName" "Type is not a store factory."

        static member TryDefine(storeFactoryQualifiedName : string, connectionString : string, throwOnError : bool) =
            match Type.GetType(storeFactoryQualifiedName, throwOnError = throwOnError) with
            | null -> None
            | factoryType -> Some <| StoreDefinition.Create(factoryType, connectionString)

        /// Create a StoreProvider object from the storeProvider, storeEndpoint configuration.
        static member Parse(name : string, connectionString : string) =
            match name with
            | "LocalFS" -> StoreDefinition.LocalFS
            | "FileSystem" -> StoreDefinition.FileSystem connectionString
            | "SqlStore" -> StoreDefinition.SqlServer connectionString
            | _ -> StoreDefinition.TryDefine(name, connectionString, throwOnError = true) |> Option.get


        /// <summary>
        /// A store provider using the file system with an endpoint being either a
        /// path in the local file system, or a UNC path.
        /// </summary>
        /// <param name="path">UNC path</param>
        static member FileSystem (path : string) = StoreDefinition.Create<FileSystemStoreFactory>(path)

        /// A store provider using the local file system (and a folder in the users temp path).
        /// Any endpoint given will be ignored.
        static member LocalFS = StoreDefinition.FileSystem(Path.Combine(Path.GetTempPath(), "mbrace-localfs"))

        /// <summary>
        ///     Defines an SQL server store with given connection string
        /// </summary>
        /// <param name="connectionString">SQL server connection string.</param>
        static member SqlServer (connectionString : string) = StoreDefinition.Create<SqlServerStoreFactory>(connectionString)