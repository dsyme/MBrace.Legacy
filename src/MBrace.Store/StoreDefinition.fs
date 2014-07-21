namespace Nessos.MBrace.Store

    open System
    open System.IO

    /// Represents the storage provider used by CloudRefs etc.
    /// This can be the local filesystem (for local usage),
    /// a shared filesystem (like a UNC path)
    /// or any custom provider that implements the ICloudStore interface.
    [<AutoSerializable(true); NoEquality ; NoComparison>]
    type StoreDefinition private (id : StoreId, storeFactoryType : Type, connectionString : string) =

        /// Store identifier for definition
        member __.Id = id

        /// ICloudStoreFactory instance used by the store
        member __.StoreFactoryType = storeFactoryType

        /// Connection string for given endpoint
        member __.ConnectionString = connectionString

        /// Defines a new store provider.
        static member Create<'Factory when 'Factory :> ICloudStoreFactory>(connectionString : string) =
            // attempt a connection before returning
            let factory = System.Activator.CreateInstance<'Factory> ()
            let store = factory.CreateStoreFromConnectionString connectionString
            let id = StoreId.Generate store

            new StoreDefinition(id, typeof<'Factory>, connectionString)

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