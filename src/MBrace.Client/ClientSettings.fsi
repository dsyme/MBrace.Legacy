namespace Nessos.MBrace.Client
    open Nessos.MBrace

    /// The object representing the {m}brace client settings.
    type MBraceSettings =
        class
            private new : unit -> MBraceSettings
            static member internal Initialize : unit -> unit

            ///Gets the Store Provider currently used by the client.
            static member TryGetStoreProvider : unit -> StoreProvider option

            ///Gets the path where the dependent assemblies are saved.
            static member AssemblyCachePath : string with set, get

            ///Gets the client's unique identifier.
            static member ClientId : System.Guid

            ///Turns on or off the client side expression checking.
            ///Turning this on will make expression checks happen at the client rather
            ///than the runtime.
            static member ClientSideExpressionCheck : bool with set, get

//            ///The path that will be used as a local cache by the client. CloudRefs/CloudSeqs
//            ///may be cached there.
//            static member LocalCachePath : string with set, get

            ///The (relative/absolute) path to the mbraced.exe.
            static member MBracedExecutablePath : string with set, get

            ///Gets or sets the StoreProvider used by the client.
            static member StoreProvider : StoreProvider with set, get
        end
//
//namespace Nessos.MBrace.Runtime
//    open Nessos.MBrace.Client
//
//    type MBraceSettingsExtensions =
//        class
//            static member Init : unit -> unit
//        end