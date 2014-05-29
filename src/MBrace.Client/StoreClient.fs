namespace Nessos.MBrace.Client

    open System
    open System.Reflection
    open System.IO

    open Nessos.Thespian.ConcurrencyTools

    open Nessos.FsPickler
    open Nessos.UnionArgParser
    open Nessos.Vagrant
    open Nessos.Thespian.Serialization
    open Nessos.Thespian.Remote

    open Nessos.MBrace
    open Nessos.MBrace.Core
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.Retry
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.Runtime.Logging
    open Nessos.MBrace.Runtime.Store


    type StoreClient internal (config : PrimitiveConfiguration) =
        class end
//        member this.CreateCloudRefAsync(container : Container,  value : 'T) =
//            config.CloudRefProvider.Create



