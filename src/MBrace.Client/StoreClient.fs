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

        //---------------------------------------------------------------------------------
        // CloudRef

        member this.CreateCloudRefAsync(container : Container,  value : 'T) =
            config.CloudRefProvider.Create(container, Guid.NewGuid().ToString(), value)

        member this.GetCloudRefsAsync(container : Container) =
            config.CloudRefProvider.GetContainedRefs(container)

        member this.GetCloudRefAsync(container : Container, id : Id) =
            config.CloudRefProvider.GetExisting(container, id)
            

        //---------------------------------------------------------------------------------
        // CloudSeq
        member this.CreateCloudSeqAsync(container : Container,  values : 'T seq) =
            config.CloudSeqProvider.Create(container, Guid.NewGuid().ToString(), values)

        member this.GetCloudSeqsAsync(container : Container) =
            config.CloudSeqProvider.GetContainedSeqs(container)

        member this.GetCloudSeqAsync(container : Container, id : Id) =
            config.CloudSeqProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // CloudFile
        member this.CreateCloudFileAsync(container : Container, writer : Stream -> Async<unit>) =
            config.CloudFileProvider.Create(container, Guid.NewGuid().ToString(), writer)

        member this.GetCloudFilesAsync(container : Container) =
            config.CloudFileProvider.GetContainedFiles(container)

        member this.GetCloudFileAsync(container : Container, id : Id) =
            config.CloudFileProvider.GetExisting(container, id)

        //---------------------------------------------------------------------------------
        // MutableCloudRef

        member this.CreateMutableCloudRefAsync(container : Container,  value : 'T) =
            config.MutableCloudRefProvider.Create(container, Guid.NewGuid().ToString(), value)

        member this.GetMutableCloudRefsAsync(container : Container) =
            config.MutableCloudRefProvider.GetContainedRefs(container)

        member this.GetMutableCloudRefAsync(container : Container, id : Id) =
            config.MutableCloudRefProvider.GetExisting(container, id)
            

