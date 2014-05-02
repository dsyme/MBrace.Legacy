namespace Nessos.MBrace.Runtime
    
    open System
    open System.IO
    open System.Runtime.Serialization

    open Nessos.MBrace
    open Nessos.MBrace.Utils

    type ICloudFileStore =
        abstract Create   : Container * Id * (Stream -> Async<unit>) -> Async<ICloudFile>
        abstract Read     : ICloudFile * (Stream -> Async<obj>) -> Async<obj>
        abstract ReadAsSeq: ICloudFile * (Stream -> Async<obj>) * Type -> Async<obj>
        abstract GetFiles : Container                            -> Async<ICloudFile []>
        abstract GetFile  : Container  * Id                      -> Async<ICloudFile   >
        abstract Exists   : Container                            -> Async<bool         >
        abstract Exists   : Container  * Id                      -> Async<bool         >
        abstract Delete   : Container  * Id                      -> Async<unit         >

    type CloudFile(id : string, container : string) =

        let fileStoreLazy = lazy IoC.Resolve<ICloudFileStore>()

        interface ICloudFile with
            member self.Name = id
            member self.Container = container
            member self.Dispose () =
                fileStoreLazy.Value.Delete(container, id)

        override self.ToString() = sprintf' "%s - %s" container id

        new (info : SerializationInfo, context : StreamingContext) = 
                CloudFile(info.GetValue("id", typeof<string>) :?> string,
                            info.GetValue("container", typeof<string>) :?> string)
        
        interface ISerializable with 
            member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)