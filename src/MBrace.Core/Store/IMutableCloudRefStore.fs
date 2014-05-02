namespace Nessos.MBrace.Runtime
    
    open System
    open System.Runtime.Serialization
    open System.Collections.Generic

    open Nessos.MBrace
    open Nessos.MBrace.Utils

    type IMutableCloudRefStore =
            abstract member Create : Container * Id * obj * System.Type -> Async<IMutableCloudRef>
            abstract member Create : Container * Id * 'T -> Async<IMutableCloudRef<'T>>
            abstract member Delete : Container * Id -> Async<unit>
            abstract member Exists : Container -> Async<bool>
            abstract member Exists : Container * Id -> Async<bool>
            abstract member ForceUpdate : IMutableCloudRef * obj -> Async<unit>
            abstract member GetRefType : Container * Id -> Async<System.Type>
            abstract member GetRefs : Container -> Async<IMutableCloudRef []>
            abstract member Read : IMutableCloudRef -> Async<obj>
            abstract member Read : IMutableCloudRef<'T> -> Async<'T>
            abstract member Update : IMutableCloudRef * obj -> Async<bool>
            abstract member Update : IMutableCloudRef<'T> * 'T -> Async<bool>


    type MutableCloudRef<'T>(id : string, container : string, tag : Tag, ty : Type) =
        
        let mutablecloudrefstorelazy = lazy IoC.Resolve<IMutableCloudRefStore>()

        interface IMutableCloudRef with
            member self.Name = id
            member self.Container = container
            member self.Type = ty
            member self.Dispose () =
                let self = self :> IMutableCloudRef
                mutablecloudrefstorelazy.Value.Delete(self.Container, self.Name)

        interface IMutableCloudRef<'T>

        interface IMutableCloudRefTagged with
            member val Tag = tag with get, set

        interface IMutableCloudRefTagged<'T>

        override self.ToString() =  sprintf' "%s - %s" container id

        new (info : SerializationInfo, context : StreamingContext) = 
                MutableCloudRef<'T>(info.GetValue("id", typeof<string>) :?> string,
                                    info.GetValue("container", typeof<string>) :?> string,
                                    info.GetValue("tag", typeof<string>) :?> string,
                                    info.GetValue("type", typeof<Type>) :?> Type)
        
        interface ISerializable with 
            member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                info.AddValue("id", id)
                info.AddValue("container", container)
                info.AddValue("tag", (self :> IMutableCloudRefTagged).Tag)
                info.AddValue("type", ty)