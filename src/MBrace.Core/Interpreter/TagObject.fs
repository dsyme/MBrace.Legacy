namespace Nessos.MBrace.Utils

    open System.Runtime.Serialization

    open Nessos.Thespian.PowerPack

    [<AutoOpen>]
    module TagObject = 
       

        type TagObj (value : obj, t : System.Type) = 
            
            new (info : SerializationInfo, context : StreamingContext) = 
                TagObj(info.GetValue("value", typeof<obj>), info.GetValue("type", typeof<System.Type>) :?> System.Type)

            member self.Type = t
            member self.Value = value
            override self.ToString() = 
                sprintf' "TagObj (%s, %A)" t.Name value

            interface ISerializable with
                member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
                    info.AddValue("value", value)
                    info.AddValue("type", t)
                    

        let (|TagObj|) (tagObject : TagObj) = (tagObject.Value, tagObject.Type)

        let safeBox<'T> (value : 'T) = new TagObj(value :> obj, typeof<'T>)

        let safeUnbox<'T> (TagObj (value : obj, t : System.Type)) : 'T =
            if t = typeof<'T> then
                if value <> null then
                    value :?> 'T
                else
                    if t = typeof<unit> then
                        (() :> obj) :?> 'T
                    else
                        let attrs = t.GetCustomAttributes(typeof<CompilationRepresentationAttribute>, false)
                        if attrs.Length = 1
                            && (attrs.[0] :?> CompilationRepresentationAttribute).Flags 
                            &&& CompilationRepresentationFlags.UseNullAsTrueValue <> enum 0 then
                            value :?> 'T
                        else
                            value :?> 'T
            else
                raise <| new System.InvalidCastException("type mismatch")