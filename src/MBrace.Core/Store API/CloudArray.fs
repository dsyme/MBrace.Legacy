namespace Nessos.MBrace
    
    open System.Collections
    
    open Nessos.MBrace.CloudExpr

    type CloudArray =

        static member New<'T>(container : string, values : seq<'T>) : Cloud<ICloudArray<'T>> =
            CloudExpr.wrap <| NewCloudArray (container, values :> IEnumerable, typeof<'T>)