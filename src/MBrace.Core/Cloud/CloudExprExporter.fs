namespace Nessos.MBrace.CloudExpr

    open Nessos.MBrace

    type CloudExprHelper =
        static member Unwrap(cloud : Cloud<'T>) = cloud.CloudExpr