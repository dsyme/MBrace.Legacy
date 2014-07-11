namespace Nessos.MBrace.CloudExpr

    open Nessos.MBrace

    type CloudExprHelpers =
        static member Unwrap(cloud : Cloud<'T>) = cloud.CloudExpr