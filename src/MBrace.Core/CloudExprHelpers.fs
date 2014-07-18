namespace Nessos.MBrace.CloudExpr

    open Nessos.MBrace

    /// CloudExpr helpers; for internal use.
    type CloudExprHelpers =
        /// Converts a Cloud computation to a CloudExpr.
        static member Unwrap(cloud : Cloud<'T>) = cloud.CloudExpr