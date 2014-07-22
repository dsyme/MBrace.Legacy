namespace Nessos.MBrace

    open System

    open Nessos.MBrace.CloudExpr

    /// Adding this attribute to a let-binding marks that
    /// the value definition contains cloud expressions.
    type CloudAttribute = ReflectedDefinitionAttribute

    /// The identifier of the running cloud process.
    type ProcessId = Nessos.MBrace.CloudExpr.ProcessId

    /// Disable tracing for the current cloud workflow.
    [<Sealed>]
    [<System.AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Method ||| AttributeTargets.Property, AllowMultiple = false)>]
    type NoTraceInfoAttribute() = 
        inherit System.Attribute()

    /// Disable static check warnings being generated for current workflow.
    [<Sealed>]
    type NoWarnAttribute() =
        inherit System.Attribute()

    /// Representation of a cloud computation, which, when run 
    /// will produce a value of type 'T, or raise an exception.
    [<Sealed>]
    type Cloud<'T> internal (cloudExpr : CloudExpr) =

        /// The type of the returned value.
        member __.Type = typeof<'T>
        member internal __.CloudExpr = cloudExpr


namespace Nessos.MBrace.CloudExpr

    open Nessos.MBrace

    [<RequireQualifiedAccess>]
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module internal CloudExpr =

        let inline wrap (cloudExpr : CloudExpr) = new Cloud<'T>(cloudExpr)
        let inline unwrap (cloudBlock : Cloud<'T>) = cloudBlock.CloudExpr


    /// CloudExpr helpers; for interpreter use.
    type CloudExprHelpers =

        /// Exposes the untyped expression tree for given cloud computation
        static member Unwrap(cloud : Cloud<'T>) = CloudExpr.unwrap cloud