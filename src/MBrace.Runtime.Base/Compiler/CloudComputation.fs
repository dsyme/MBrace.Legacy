namespace Nessos.MBrace.Core

    open System
    open System.Reflection
    open System.Runtime.Serialization

    open Microsoft.FSharp.Quotations

    open Nessos.MBrace
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Utils.PrettyPrinters


    [<AbstractClass>]
    type CloudComputation internal () =

        /// name given to the cloud computation
        abstract Name : string
        /// specifies if given computation has been verified
        abstract IsVerifiedComputation : bool
        /// return type of the cloud computation
        abstract ReturnType : Type
        /// assemblies which computation depends on
        abstract Dependencies : Assembly list
        /// function metadata for cloud computation
        abstract Functions : FunctionInfo list
        /// compiler warnings in cloud computation
        abstract Warnings : string list

        /// returns the inner expression tree of the cloud computation
        abstract GetCloudExpr : unit -> CloudExpr


    [<AbstractClass>]
    type CloudComputation<'T> internal () =
        inherit CloudComputation()

        /// contained cloud computation
        abstract Value : Cloud<'T>

        override __.ReturnType = typeof<'T>


    type internal BareCloudComputation<'T> internal (name : string, value : Cloud<'T>, dependencies) =
        inherit CloudComputation<'T> ()

        override __.Name = name
        override __.IsVerifiedComputation = false
        override __.Value = value
        override __.Dependencies = dependencies
        override __.Functions = []
        override __.Warnings = []

        override __.GetCloudExpr () = Interpreter.extractCloudExpr value


    type internal QuotedCloudComputation<'T> internal (name : string, expr : Expr<Cloud<'T>>, dependencies, functions, warnings) =
        inherit CloudComputation<'T> ()

        let getValue () = Swensen.Unquote.Operators.eval expr

        override __.Name = name
        override __.IsVerifiedComputation = true
        override __.Value = getValue ()
        override __.Functions = functions
        override __.Dependencies = dependencies
        override __.Warnings = warnings

        override __.GetCloudExpr () = Interpreter.extractCloudExpr(getValue ())


    [<Serializable>]
    type CompilerException = 
        inherit MBraceException

        override e.Message = 
            let name = 
                if String.IsNullOrEmpty e.Name then ""
                else
                    sprintf "'%s' " e.Name

            e.Errors
            |> String.concat "\n"
            |> sprintf "Cloud workflow %sof type '%s' contains errors:\n%s" name (Type.prettyPrint e.Type)

        val public Name : string
        val public Type : Type
        val public Errors : string list
        val public Warnings : string list

        internal new (name : string, t : Type, errors : string list, warnings : string list) = 
            { 
                inherit MBraceException()
                Name = name
                Type = t
                Errors = errors
                Warnings = warnings
            }

        internal new (si : SerializationInfo, sc : StreamingContext) = 
            { 
                inherit MBraceException(si, sc)
                Name = si.Read "name"
                Type = si.Read "type"
                Errors = si.Read "errors"
                Warnings = si.Read "warnings"
            }

        interface ISerializable with
            member __.GetObjectData(si : SerializationInfo, sc : StreamingContext) =
                base.GetObjectData(si, sc)
                si.Write "name" __.Name
                si.Write "type" __.Type
                si.Write "errors" __.Errors
                si.Write "warnings" __.Warnings