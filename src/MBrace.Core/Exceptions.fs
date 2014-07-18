namespace Nessos.MBrace

    open System
    open System.Runtime.Serialization

    open Nessos.MBrace.CloudExpr

    // TODO : move specialized exceptions to Runtime assemblies

    [<AutoOpen>]
    module private ExceptionUtils =

        let inline write<'T> (sI : SerializationInfo) (name : string) (x : 'T) =
            sI.AddValue(name, x, typeof<'T>)

        let inline read<'T> (sI : SerializationInfo) (name : string) =
            sI.GetValue(name, typeof<'T>) :?> 'T

    [<Serializable>]
    /// The base type for all the MBrace system exceptions.
    type MBraceException =
        inherit System.Exception

        new () = { inherit System.Exception() }
        new (message : string, ?inner : exn) = 
            match inner with
            | None -> { inherit System.Exception(message) }
            | Some e -> { inherit System.Exception(message, e) }

        new (x : SerializationInfo, y : StreamingContext) = { inherit System.Exception(x, y) }

    [<Serializable>]
    /// Represents a failure in executing an operation in the underlying store.
    type StoreException = 
        inherit MBraceException
    
        new (msg : string, ?inner : exn) = { inherit MBraceException(msg, ?inner = inner) }
        new (x : SerializationInfo, y : StreamingContext) = { inherit MBraceException(x, y) }

    [<Serializable>]
    /// Represents a failure in executing a dereference operation in the cloud ref primitives.
    type NonExistentObjectStoreException(container : string, name : string) = 
        inherit StoreException(sprintf "Object %s - %s has been disposed of or does not exist." container name)

        member __.Container with get () = container
        member __.Name      with get () = name

    [<Serializable>]
    /// Represents the fact that the system got in a corrupted state.
    type SystemCorruptedException =
        inherit MBraceException

        new (msg : string, ?inner : exn) = { inherit MBraceException(msg, ?inner = inner) }
        new (x : SerializationInfo, y : StreamingContext) = { inherit MBraceException(x, y) }

    [<Serializable>]
    /// Represents a failure in an operation executed by the runtime.
    type SystemFailedException =
        inherit MBraceException

        new (msg : string, ?inner : exn) = { inherit MBraceException(msg, ?inner = inner) }
        new (x : SerializationInfo, y : StreamingContext) = { inherit MBraceException(x, y) }

    [<Serializable>]
    /// Represents a failure to obtain a process' result because the process was killed.
    type ProcessKilledException =
        inherit MBraceException
            
        new (msg : string, ?inner : exn) = { inherit MBraceException(msg, ?inner = inner) }
        new (x : SerializationInfo, y : StreamingContext) = { inherit MBraceException(x, y) }


    [<Serializable>]
    /// Represents an exception thrown by user code.
    type CloudException =
        inherit MBraceException

        val private processId : ProcessId
        val private context : CloudDumpContext option

        new(message : string, processId : ProcessId, ?inner : exn, ?context : CloudDumpContext) =
            {
                inherit MBraceException(message, ?inner = inner)
                processId = processId
                context = context
            }

        new (inner : exn, processId : ProcessId, ?context : CloudDumpContext) =
            let message = sprintf "%s: %s" (inner.GetType().FullName) inner.Message
            CloudException(message, processId, inner = inner, ?context = context)

        internal new (si : SerializationInfo, sc : StreamingContext) =
            {
                inherit MBraceException(si, sc)
                processId = read<ProcessId> si "processId"
                context = read<CloudDumpContext option> si "cloudDumpContext"
            }

        // TODO: implement a symbolic trace
//        member __.Trace = inner.StackTrace

        /// The identifier of the corresponding cloud process.
        member e.ProcessId = e.processId
        /// The filename.
        member e.File = match e.context with Some c -> c.File | None -> "unknown"
        /// The name of the function that threw an exception.
        member e.FunctionName = match e.context with Some c -> c.FunctionName | None -> "unknown"
        /// The row and column of the expression start.
        member e.StartPos = match e.context with Some c -> c.Start | None -> (-1,-1)
        /// The row and column of the expression end.
        member e.EndPos = match e.context with Some c -> c.End | None -> (-1,-1)
        /// Variables captured by the expression.
        member e.Environment = match e.context with Some c -> c.Vars | None -> [||]
    
        override e.GetObjectData(si : SerializationInfo, sc : StreamingContext) =
            base.GetObjectData(si, sc)
            write<ProcessId> si "processId" e.processId
            write<CloudDumpContext option> si "cloudDumpContext" e.context

        interface ISerializable with
            member e.GetObjectData(si : SerializationInfo, sc : StreamingContext) = e.GetObjectData(si, sc)