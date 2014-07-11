namespace Nessos.MBrace.Runtime

    open System

    open Nessos.Thespian
    open Nessos.Vagrant
        
    open Nessos.MBrace
    open Nessos.MBrace.CloudExpr
    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime.Compiler

    /// Process execution state
    type ProcessState =
        | Initialized
        | Created 
        | Running 
        | Completed 
        | Failed 
        | Killed
    with
        override s.ToString() = sprintf "%A" s

    and ProcessResultImage =
        | Pending
        | InitError of exn
        | Success of Pickle<obj>
        | UserException of Pickle<exn * CloudDumpContext option>
        | Fault of exn
        | Killed
    with
        static member OfResult(r : Result<obj>) =
            match r with
            | ValueResult o -> Success <| Serialization.Pickle o
            | ExceptionResult(e, ctx) -> UserException <| Serialization.Pickle (e,ctx)

        static member GetUserExceptionInfo (image : ProcessResultImage) =
            match image with
            | UserException p -> Serialization.UnPickle p
            | _ -> invalidOp "not an exception branch"

        static member GetUserValue (image : ProcessResultImage) =
            match image with
            | Success value -> Serialization.UnPickle value
            | _ -> invalidOp "not a value branch"

    /// Cloud process info with serialized entries.
    and ProcessInfo =
        {
            Name : string
            ProcessId : ProcessId
            TypeRaw : Pickle<Type>
            InitTime : DateTime
            ExecutionTime : TimeSpan
            TypeName : string
            Workers : int
            Tasks : int
            ResultRaw : ProcessResultImage
            State: ProcessState
            Dependencies : AssemblyId list
            ClientId : Guid
        }
    with
        member info.Type = Serialization.UnPickle info.TypeRaw

    /// ProcessManager actor API
    and ProcessManager =
        //Throws
        //MBrace.Exception => Failed to activate process
        //MBrace.SystemCorruptedException => system corruption while trying to activate process ;; SYSTEM FAULT
        //MBrace.SystemFailedException => SYSTEM FAULT
        | CreateDynamicProcess of IReplyChannel<ProcessInfo> * Guid * CloudComputationImage
        | GetAssemblyLoadInfo of IReplyChannel<AssemblyLoadInfo list> * Guid * AssemblyId list
        | LoadAssemblies of IReplyChannel<AssemblyLoadInfo list> * Guid * PortableAssembly list
        | GetProcessInfo of IReplyChannel<ProcessInfo> * ProcessId
        | GetAllProcessInfo of IReplyChannel<ProcessInfo []>
        | ClearProcessInfo of IReplyChannel<unit> * ProcessId // Clears process from logs if no longer running
        | ClearAllProcessInfo of IReplyChannel<unit> // Clears all inactive processes
        | RequestDependencies of IReplyChannel<PortableAssembly list> * AssemblyId list // Assembly download API
        | KillProcess of IReplyChannel<unit> * ProcessId