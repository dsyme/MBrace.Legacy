// Contains types used by the MBrace interpreter. Normally user code should not use any of these types.
namespace Nessos.MBrace.CloudExpr

    open System
    open System.IO
    open System.Collections

    open Nessos.MBrace

    /// Represents an untyped cloud computation expression tree.
    type CloudExpr = 
        // Monadic Exprs
        | DelayExpr of (unit -> CloudExpr) * ObjFunc
        | BindExpr of CloudExpr * (obj -> CloudExpr) * ObjFunc
        | ReturnExpr of obj * Type
        | TryWithExpr of CloudExpr * (exn -> CloudExpr) * ObjFunc
        | TryFinallyExpr of CloudExpr * (unit -> unit)
        | ForExpr of obj [] * (obj -> CloudExpr) * ObjFunc
        | WhileExpr of (unit -> bool) * CloudExpr 
        | CombineExpr of CloudExpr * CloudExpr
        | DisposableBindExpr of ICloudDisposable * System.Type * (obj -> CloudExpr) * ObjFunc
        // Primitives
        | GetWorkerCountExpr 
        | GetProcessIdExpr 
        | GetTaskIdExpr 
        | LocalExpr of CloudExpr 
        | OfAsyncExpr of IAsyncContainer
        | ParallelExpr of CloudExpr [] * Type
        | ChoiceExpr of CloudExpr [] * Type
        | LogExpr of string
        | TraceExpr of CloudExpr 

        (* CLOUDREF *)
        | NewRefByNameExpr  of Container * obj * Type
        | GetRefsByNameExpr of Container
        | GetRefByNameExpr  of Container * Id * Type

        (* MUTABLE CLOUDREF *)
        | NewMutableRefByNameExpr  of Container        * Id * obj * Type
        | ReadMutableRefExpr       of IMutableCloudRef * Type
        | SetMutableRefExpr        of IMutableCloudRef * obj
        | ForceSetMutableRefExpr   of IMutableCloudRef * obj
        | GetMutableRefsByNameExpr of Container
        | GetMutableRefByNameExpr  of Container        * Id * Type
        | FreeMutableRefExpr       of IMutableCloudRef

        (* CLOUDSEQ *)
        | NewCloudSeqByNameExpr  of Container * IEnumerable * Type
        | GetCloudSeqByNameExpr  of Container * Id          * Type
        | GetCloudSeqsByNameExpr of Container 

        (* CLOUDFILE *)
        | NewCloudFile          of Container  * Id * (Stream -> Async<unit>)
        | GetCloudFile          of Container  * Id
        | GetCloudFiles         of Container
        | ReadCloudFile         of ICloudFile * (Stream -> Async<obj>) * Type

        (* CLOUDARRAY *)
        | NewCloudArray         of Container * IEnumerable * Type
        | GetCloudArray         of Container  * Id * Type
        | GetCloudArrays        of Container

        // Commands
        | DoEndDelayExpr of ObjFunc
        | DoEndBindExpr of obj * ObjFunc
        | DoEndTryWithExpr of obj * ObjFunc
        | DoBindExpr of (obj -> CloudExpr) * ObjFunc
        | DoTryWithExpr of (exn -> CloudExpr) * ObjFunc
        | DoTryFinallyExpr of (unit -> unit)
        | DoForExpr of obj [] * int * (obj -> CloudExpr) * ObjFunc
        | DoWhileExpr of (unit -> bool) * CloudExpr 
        | DoCombineExpr of CloudExpr 
        | DoEndTraceExpr
        | DoDisposableBindExpr of ICloudDisposable
        // Value
        | ValueExpr of Value

    /// [omit]
    and Value = 
        | Obj of ObjValue * Type 
        | Exc of exn * CloudDumpContext option
        | ParallelValue of (CloudExpr [] * Type)
        | ParallelThunks of (ThunkValue [] * Type)
        | ChoiceValue of (CloudExpr [] * Type)
        | ChoiceThunks of (ThunkValue [] * Type)

    /// [omit]
    and ObjValue =
        | ObjValue of obj
        | CloudRefValue of ICloudRef<obj>

    /// Represents the result of a cloud computation.
    and Result<'T> = 
        | ValueResult of 'T 
        | ExceptionResult of (exn * CloudDumpContext option) //ValueResult | ExceptionResult are valid process results

    /// The container of an object stored in the CloudStore.
    and Container = string
    /// The identifier of an object stored in the CloudStore.
    and Id = string
    /// [omit]
    and ObjFunc = obj
    /// The identifier of a cloud process.
    and ProcessId = int
    /// The identifier of a task.
    and TaskId = string

    /// [omit]
    and ThunkValue = Thunk of CloudExpr | ThunkId of string

    /// [omit]
    and IAsyncContainer = 
        abstract Unpack<'R> : IAsyncConsumer<'R> -> 'R

    /// [omit]
    and IAsyncConsumer<'R> = 
        abstract Invoke<'T> : Async<'T> -> 'R

    /// Contains debug information.
    and CloudDumpContext =
        {
            ///Filename
            File : string
            ///Start position (row and column) of the expression.
            Start : int * int // row * col
            ///Endposition (row and column) of the expression.
            End : int * int
            ///The expression dump.
            CodeDump : string
            ///The function's name.
            FunctionName : string
            ///Variables captured by the expression.
            Vars : (string * obj) []
        }