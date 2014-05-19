namespace Nessos.MBrace

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization
    open Microsoft.FSharp.Quotations

    type CloudAttribute = ReflectedDefinitionAttribute

    // keep interface name for sake of compatibility; change later

    [<AbstractClass>]
    type ICloud internal (cloudExpr : CloudExpr) =
        abstract Type : Type
        member internal __.CloudExpr = cloudExpr

    and ICloud<'T> internal (cloudExpr : CloudExpr) =
        inherit ICloud(cloudExpr)
        override __.Type = typeof<'T>

    and internal CloudExpr = 
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
        | OfAsyncExpr of ICloudAsync
        | ParallelExpr of CloudExpr [] * Type
        | ChoiceExpr of CloudExpr [] * Type
//        | QuoteExpr of Expr
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
//        | ReadCloudFileAsSeq    of ICloudFile * (System.IO.Stream -> Async<obj>) * System.Type

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

    and internal Value = 
        | Obj of ObjValue * Type 
        | Exc of exn * CloudDumpContext option
        | ParallelValue of (CloudExpr [] * Type)
        | ParallelThunks of (ThunkValue [] * Type)
        | ChoiceValue of (CloudExpr [] * Type)
        | ChoiceThunks of (ThunkValue [] * Type)

    and ObjValue =
        | ObjValue of obj
        | CloudRefValue of ICloudRef<obj>

    and Result<'T> = 
        | ValueResult of 'T 
        | ExceptionResult of (exn * CloudDumpContext option) //ValueResult | ExceptionResult are valid process results

    and internal Container = string
    and internal Id = string
    and internal Tag = string
    and internal ObjFunc = obj
    // runtime artifact ; maybe move elsewhere?
    and ProcessId = int

    and internal ThunkValue = Thunk of CloudExpr | ThunkId of string

    and internal ICloudAsync = 
        abstract UnPack : IPolyMorphicMethodAsync -> unit

    and internal IPolyMorphicMethodAsync = 
        abstract Invoke<'T> : Async<'T> -> unit

    and CloudDumpContext =
        {
            File : string
            Start : int * int // row * col
            End : int * int
            CodeDump : string
            FunctionName : string
            Vars : (string * obj) []
        }

    and [<System.AttributeUsage(System.AttributeTargets.Class ||| System.AttributeTargets.Method ||| System.AttributeTargets.Property ||| System.AttributeTargets.Constructor, AllowMultiple = false)>]
         [<Sealed>]
         NoTraceInfoAttribute() = 
            inherit System.Attribute()
            member self.Name = "NoTraceInfo"