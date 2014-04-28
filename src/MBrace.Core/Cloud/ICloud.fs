namespace Nessos.MBrace

    open System
    open System.IO
    open System.Collections
    open System.Collections.Generic
    open System.Runtime.Serialization
    open Microsoft.FSharp.Quotations

    type CloudAttribute = ReflectedDefinitionAttribute

    type ICloud =
        abstract ReturnType : Type
    and ICloud<'T> = 
        inherit ICloud
    and Result<'T> = ValueResult of 'T | ExceptionResult of (exn * CloudDumpContext option) //ValueResult | ExceptionResult are valid process result
    and CloudExprWrap internal (cloudExpr : CloudExpr) =
        member internal self.CloudExpr with get() = cloudExpr
        override self.ToString () = "cloud { . . . }"
    and CloudExprWrap<'T> internal (cloudExpr : CloudExpr) =
        inherit CloudExprWrap(cloudExpr)
        interface ICloud<'T> with
            member self.ReturnType = typeof<'T>
    and internal Container = string
    and internal Id = string
    and internal Tag = string
    and internal ObjFunc = obj
    and ProcessId = int
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
        | QuoteExpr of Expr
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
        | ReadCloudFileAsSeq    of ICloudFile * (System.IO.Stream -> Async<obj>) * System.Type

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
    and internal ThunkValue = Thunk of CloudExpr | ThunkId of string
    and internal ICloudAsync = 
        abstract UnPack : IPolyMorphicMethodAsync -> unit
    and internal IPolyMorphicMethodAsync = 
        abstract Invoke<'T> : Async<'T> -> unit
    and ICloudDisposable =
        inherit ISerializable
        abstract Dispose : unit -> Async<unit>
    and ICloudRef = 
        inherit ISerializable
        inherit ICloudDisposable
        abstract Name : string 
        abstract Container : string
        abstract Type : Type
        abstract Value : obj
        abstract TryValue : obj option
    and ICloudRef<'T> = 
        inherit ICloudRef
        abstract Value : 'T
        abstract TryValue : 'T option
    and ICloudSeq =
        inherit ISerializable
        inherit ICloudDisposable
        abstract Name : string
        abstract Container : string
        abstract Type : Type
        abstract Size : int64
        abstract Count : int
    and ICloudSeq<'T> =
        inherit IEnumerable
        inherit IEnumerable<'T>
        inherit ICloudSeq
    and IMutableCloudRef = 
        inherit ISerializable
        inherit ICloudDisposable
        abstract Name : string
        abstract Container : string
        abstract Type : Type
    and IMutableCloudRef<'T> = 
        inherit IMutableCloudRef
    and ICloudFile =
        inherit ISerializable
        inherit ICloudDisposable
        abstract Name : string
        abstract Container : string
//        abstract ToArray : unit -> byte []
//        abstract AllText : unit -> string
//        abstract AllLines : unit -> string seq
    and internal IMutableCloudRefTagged =
        inherit IMutableCloudRef
        abstract Tag : Tag with get, set
    and internal IMutableCloudRefTagged<'T> =
        inherit IMutableCloudRefTagged

    and CloudDumpContext =
        {
            File : string
            Start : int * int // row * col
            End : int * int
            CodeDump : string
            FunctionName : string
            Vars : (string * obj) []
        }

    // communicates a proof that expr is of type ICloud<ReturnType>
    and CloudPackage private (expr : Expr, t : Type) =
        member __.Expr = expr
        member __.ReturnType = t
        static member Create (expr : Expr<ICloud<'T>>) =
            CloudPackage(expr, typeof<'T>)
    and  [<System.AttributeUsage(System.AttributeTargets.Class ||| System.AttributeTargets.Method ||| System.AttributeTargets.Property ||| System.AttributeTargets.Constructor, AllowMultiple = false)>]
         [<Sealed>]
         NoTraceInfoAttribute() = 
            inherit System.Attribute()
            member self.Name = "NoTraceInfo"
            
        
            
