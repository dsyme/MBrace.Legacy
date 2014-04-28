namespace Nessos.MBrace
    /// Adding this attribute to a let-binding marks that
    /// the value definition contains cloud expressions.
    /// This attribute is mandatory.
    type CloudAttribute = ReflectedDefinitionAttribute

    /// Represents a computation that will be executed on the cloud.
    type ICloud =
        interface
            abstract member ReturnType : System.Type
        end

    /// Represents a computation that will be executed on the cloud.
    and ICloud<'T> =
        interface
            inherit ICloud
        end

    /// Represents the result of a cloud computation.
    and Result<'T> =
        | ValueResult of 'T
        | ExceptionResult of (exn * CloudDumpContext option)
    and internal CloudExprWrap =
        class
            internal new : cloudExpr:CloudExpr -> CloudExprWrap
            member internal CloudExpr : CloudExpr
        end
    and internal CloudExprWrap<'T> =
        class
            inherit CloudExprWrap
            interface ICloud<'T>
            internal new : cloudExpr:CloudExpr -> CloudExprWrap<'T>
        end
    and internal Container = string
    and internal Id = string
    and internal Tag = string
    and internal ObjFunc = obj
    and ProcessId = int
    and internal CloudExpr =
        | DelayExpr of (unit -> CloudExpr) * ObjFunc
        | BindExpr of CloudExpr * (obj -> CloudExpr) * ObjFunc
        | ReturnExpr of obj * System.Type
        | TryWithExpr of CloudExpr * (exn -> CloudExpr) * ObjFunc
        | TryFinallyExpr of CloudExpr * (unit -> unit)
        | ForExpr of obj [] * (obj -> CloudExpr) * ObjFunc
        | WhileExpr of (unit -> bool) * CloudExpr
        | CombineExpr of CloudExpr * CloudExpr
        | DisposableBindExpr of ICloudDisposable * System.Type * (obj -> CloudExpr) * ObjFunc
        | GetWorkerCountExpr
        | GetProcessIdExpr
        | GetTaskIdExpr
        | LocalExpr of CloudExpr
        | OfAsyncExpr of ICloudAsync
        | ParallelExpr of CloudExpr [] * System.Type
        | ChoiceExpr of CloudExpr [] * System.Type
        | QuoteExpr of Quotations.Expr
        | LogExpr of string
        | TraceExpr of CloudExpr
        | NewRefByNameExpr of Container * obj * System.Type
        | GetRefsByNameExpr of Container
        | GetRefByNameExpr of Container * Id * System.Type
        | NewMutableRefByNameExpr of Container * Id * obj * System.Type
        | ReadMutableRefExpr of IMutableCloudRef * System.Type
        | SetMutableRefExpr of IMutableCloudRef * obj
        | ForceSetMutableRefExpr of IMutableCloudRef * obj
        | GetMutableRefsByNameExpr of Container
        | GetMutableRefByNameExpr of Container * Id * System.Type
        | FreeMutableRefExpr of IMutableCloudRef
        | NewCloudSeqByNameExpr of Container * System.Collections.IEnumerable * System.Type
        | GetCloudSeqByNameExpr of Container * Id * System.Type
        | GetCloudSeqsByNameExpr of Container
        
        | NewCloudFile of Container * Id * (System.IO.Stream -> Async<unit>)
        | GetCloudFile  of Container * Id
        | GetCloudFiles of Container
        //| ReadCloudFile of ICloudFile * CloudFileInterpretation
        | ReadCloudFile of ICloudFile * (System.IO.Stream -> Async<obj>) * System.Type
        | ReadCloudFileAsSeq of ICloudFile * (System.IO.Stream -> Async<obj>) * System.Type

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
        | ValueExpr of Value
    and internal Value =
        | Obj of ObjValue * System.Type
        | Exc of exn * CloudDumpContext option
        | ParallelValue of (CloudExpr [] * System.Type)
        | ParallelThunks of (ThunkValue [] * System.Type)
        | ChoiceValue of (CloudExpr [] * System.Type)
        | ChoiceThunks of (ThunkValue [] * System.Type)
    and ObjValue =
        | ObjValue of obj
        | CloudRefValue of ICloudRef<obj>
    and internal ThunkValue =
        | Thunk of CloudExpr
        | ThunkId of string
    and internal ICloudAsync =
        interface
            abstract member UnPack : IPolyMorphicMethodAsync -> unit
        end
    and internal IPolyMorphicMethodAsync =
        interface
            abstract member Invoke : Async<'T> -> unit
        end
    
    /// Represents an object that can be disposed in a distributed context.
    /// Objects implementing this interface can be placed in `use` expressions
    /// in cloud workflows.
    and ICloudDisposable =
        inherit System.Runtime.Serialization.ISerializable
        /// Performs actions such as freeing, releasing, etc resources in a distributed context.
        abstract Dispose : unit -> Async<unit>

    /// Represents an immutable reference to an
    /// object that is stored in the underlying store.
    and ICloudRef =
        interface
            inherit ICloudDisposable
            /// The CloudRef's container (folder).
            abstract member Container : string
            /// The CloudRef's name.
            abstract member Name : string
            /// The value of the object stored in the CloudRef.
            abstract member TryValue : obj option
            /// The type of the object stored in the CloudRef.
            abstract member Type : System.Type
            /// The value of the object stored in the CloudRef.
            abstract member Value : obj
        end

    /// Represents a reference to an
    /// object that is stored in the global store.
    and ICloudRef<'T> =
        interface
            inherit ICloudRef
            /// The value of the object stored in the CloudRef.
            abstract member TryValue : 'T option
            /// The value of the object stored in the CloudRef.
            abstract member Value : 'T
        end

    /// Represents a finite and immutable sequence of
    /// elements that is stored in the underlying store
    /// and will be enumerated on demand.
    and ICloudSeq =
        interface
            inherit ICloudDisposable

            /// The CloudSeq's container (folder).
            abstract member Container : string
            /// The number of elements contained in the CloudSeq.
            abstract member Count : int
            /// The CloudSeq's name.
            abstract member Name : string
            /// The size of the CloudSeq in the underlying store.
            /// The value is in bytes and might be an approximation.
            abstract member Size : int64
            /// The type of the object stored in the CloudRef.
            abstract member Type : System.Type
        end

    /// Represents a finite and immutable sequence of
    /// elements that is stored in the underlying store
    /// and will be enumerated on demand.
    and ICloudSeq<'T> =
        interface
            inherit ICloudSeq
            inherit System.Collections.Generic.IEnumerable<'T>
            inherit System.Collections.IEnumerable
        end

    /// Represents a mutable reference to an
    /// object that is stored in the underlying store.
    and IMutableCloudRef =
        interface
            inherit ICloudDisposable

            /// The MutableCloudRef's container (folder).
            abstract member Container : string
            /// The MutableCloudRef's name.
            abstract member Name : string
            /// The type of the object stored in the MutableCloudRef.
            abstract member Type : System.Type
        end

    /// Represents a mutable reference to an
    /// object that is stored in the underlying store.
    and IMutableCloudRef<'T> =
        interface
            inherit IMutableCloudRef
        end

    /// Represents an untyped object (a file actually) that
    /// exists in the store.
    and ICloudFile = 
        interface
            inherit ICloudDisposable
            /// The CloudFile's name in the store.
            abstract Name : string
            /// The folder containing the CloudFile.
            abstract Container : string
        end

    and internal IMutableCloudRefTagged =
        interface
            inherit IMutableCloudRef
            abstract member Tag : Tag
            abstract member Tag : Tag with set
        end
    and internal IMutableCloudRefTagged<'T> =
        interface
            inherit IMutableCloudRefTagged
        end
    and internal CloudDumpContext =
        {File: string;
         Start: int * int;
         End: int * int;
         CodeDump: string;
         FunctionName: string;
         Vars: (string * obj) [];}
    and internal CloudPackage =
        class
            private new : expr:Quotations.Expr * t:System.Type -> CloudPackage
            member Expr : Quotations.Expr
            member ReturnType : System.Type
            static member Create : expr:Quotations.Expr<ICloud<'T>> -> CloudPackage
        end
    and  [<System.AttributeUsage(System.AttributeTargets.Class ||| System.AttributeTargets.Method ||| System.AttributeTargets.Property ||| System.AttributeTargets.Constructor, AllowMultiple = false)>]
         [<Sealed>]
         /// Adding this attribute to a let-binding marks that the Tracing mechanism is disabled for the annotated let scope.
         NoTraceInfoAttribute =
            class
                inherit System.Attribute
                new : unit -> NoTraceInfoAttribute
                member Name : string
            end
