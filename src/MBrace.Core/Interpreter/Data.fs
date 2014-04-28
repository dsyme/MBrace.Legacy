[<AutoOpen>]
module internal Nessos.MBrace.Core.Data

    open System
    open System.Reflection
    open System.Runtime.Serialization
    
    open Microsoft.FSharp.Quotations

    open Nessos.Thespian.PowerPack

    open Nessos.MBrace
    open Nessos.MBrace.Utils

    type VariableName = String
    and Variable = VariableName * Type
    and FunctionName = String
    and ThunkId = String
    and ExprId = String
    and ElementType = Type
    and ReturnType = Type
//    and ICloudClosure = 
//        abstract Expression : Expression
//        abstract Enviroment : (VariableName * TagObj) []
//        abstract Type : Type
//    and ICloudParallelClosure = 
//        abstract Computations : ICloud []
//        abstract ElementType : ElementType
//    and ICloudChoiceClosure = 
//        abstract Computations : ICloud []
//        abstract ElementType : ElementType
//    and ICloudRunLocal = 
//        abstract LocalCloud : ICloud
//    and ICloudRefNew = 
//        abstract Value : obj
//        abstract Type : Type
//    and ICloudRefGet = 
//        abstract Ref : ICloudRef
//    and ICloudGetWorkerCount = interface end
//    and ILambdaClosure = 
//            abstract Var : VariableName
//            abstract Body : Expression
//            abstract Enviroment : (VariableName * obj) []
//            abstract Invoke : obj -> obj
//    and    
//        CloudClosure<'T>(expression : Expression, enviroment : (VariableName * TagObj) []) =
//        new (info : SerializationInfo, context : StreamingContext) = 
//                CloudClosure<'T>(   info.GetValue("expression", typeof<Expression>) :?> Expression,
//                                    info.GetValue("enviroment", typeof<(VariableName * TagObj) []>) :?> (VariableName * TagObj) [])
//
//        interface ICloudClosure with
//            member self.Expression with get() = expression
//            member self.Enviroment with get() = enviroment
//            member self.Type with get() = typeof<'T>
//        interface ICloud<'T> with
//            member self.ReturnType = typeof<'T>
//        interface ISerializable with
//                member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
//                    info.AddValue("expression", expression)
//                    info.AddValue("enviroment", enviroment)
//    and
//       CloudParallelClosure<'T>(computations : ICloud []) = 
//
//       interface ICloudParallelClosure with
//            member self.Computations with get() = computations 
//            member self.ElementType with get()  = typeof<'T>
//       interface ICloud<'T []> with
//            member self.ReturnType = typeof<'T>
//    and CloudChoiceClosure<'T>(computations : ICloud []) = 
//
//        interface ICloudChoiceClosure with
//            member self.Computations with get() = computations 
//            member self.ElementType with get()  = typeof<'T>
//        interface ICloud<'T option> with
//            member self.ReturnType = typeof<'T>
//    and
//        CloudAsync<'T>(computation : Async<'T>) =
//
//        interface ICloudAsync with
//            member self.UnPack (polyMorpInvoker : IPolyMorphicMethodAsync) =
//                polyMorpInvoker.Invoke<'T>(computation)
//        interface ICloud<'T> with
//            member self.ReturnType = typeof<'T>
//
//    and CloudRunLocal<'T>(computation : ICloud<'T>) =
//
//        interface ICloudRunLocal with
//            member self.LocalCloud with get() = computation :> _
//        interface ICloud<'T> with
//            member self.ReturnType = typeof<'T>
//
//    and CloudRefNew<'T>(value : 'T) =
//
//        interface ICloudRefNew with
//            member self.Value with get() = value :> obj
//            member self.Type with get() = typeof<'T>
//        interface ICloud<ICloudRef<'T>> with
//            member self.ReturnType = typeof<ICloudRef<'T>>
//
//    and CloudRefGet<'T>(cloudRef : ICloudRef<'T>) =
//
//        interface ICloudRefGet with
//            member self.Ref with get() = cloudRef :> ICloudRef
//            
//        interface ICloud<'T> with
//            member self.ReturnType = typeof<'T>
//
//    and CloudGetWorkerCount() =
//
//        interface ICloudGetWorkerCount
//            
//        interface ICloud<int> with
//            member self.ReturnType = typeof<int>
//
//    and LambdaClosure<'Arg, 'Result>(var : VariableName, body : Expression, enviroment : (VariableName * obj) []) =
//        inherit FSharpFunc<'Arg, 'Result>() 
//        // Constructor
//        let (funId, functionName, freeVars, bodyExpr) =  match body with | FSharpExpr (funId, functionName, freeVars, expr) -> (funId, functionName, freeVars, expr) | _ -> throwInvalidState body
//        let enviroment' = lazy (List.ofArray enviroment)
//        let tryLambdaRef : ('Arg -> 'Result) option ref = ref None
//
//        new (info : SerializationInfo, context : StreamingContext) = 
//            LambdaClosure<'Arg, 'Result>(info.GetValue("var", typeof<string>) :?> string, 
//                                            info.GetValue("body", typeof<Expression>) :?> Expression, 
//                                            info.GetValue("enviroment", typeof<(VariableName * obj) []>) :?> (VariableName * obj) [])
//
//        override self.Invoke(value : 'Arg) : 'Result = 
//            try
//                match tryLambdaRef.Value with
//                | Some f -> 
//                    let result = f value
//                    result
//                | None ->
//                    match QuotationCompiler.cacheCompiledExprsAtom.Value.TryFind funId with
//                    | Some (Some f) -> 
//                        let f' = (enviroment |> Array.map snd |> f) :?> ('Arg -> 'Result)
//                        tryLambdaRef := Some f'
//                        let result = f' value
//                        result 
//                    | Some None -> 
//                        (Map.ofList ((var, value :> obj) :: enviroment'.Value), bodyExpr) ||> Swensen.Unquote.Operators.evalRawWith<'Result>
//                    | None ->
//                        // Post Compilation Job
//#if QUOTATIONS_COMPILER
//                        Atom.swap QuotationCompiler.cacheCompiledExprsAtom (fun cacheCompiledExprs' -> Map.add funId None cacheCompiledExprs')
//                        let var' = 
//                            match freeVars |> Array.tryFind (fun var' -> var'.Name = var) with
//                            | Some var' -> var'
//                            | None -> new Var(var, typeof<'Arg>)
//                        QuotationCompiler.compileToILActor.Post (funId, functionName, Expr.Lambda(var', bodyExpr))
//#endif
//                        // Normal Interpreter
//                        let result = (Map.ofList ((var, value :> obj) :: enviroment'.Value), bodyExpr) ||> Swensen.Unquote.Operators.evalRawWith<'Result>
//                        result
//            with e ->
//                QuotationCompiler.logger.Value.LogWithException e "LambdaClosure Invoke Error" LogLevel.Error
//                reraise()
//
//
//        interface ILambdaClosure with
//            member self.Var with get() = var
//            member self.Body with get() = body
//            member self.Enviroment with get() = enviroment
//            member self.Invoke (value : obj) = self.Invoke(value :?> 'Arg) :> obj
//
//        interface ISerializable with
//            member self.GetObjectData(info : SerializationInfo, context : StreamingContext) =
//                info.AddValue("var", var)
//                info.AddValue("body", body)
//                info.AddValue("enviroment", enviroment)
//    and
//        Instruction = Command of Command | Expression of Expression
//    and
//        Command = DoApp | DoBind 
//                  | DoLet of (VariableName * Expression) | DoVarSet of VariableName  
//                  | DoTryWith | DoTryFinally | DoApplyValueAfterTryFinally of Value
//                  | DoIfThenElse 
//                  | DoCombine | DoSequential 
//                  | DoFor of Expression | DoWhile of (Expression * Expression)
//                  | DoRun of ReturnType
//    and
//        Expression =
//        | ObjectConst of TagObj 
//        | Variable of Variable
//        | VarSet of (Variable * Expression)
//        | Lambda of (Variable * Expression)
//        | Let of (Variable * Expression * Expression)
//        | LetMutable of (Variable * Expression * Expression)
//        | App of (Expression * Expression)
//        | Return of Expression
//        | ReturnFrom of Expression
//        | Bind of (Expression * Expression)
//        | TryWith of (Expression * Expression)
//        | TryFinally of (Expression * Expression)
//        | Combine of (Expression * Expression)
//        | For of (Expression * Expression)
//        | While of (Expression * Expression)
//        | Sequential of (Expression * Expression)
//        | IfThenElse of (Expression * Expression * Expression)
//        | FSharpExpr of ExprId * FunctionName * Var [] * Expr
//        | Zero
//    and
//        Value =
//        | ParallelThunkExpressionsValue of (ICloud [] * ElementType)
//        | ChoiceThunkExpressionsValue of (ICloud [] * ElementType)
//        | ChoiceThunksValue of (ThunkValue [] * ElementType)
//        | ParallelThunksValue of (ThunkValue [] * ElementType)
//        | ObjectValue of TagObj 
//        | ExceptionValue of (exn * MBrace.CloudExceptionContext option) 
//        | Closure of (Environment * Variable * Expression)
    and Row = int
    and Col = int
    and File = string
    and FunctionInfo = { MethodInfo : System.Reflection.MethodInfo; File : string; StartRow : int; StartCol : int; EndRow : int; EndCol : int; Expr : Expr }
    and Function = FunctionInfo 
//    and 
//        ThunkValue = ThunkValue of Value | ThunkIdValue of ThunkId
//    and
//        Stack = Value list
//    and
//        Environment = Map<VariableName, Value>
//    and
//        Code = Instruction list 
    and
        //Dump = Empty | Dump of (Stack * Environment * Code * Dump)
        Dump = Dump of CloudExpr list
