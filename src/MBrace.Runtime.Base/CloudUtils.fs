namespace Nessos.MBrace.Runtime

    module CloudUtils =

        open System
        open System.Reflection

        open Microsoft.FSharp.Quotations
        open Microsoft.FSharp.Quotations.Patterns

        open Nessos.MBrace
        open Nessos.MBrace.Utils
        open Nessos.MBrace.Utils.Reflection
        open Nessos.MBrace.Utils.Quotations

        let rec yieldsICloud (t : Type) =
            if typeof<ICloud>.IsAssignableFrom(t) then true else
            
            match t with
            | FSharpFunc(_,resultT) -> yieldsICloud resultT
            | Named(_, genericArgs) -> Array.exists yieldsICloud genericArgs
            | Param _ -> false
            | Array(t,_) -> yieldsICloud t
            | Ptr(_,t) -> yieldsICloud t
            
        let isCloudPrimitive (t : Type) = t.Assembly = typeof<ICloud>.Assembly

        /// matches against a property whose return type contains cloud blocks
        let (|CloudProperty|_|) (propInfo : PropertyInfo) =
            if yieldsICloud propInfo.PropertyType && not <| isCloudPrimitive propInfo.DeclaringType then
                Some propInfo
            else None

        /// matches against a method whose return type contains cloud blocks
        let (|CloudMethod|_|) (methodInfo : MethodInfo) =
            let gmeth = if methodInfo.IsGenericMethod then methodInfo.GetGenericMethodDefinition() else methodInfo

            if yieldsICloud gmeth.ReturnType && not <| isCloudPrimitive gmeth.DeclaringType then
                Some methodInfo
            else None

        /// matches against a method or property that yields ICloud
        let (|CloudMoP|_|) (m : MethodOrProperty) =
            match m with
            | PropertyGetter(CloudProperty _)
            | MethodCall(CloudMethod _) -> Some m
            | _ -> None

        let (|CloudBuilder|_|) (name : string) (m : MethodInfo) =
            if m.DeclaringType = typeof<CloudBuilder> && m.Name = name then
                Some ()
            else
                None

        /// recognizes a top-level 'cloud { }' expression
        let (|CloudBuilderExpr|_|) (e : Expr) =
            match e with
            | Application(Lambda(bv,body),PropertyGet(None,_,[])) 
                when bv.Type = typeof<CloudBuilder> -> Some body
            | _ -> None

        /// recognized monadic return
        let (|CloudReturn|_|) (e : Expr) =
            match e with
            | Call(Some(Var _), CloudBuilder "Return", [value]) -> Some value
            | _ -> None

        /// recognizes monadic return!
        let (|CloudReturnFrom|_|) (e : Expr) =
            match e with
            | Call(Some(Var _), CloudBuilder "ReturnFrom", [expr]) -> Some expr
            | _ -> None

        /// recognizes monadic bind
        let (|CloudBind|_|) (e : Expr) =
            match e with
            | Call(Some(Var _), CloudBuilder "Bind", [body ; Lambda(v, cont)]) -> 
                match cont with
                | Let(v', Var(v''), cont) when v = v'' -> Some(v', body, cont)
                | _ -> Some(v, body, cont)
            | _ -> None

        /// recognizes monadic combine
        let (|CloudCombine|_|) (e : Expr) =
            match e with
            | Call(Some(Var _), CloudBuilder "Combine", [f ; g]) -> Some(f,g)
            | _ -> None

        /// recognizes monadic delay
        let (|CloudDelay|_|) (e : Expr) =
            match e with
            | Call(Some(Var _), CloudBuilder "Delay", [Lambda(unitVar, body)]) when unitVar.Type = typeof<unit> -> Some body
            | _ -> None

        /// recognizes monadic use bindings
        let rec (|CloudUsing|_|) (e : Expr) =
            match e with
            | Call(Some(Var _), CloudBuilder "Using", [_ ; Lambda(_, Let(v,body,cont)) ]) -> Some(v,body, cont)
            | CloudBind(_, b, Let(_,_,CloudUsing(v,_,cont))) -> Some(v,b,cont)
            | _ -> None

        /// recognizes monadic try/with
        let (|CloudTryWith|_|) (e : Expr) =
            match e with
            | Call(Some(Var _), CloudBuilder "TryWith", [CloudDelay f ; Lambda(_, body)]) -> Some(f, body)
            | _ -> None

        /// recognizes monadic try/finally
        let (|CloudTryFinally|_|) (e : Expr) =
            match e with
            | Call(Some(Var _), CloudBuilder "TryFinally", [CloudDelay f; ff]) -> Some(f, ff)
            | _ -> None

        /// recognizes a monadic for loop
        let (|CloudFor|_|) (e : Expr) =
            match e with
            | Call(Some(Var _), CloudBuilder "For" , [inputs ; Lambda(_, Let(v,_,body)) ]) -> Some(inputs, v, body)
            | _ -> None

        /// recognizes a monadic while loop
        let (|CloudWhile|_|) (e : Expr) =
            match e with
            | Call(Some(Var _), CloudBuilder "While" , [cond ; CloudDelay f]) -> Some(cond, f)
            | _ -> None

        type MethodOrProperty with
            member self.yieldsICloud () =
                match self with CloudMoP _ -> true | _ -> false

        type Quotation with
            member self.yieldsICloud() =
                match self.ReflectedMethod with
                | None -> self.Expr.Type |> yieldsICloud
                | Some r -> r.yieldsICloud ()