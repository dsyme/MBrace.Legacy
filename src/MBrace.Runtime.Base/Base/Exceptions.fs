//namespace Nessos.MBrace
//
//    open System
//    open System.Runtime.Serialization
//
//    open Nessos.MBrace
//    open Nessos.MBrace.Client
//    open Nessos.MBrace.Utils
//    open Nessos.MBrace.Utils.String
//
//
//    [<Serializable>]
//    /// Represents an exception thrown by user code.
//    type CloudException internal (message : string, inner : exn, processId : ProcessId, ?context : CloudDumpContext) =
//        inherit MBraceException(message, inner)
//
//        internal new (inner : exn, processId : ProcessId, ?context : CloudDumpContext) =
//            let message = sprintf' "%s: %s" (inner.GetType().FullName) inner.Message
//            CloudException(message, inner, processId, ?context = context)
//
//        // TODO: implement a symbolic trace
//        // member __.Trace = inner.StackTrace
//        member __.ProcessId = processId
//        member __.File = context |> Option.bind2 (fun c -> c.File) "unknown"
//        member __.FunctionName = context |> Option.bind2 (fun c -> c.FunctionName) "unknown"
//        member __.StartPos = context |> Option.bind2 (fun c -> c.Start) (-1,-1)
//        member __.EndPos = context |> Option.bind2 (fun c -> c.End) (-1,-1)
//        member __.Environment = context |> Option.bind2 (fun c -> c.Vars) [||]
//
//        new (s : SerializationInfo, _ : StreamingContext) =
//            CloudException(s.Get(), s.Get(), ?context = s.Get())
//
//        interface ISerializable with
//            member e.GetObjectData(s : SerializationInfo, _ : StreamingContext) =
//                s.Set inner
//                s.Set processId
//                s.Set context
//
//        override e.ToString() =
//            string {
//                yield sprintf' "MBrace.CloudException: %s\n" e.Message
//
//                yield sprintf' "--- Begin {m}brace dump ---\n"
//
//                yield sprintf' " Exception:\n" 
//                yield "  " + e.InnerException.ToString() + "\n"
//
//                match context with
//                | None -> ()
//                | Some ctx ->
//                    yield sprintf' " File: %s \n" ctx.File 
//                    yield sprintf' " Function: %s line: %A\n" ctx.FunctionName (fst ctx.Start)
//                    //yield sprintf' " CodeDump: %s \n" ctx.CodeDump // SECD Dump rethinking
//
//                    if ctx.Vars.Length > 0 then 
//                        yield sprintf' " Environment:\n"
//                        for name, value in ctx.Vars do
//                            yield sprintf' "\t\t %s -> %A\n" name value
//
//                yield sprintf' "--- End {m}brace dump ---\n"
//    
//                yield e.StackTrace
//            } |> String.build
//                
//
//    [<Serializable>]
//    /// Represents one or more exceptions thrown by user code in a Cloud.Parallel context.
//    type ParallelCloudException(results : Result<ObjValue> []) =
//        inherit MBraceException()
//            new (info : SerializationInfo, context : StreamingContext) = 
//                    ParallelCloudException(info.GetValue("results", typeof<Result<ObjValue> []>) :?> Result<ObjValue> [])
//
//            member self.Results = results
//            member self.Exceptions = results |> Array.choose (fun result -> match result with  ExceptionResult _ -> Some result | _ -> None) 
//            member self.Values = results |> Array.choose (fun result -> match result with ValueResult _ -> Some result | _ -> None) 
//
//            override self.ToString() = 
//                //sprintf "%A" results
//
//                // workaround because of the FUUUUUUCKING string monad.
//                let mystring : obj -> string = sprintf "%A" // MAGIC
//                
//                results
//                |> Array.map (function 
//                    | (ValueResult v) -> 
//                        match v with 
//                        | CloudRefValue o -> sprintf "ValueResult (%A)" o.Value 
//                        | _ as x -> mystring x
//                    | _ as e -> mystring e)
//                |> mystring
//
//
//        interface ISerializable with      
//            override self.GetObjectData(info : SerializationInfo, context : StreamingContext) : unit = 
//                info.AddValue("results", results)  


namespace Nessos.MBrace.Runtime

    open Nessos.MBrace
    
    [<AutoOpen>]
    module MBraceException =

        let mkMBraceExn inner msg = 
            match inner with
            | Some (inner : exn) -> MBraceException (msg, inner)
            | None -> MBraceException msg

        let inline mfailwith msg = mkMBraceExn None msg |> raise
        let inline mfailwithInner exn msg = mkMBraceExn (Some exn) msg |> raise
        let inline mfailwithf fmt = Printf.ksprintf(mfailwith) fmt
        let inline mfailwithfInner exn fmt = Printf.ksprintf (mfailwithInner exn) fmt

        let rec (|MBraceExn|_|) (e : exn) =
            match e with
            | :? MBraceException as mbe -> Some mbe
            | e when e.InnerException <> null -> (|MBraceExn|_|) e.InnerException
            | _ -> None