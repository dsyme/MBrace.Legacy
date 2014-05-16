namespace Nessos.MBrace.Azure.Common

    open System
    open System.IO
    open Microsoft.WindowsAzure
    open Microsoft.WindowsAzure.Storage
    open Microsoft.WindowsAzure.Storage.Table

    module Limits =
        let PayloadSizePerProperty = 64L * 1024L
        let NumberOfProperties = 15L
        let MaxPayloadSize = NumberOfProperties * PayloadSizePerProperty

    module Helpers = 
        let partitionIn n (a : byte []) =
            let n = ((float a.Length) / (float n) |> ceil |> int)
            [| for i in 0 .. n - 1 ->
                let i, j = a.Length * i / n, a.Length * (i + 1) / n
                Array.sub a i (j - i) |]

        let isFatEntity serializeTo =
            async {
                if Limits.MaxPayloadSize <= 0L then return false
                else
                    let b = Array.zeroCreate (int Limits.MaxPayloadSize)
                    use fix = new MemoryStream(b) :> Stream
                    try 
                        do! serializeTo fix
                        return true
                    with _ -> return false
                
            }

        let toBinary serialize =
            async {
                use ms = new MemoryStream()
                do! serialize (ms :> Stream)
                return ms.ToArray()  
            }

        let ofBinary (bin : byte []) = new MemoryStream(bin) :> Stream

//        let containerPrefix = "mbrace"
//        let prefix = sprintf "%s%s" containerPrefix
//
//        let removePrefix (str : string) = str.Substring(containerPrefix.Length)

    module Validation =
        let private letters = set {'a'..'z'} + set {'A'..'z'}
        let private numbers = set {'0'..'9'}
        let private partition_key_black_list = set [ '/'; '\\'; '#'; '?' ]

        let checkFolder (name : string) =
            if  not (String.IsNullOrEmpty name) &&
                String.forall (fun c -> letters.Contains c || numbers.Contains c ) name &&
                not (numbers.Contains name.[0] )
            then ()
            else raise <| Exception(sprintf "Invalid folder name %s. Folder name must : contain only alphanumeric characters, start with a letter, have length >= 3." name)

        let checkFile (name : string) =
            if  not (String.IsNullOrEmpty name) &&
                not (String.exists partition_key_black_list.Contains name)
            then ()
            else raise <| Exception(sprintf "Invalid file name %s. File name must : not contain characters in %A. Consider encoding the file name." name partition_key_black_list)

    module Clients =
        let getTableClient conn =
            let account = CloudStorageAccount.Parse(conn) 
            let client = account.CreateCloudTableClient()
            client.GetTableServiceContext().IgnoreResourceNotFoundException <- true
            client

        let getBlobClient conn =
            let account = CloudStorageAccount.Parse(conn) 
            let client = account.CreateCloudBlobClient()

            do client.ParallelOperationThreadCount <- System.Nullable(4 * System.Environment.ProcessorCount)
            do client.SingleBlobUploadThresholdInBytes <- System.Nullable(1L <<< 23) // 8MB, possible ranges: 1..64MB, default 32MB

            client

    /// A lightweight object for low latency communication with the azure storage.
    /// Lightweight : payload size up to 15 * 64K = 960K.
    /// See 'http://www.windowsazure.com/en-us/develop/net/how-to-guides/table-services/'
    type FatEntity (entityId, binary) =
        inherit TableEntity(entityId, String.Empty)

        let check (a : byte [] []) i = if a = null then null elif i >= a.Length then Array.empty else a.[i]
        let binaries = 
            if binary <> null 
            then Helpers.partitionIn Limits.PayloadSizePerProperty binary
            else null

        /// Max size 64KB
        member val Part00 = check binaries 0  with get, set
        member val Part01 = check binaries 1  with get, set
        member val Part02 = check binaries 2  with get, set
        member val Part03 = check binaries 3  with get, set
        member val Part04 = check binaries 4  with get, set
        member val Part05 = check binaries 5  with get, set
        member val Part06 = check binaries 6  with get, set
        member val Part07 = check binaries 7  with get, set
        member val Part08 = check binaries 8  with get, set
        member val Part09 = check binaries 9  with get, set
        member val Part10 = check binaries 10 with get, set
        member val Part11 = check binaries 11 with get, set
        member val Part12 = check binaries 12 with get, set
        member val Part13 = check binaries 13 with get, set
        member val Part14 = check binaries 14 with get, set

        member this.GetPayload () = 
            [|this.Part00 ; this.Part01; this.Part02; this.Part03
              this.Part04 ; this.Part05; this.Part06; this.Part07
              this.Part08 ; this.Part09; this.Part10; this.Part11
              this.Part12 ; this.Part13; this.Part14 |]
            |> Array.map (fun a -> if a = null then Array.empty else a)
            |> Array.concat
        
        new () = FatEntity (null, null)

    type MutableFatEntity(entityId, isRef : bool, reference : string, bin) =
        inherit FatEntity(entityId, bin)
        
        interface ITableEntity

        member val IsReference = isRef with get, set
        member val Reference = reference with get, set

        new () = MutableFatEntity(null, false, null, null)

//    type OnDesposeStream (stream : Stream, onDispose : unit -> unit) =
//        inherit Stream () with
//            override __.CanSeek = stream.CanSeek            
//            override __.CanRead = stream.CanRead
//            override __.CanWrite = stream.CanWrite
//            override __.Write(b,o,c) = stream.Write(b,o,c)
//            override __.Read(b,o,c) = stream.Read(b,o,c)
//            override __.SetLength(l) = stream.SetLength(l)
//            override __.Length = stream.Length
//            override __.Position with get () = stream.Position
//                                 and  set p  = stream.Position <- p
//            override __.Flush () = stream.Flush()
//            override __.Seek(a,b) = stream.Seek(a,b)
//            override __.Dispose(disposing) = 
//                onDispose ()
//                stream.Dispose(disposing)
//
//            
////        interface IDisposable with
////            member  __.Dispose () = onDispose (); stream.Dispose()

    [<AutoOpen>]
    module AsyncEx =
        type Async with
            static member FromBeginEndCancellable(beginAction, endAction) =
                Async.FromBeginEnd((fun (c,s) -> beginAction(c,s) :> System.IAsyncResult), endAction)
            
            static member FromBeginEndCancellable(beginAction, endAction, o) =
                Async.FromBeginEnd((fun (c,s) -> beginAction(o, c, s) :> System.IAsyncResult), endAction)

            static member And(left, right) = async {
                let! left = left
                if left then return! right
                else return false
            }

