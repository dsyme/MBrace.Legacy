namespace Nessos.MBrace.Azure.Common

    open System
    open System.IO
    open Microsoft.WindowsAzure
    open Microsoft.WindowsAzure.Storage
    open Microsoft.WindowsAzure.Storage.Table

    module internal Limits =
        let PayloadSizePerProperty = 64L * 1024L
        let NumberOfProperties = 15L
        let MaxPayloadSize = NumberOfProperties * PayloadSizePerProperty

    module internal Helpers = 
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

    module internal Validation =
        let private lowercase = set {'a'..'z'}
        let private letters = lowercase + set {'A'..'z'}
        let private numbers = set {'0'..'9'}
        let private partition_key_black_list = set [ '/'; '\\'; '#'; '?' ]

        let checkFolder (name : string) =
            if  not (String.IsNullOrEmpty name) &&
                String.forall (fun c -> lowercase.Contains c || numbers.Contains c ) name &&
                not (numbers.Contains name.[0] )
            then ()
            else raise <| Exception(sprintf "Invalid folder name %s. Folder name must : contain only alphanumeric characters, start with a letter, have length >= 3." name)

        let checkFile (name : string) =
            if  not (String.IsNullOrEmpty name) &&
                not (String.exists partition_key_black_list.Contains name)
            then ()
            else raise <| Exception(sprintf "Invalid file name %s. File name must : not contain characters in %A. Consider encoding the file name." name partition_key_black_list)

    module internal Clients =
        let getTableClient (account : CloudStorageAccount) =
            let client = account.CreateCloudTableClient()
            client

        let getBlobClient (account : CloudStorageAccount) =
            let client = account.CreateCloudBlobClient()
            
            client.DefaultRequestOptions.ParallelOperationThreadCount <- System.Nullable(4 * System.Environment.ProcessorCount)
            client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes <- System.Nullable(1L <<< 23) // 8MB, possible ranges: 1..64MB, default 32MB

            client

    /// A lightweight object for low latency communication with the azure storage.
    /// Lightweight : payload size up to 15 * 64K = 960K.
    /// See 'http://www.windowsazure.com/en-us/develop/net/how-to-guides/table-services/'
    type internal FatEntity (entityId, binary) =
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

    type internal MutableFatEntity(entityId, isRef : bool, reference : string, bin) =
        inherit FatEntity(entityId, bin)
        
        interface ITableEntity

        member val IsReference = isRef with get, set
        member val Reference = reference with get, set

        new () = MutableFatEntity(null, false, null, null)
