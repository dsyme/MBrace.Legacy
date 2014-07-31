
/// A stream wrapper that fails when the size of the stream gt maxSize.
/// 'Write'-only, no data actually written.
module internal FixedStream 
    open System
    open System.IO

    exception FixedSizeExceededException of unit

    type FixedSizeStream (maxSize : int64) =
        inherit Stream() with
            let mutable length = 0L
            member private __.LengthInternal
                with get () = length
                and  set l = 
                    if l > maxSize then raise(FixedSizeExceededException())
                    else length <- l
            let mutable position = 0L

            override this.CanRead  = false
            override this.CanSeek  = false
            override this.CanWrite = true
            override this.Length = this.LengthInternal
            override this.Position 
                with get () = position
                and  set v = position <- v
            override this.Flush () = ()
            override this.Seek(_,_) = raise(NotSupportedException())
            override this.SetLength l = this.LengthInternal <- l
            override this.Read(_,_,_) = raise(NotSupportedException())
            override this.Write(bytes, offset, count) =
                let len = bytes.Length
                if offset >= len || offset + count > len || offset < 0 then
                    raise(ArgumentException("Offset and length were out of bounds for the array or count is greater than the number of elements from index to the end of the source collection."))
                else 
                    position <- position + int64 count
                    if position > this.LengthInternal then
                        this.LengthInternal <- position

    let isSmallObj threshold serializeTo =
        if threshold <= 0L then false
        else
            let b = Array.zeroCreate (int threshold)
            use fix = new MemoryStream(b) //new FixedSizeStream(threshold) :> Stream
            try serializeTo fix ; true
            with _ -> false