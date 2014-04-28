module Nessos.MBrace.Utils.Buffer

    open System
    open System.Threading
    open System.Collections
    open System.Collections.Generic

    type CircBuff<'T> (seq : IEnumerable<'T>, size : int) =
        let size = size
        interface IEnumerable<'T> with
            member this.GetEnumerator() = new CircEnum<'T>(seq, size) :> IEnumerator<'T>
            member this.GetEnumerator() = new CircEnum<'T>(seq, size) :> IEnumerator
    and CircEnum<'T> (seq, size) as this =
        let mutable endOfStream = false
        let mutable finalConsume = false
        let mutable endPoint = size
        let mutable enum : IEnumerator<'T> = seq.GetEnumerator()
        let mutable consumerBuff = 0
        let mutable consumerCount = -1
        let mutable thread : Thread = new Thread((fun _ -> this.ProduceAll(1)) : unit -> unit)
        let syncRoot = new Object()
        let buff : 'T array array = [| Array.zeroCreate size; Array.zeroCreate size |]

        let producerEvent = new AutoResetEvent(true)
        let consumerEvent = new AutoResetEvent(false)
    
        do this.Produce(0)
        do thread.Start()

        member private this.Consume() =
            consumerCount <- consumerCount + 1
            if finalConsume && consumerCount > endPoint then
                false
            else
                if consumerCount < size then
                    true
                else
                    let x = consumerBuff
                    consumerEvent.WaitOne() |> ignore
                    lock syncRoot (fun () ->
                        producerEvent.Set() |> ignore
                        consumerBuff <- (consumerBuff + 1) % 2
                        consumerCount <- 0
                        if endOfStream then
                            if endPoint = 0 then
                                false
                            else
                                finalConsume <- true
                                true
                        else
                            true
                    )
        
        member private this.Produce(producerBuff : int) =
            let mutable i = 0
            while i < size && not endOfStream do
                if enum.MoveNext() then
                    buff.[producerBuff].[i] <- enum.Current
                else
                    Monitor.Enter(syncRoot)
                    endOfStream <- true
                    endPoint <- i
                    Monitor.Exit(syncRoot)
                i <- i + 1
        
        member private this.ProduceAll(producerBuff : int) =
            producerEvent.WaitOne() |> ignore
            this.Produce(producerBuff)
            consumerEvent.Set() |> ignore
            if not endOfStream then
                this.ProduceAll((producerBuff + 1) % 2)


        member this.Print() =
            for i in buff.[0] do
                printf "%A " i
            printfn ""
            for i in buff.[1] do
                printf "%A " i
            printfn ""
            printfn "%A %A" consumerBuff consumerCount

        member this.Initialize() =
            endOfStream <- false
            finalConsume <- false
            endPoint <- size
            enum <- seq.GetEnumerator()
            consumerBuff <- 0
            consumerCount <- -1
            producerEvent.Set() |> ignore
            consumerEvent.Reset() |> ignore
            this.Produce(0)
            thread <- new Thread((fun _ -> this.ProduceAll(1)) : unit -> unit)
            thread.Start()

        interface IEnumerator<'T> with
            member this.Current = buff.[consumerBuff].[consumerCount]
            member this.Current = buff.[consumerBuff].[consumerCount] :> obj
            member this.MoveNext() = this.Consume()
            member this.Reset() =
                if (thread <> null) then
                    endOfStream <- true
                    producerEvent.Set() |> ignore
                    thread.Join()
                this.Initialize()

            member this.Dispose() =
                endOfStream <- true
                producerEvent.Set() |> ignore
                thread.Join()

    let buffer (n : int) (seq : 'T seq) = new CircBuff<'T>(seq, n) :> 'T seq