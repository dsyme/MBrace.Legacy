namespace Nessos.MBrace.Lib.Concurrency

    open Nessos.MBrace

    /// Implementation of the Haskell MVar, build on top of the MutableCloudRefs.
    type MVar<'T> = IMutableCloudRef<'T option>

    /// Implementation of the Haskell MVar, build on top of the MutableCloudRefs.

    [<Cloud>]
    module MVar =
        /// Creates a new empty MVar.
        let newEmpty<'T> : Cloud<MVar<'T>> = MutableCloudRef.New(None)

        /// <summary>
        ///     Create a new MVar containing the given value.
        /// </summary>
        /// <param name="value">initial value.</param>
        let newValue<'T> value : Cloud<MVar<'T>> = MutableCloudRef.New(Some value)

        /// <summary>
        ///     Puts a value in the MVar. This function will block until the MVar is empty
        ///     and the put succeeds.
        /// </summary>
        /// <param name="mvar">MVar to be accessed.</param>
        /// <param name="value">value to be substituted.</param>
        let rec put (mvar : MVar<'T>) value = 
            cloud {
                let! v = MutableCloudRef.Read(mvar)
                match v with
                | None -> 
                    let! ok = MutableCloudRef.Set(mvar, Some value)
                    if not ok then return! put mvar value
                | Some _ ->
                    return! put mvar value
            }

        /// <summary>
        ///     Dereferences an MVar. 
        ///     This function will block until the MVar is non-empty.
        /// </summary>
        /// <param name="mvar">MVar to be accessed.</param>
        let rec take (mvar : MVar<'T>) =
            cloud {
                let! v = MutableCloudRef.Read(mvar)
                match v with
                | None -> 
                    return! take mvar
                | Some v -> 
                    let! ok = MutableCloudRef.Set(mvar, None)
                    if not ok then return! take mvar
                    else return v
            }

    type private Stream<'T> = MVar<Item<'T>>
    and private Item<'T> = Item of 'T * Stream<'T>

    /// An implementation of a Channel using the MVar abstraction.
    type Channel<'T> = private Channel of (MVar<Stream<'T>> * MVar<Stream<'T>>)

    [<Cloud>]
    /// Provides basic operations on the Channel type.
    module Channel =

        /// Creates a new empty Channel.
        let newEmpty<'T> : Cloud<Channel<'T>> = 
            cloud {
                let! hole = MVar.newEmpty
                let! readVar = MVar.newValue hole
                let! writeVar = MVar.newValue hole
                return Channel(readVar, writeVar)
            }

        /// <summary>
        ///     Writes a value to a Channel.
        /// </summary>
        /// <param name="chan">channel.</param>
        /// <param name="value">value written to be writted to channel.</param>
        let write<'T> (chan : Channel<'T>) (value : 'T) : Cloud<unit> =
            cloud {
                let (Channel(_, writeVar)) = chan
                let! newHole = MVar.newEmpty
                let! oldHole = MVar.take writeVar
                do! MVar.put writeVar newHole
                do! MVar.put oldHole (Item(value, newHole))
            }

        /// <summary>
        ///     Reads a value from a Channel.  
        /// </summary>
        /// <param name="chan">input channel.</param>
        let read<'T> (chan : Channel<'T>) : Cloud<'T> = 
            cloud {
                let (Channel(readVar,_)) = chan
                let! stream = MVar.take readVar
                let! (Item(value, newV)) = MVar.take stream
                do! MVar.put readVar newV
                return value
            }
