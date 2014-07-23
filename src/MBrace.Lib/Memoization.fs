namespace Nessos.MBrace.Lib

    open Nessos.MBrace

    /// Distributed memoization combinators.

    [<Cloud>]
    [<RequireQualifiedAccessAttribute>]
    module Memoization = 
    
        /// <summary>Memoize the given function using the StoreProvider and
        /// MutableCloudRefs as a lookup.</summary>
        /// <param name="cacheName"> The container name to be used by the StoreProvider.</param>
        /// <param name="encode"> The function that maps the function's domain to valid Store filenames.</param>
        /// <param name="f"> The function to memoize.</param>
        /// <returns> The function that uses memoization.</returns>
        let memoize (cacheName : string) (encode : 'a -> string) 
                    (f : 'a -> Cloud<'b>) : ('a -> Cloud<'b>) = 
            fun a ->
                cloud {
                    let! b = MutableCloudRef.TryGet<'b>(cacheName, encode a)
                    match b with
                    | None ->
                        let! v  = f a
                        let! r = Cloud.Catch <| MutableCloudRef.New(cacheName, encode a, v)
                        return v
                    | Some b -> return! MutableCloudRef.Read(b)
                }
