namespace Nessos.MBrace.Lib

    open Nessos.MBrace

    /// A classic ML list implemented using CloudRefs.
    type CloudList<'T> = Nil | Cons of 'T * ICloudRef<CloudList<'T>>

    /// A distributed binary tree structure implemented with CloudRef
    type CloudTree<'T> = 
        | Empty 
        | Leaf of 'T 
        | Branch of CloudTreeRef<'T> * CloudTreeRef<'T>

    /// Cloud Tree reference
    and CloudTreeRef<'T> = ICloudRef<CloudTree<'T>>


    [<RequireQualifiedAccess>]
    module CloudTree =
        
        /// <summary>
        ///     Distributed map workflow on given CloudTree reference
        /// </summary>
        /// <param name="f">map function.</param>
        /// <param name="tree">input tree reference.</param>
        [<Cloud>]
        let rec map (f : 'T -> Cloud<'S>) (tree : CloudTreeRef<'T>) : Cloud<CloudTreeRef<'S>> = cloud {
            match tree.Value with
            | Empty -> return! CloudRef.New Empty
            | Leaf t -> let! s = f t in return! CloudRef.New <| Leaf s
            | Branch(l,r) ->
                let! l',r' = map f l <||> map f r
                return! CloudRef.New <| Branch(l',r')
        }

        /// <summary>
        ///     Distributed reduce workflow on given CloudTree reference
        /// </summary>
        /// <param name="id">identity element.</param>
        /// <param name="f">reduce function.</param>
        /// <param name="tree">input tree reference.</param>
        [<Cloud>]
        let rec reduce (id : 'R) (f : 'R -> 'R -> Cloud<'R>) (tree : CloudTreeRef<'R>) : Cloud<'R> = cloud {
            match tree.Value with
            | Empty -> return id
            | Leaf r -> return r
            | Branch(left,right) ->
                let! r1,r2 = reduce id f left <||> reduce id f right
                return! f r1 r2
        }

        /// <summary>
        ///     Builds a cloud tree of given sequence.
        /// </summary>
        /// <param name="ts">input elements.</param>
        [<Cloud>]
        let ofSeq (ts : seq<'T>) : Cloud<CloudTreeRef<'T>> =
            let rec aux (input : 'T []) = cloud {
                match input.Length with
                | 0 -> return! CloudRef.New Empty
                | 1 -> return! CloudRef.New <| Leaf input.[0]
                | _ ->
                    let l,r = Array.split input
                    let! left = aux l
                    let! right = aux r
                    return! CloudRef.New <| Branch(left, right)
            }

            aux <| Seq.toArray ts