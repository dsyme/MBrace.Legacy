open System
open System.Collections
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns

type TestRecord = {
    Id: int
    Name: string
}

let testQuotation = <@ fun tr -> tr.Id = 5 @>

type ITable = 
    abstract RecordType: Type
    abstract Seq: IEnumerable
    abstract Find: IComparable -> obj
    abstract TryFind: IComparable -> obj option
    abstract Exists: IComparable -> bool
    abstract Attributes: Attribute list
    abstract GetIndex: string -> Map<IComparable, IComparable>


and KeyF<'T> = 'T -> IComparable
and Index<'T> = { 
    IndexName: string
    AttributeName: string
    Extract: 'T -> IComparable
    IndexMap: Map<IComparable, IComparable> 
}

and Table<'T> = { 
    PrimaryKey: Attribute<'T, IComparable>
    Attributes: Attribute list
    DataMap: Map<IComparable, 'T>
    Indexes: Map<string, Index<'T>>
} with 
    interface ITable with
        member t.RecordType = typeof<'T>
        member t.Seq = t.DataMap |> Map.toSeq |> Seq.map snd :> IEnumerable
        member t.Find k = t.DataMap.[k] :> obj
        member t.TryFind k = t.DataMap.TryFind k |> Option.map (fun r -> r :> obj)
        member t.Exists k = t.DataMap.ContainsKey k
        member t.Attributes = t.Attributes
        member t.GetIndex name = t.Indexes.[name].IndexMap

and Attribute(name: string, recordType: Type, columnType: Type, recordExtract: obj -> obj) = 
    member c.Name = name
    member c.RecordType = recordType
    member c.ColumnType = columnType
    member c.RecordExtract = recordExtract
    
    member a.CompareTo(other: obj) =
        match other with
        | :? Attribute as otherAttr -> 
            let nameComp = a.Name.CompareTo(otherAttr.Name)
            if a.RecordType <> otherAttr.RecordType && nameComp = 0 then -1
            else nameComp
        | _ -> invalidOp "Uncomparable"

    interface IComparable with
        member a.CompareTo(other: obj) = a.CompareTo other
        

and Attribute<'T>(name: string, columnType: Type, recordExtract: obj -> obj) = 
    inherit Attribute(name, typeof<'T>, columnType, recordExtract)

and Attribute<'T, 'U>(name: string, extractF: 'T -> 'U) =
    inherit Attribute<'T>(name, typeof<'U>, fun o -> extractF(o :?> 'T) :> obj)
    member c.Extract = extractF
    static member (==) (left: Attribute<'T, 'U>, right: Attribute<'Z, 'U>) = BinaryExpr(Eq, Atom(Attr(left :> Attribute)), Atom(Attr(right :> Attribute)))
    static member (==) (left: Attribute<'T, 'U>, right: 'U) = BinaryExpr(Eq, Atom(Attr(left :> Attribute)), Atom(Literal(right :> obj)))

and SigmaBinaryOp = And | Or | Eq | Lt | Gt
and SigmaUnaryOp = Not

and SigmaAtom = Top //true
                 | Bottom //false
                 | Attr of Attribute
                 | Literal of obj

and SigmaExpr = Atom of SigmaAtom
                 //| Predicate of (obj -> bool)
                 | BinaryExpr of SigmaBinaryOp * SigmaExpr * SigmaExpr
                 | UnaryExpr of SigmaUnaryOp * SigmaExpr

and RelationalExpr =
    | Relation of ITable
    | Sigma of SigmaExpr * RelationalExpr
    | Product of RelationalExpr * RelationalExpr
    | InnerJoin of SigmaExpr * RelationalExpr * RelationalExpr
    //| NaturalJoin of RelationalExpr * RelationalExpr
    //| EquiJoin of RelationalExpr * Column * RelationalExpr * Column
    //| Rename of string * string * RelationalExpr

and Query<'T> = {
    RelationalQuery: RelationalExpr
    Compiler: RelationalExpr -> seq<'T>
}

module Table =

    let create (primaryKey: Attribute<'T, 'U> when 'U :> IComparable) = { 
        PrimaryKey = Attribute<'T, IComparable>(primaryKey.Name, fun t -> (primaryKey.Extract t) :> IComparable)
        Attributes = []
        DataMap = Map.empty
        Indexes = Map.empty 
    }

    let addAttribute (attribute: Attribute<'T, 'U>) (table: Table<'T>): Table<'T> =
        { table with Attributes = (attribute :> Attribute)::table.Attributes }

type Department = {
    Id: int
    Name: string
} with
    static member IdAttribute = Attribute<_, _>("id", fun d -> d.Id)
    static member NameAttribute = Attribute<_, _>("name", fun d -> d.Name)

    static member create () = Table.create Department.IdAttribute
                              |> Table.addAttribute Department.NameAttribute

let d = Department.create()

module Query =

    type AccessPath =
        | RelationScan of SigmaExpr * ITable
        | IndexEq of string * IComparable * ITable
        | SequenceScan of SigmaExpr * AccessPath
        | PathJoin of SigmaExpr * AccessPath * AccessPath

    let rec private (|AtomicProposition|_|) expr =
        match expr with
        | BinaryExpr(And, _, _)
        | BinaryExpr(Or, _, _) -> None
        | UnaryExpr(Not, AtomicProposition _)
        | _ -> Some expr

    let rec private relationalExprAttributes = 
        function Relation table -> table.Attributes |> Set.ofList
                 | Sigma(_, rel) -> relationalExprAttributes rel
                 | InnerJoin(_, left, right)
                 | Product(left, right) -> relationalExprAttributes right |> Set.union (relationalExprAttributes left)

    let rec private sigmaAttributes =
        function Atom(Attr attribute) -> Set.singleton attribute
                 | BinaryExpr(_, left, right) -> sigmaAttributes right |> Set.union (sigmaAttributes left)
                 | UnaryExpr(_, expr) -> sigmaAttributes expr
                 | _ -> Set.empty

    let private (|SigmaAttributes|) = sigmaAttributes

    let private cnf =
        let rec deMorganSimplify =
            function UnaryExpr(Not, Atom Top) -> Atom Bottom
                     | UnaryExpr(Not, Atom Bottom) -> Atom Top
                     | UnaryExpr(Not, UnaryExpr(Not, expr)) -> deMorganSimplify <| expr
                     | UnaryExpr(Not, BinaryExpr(And, left, right)) -> BinaryExpr(Or, deMorganSimplify <| UnaryExpr(Not, left), deMorganSimplify <| UnaryExpr(Not, right))
                     | UnaryExpr(Not, BinaryExpr(Or, left, right)) -> BinaryExpr(And, deMorganSimplify <| UnaryExpr(Not, left), deMorganSimplify <| UnaryExpr(Not, right))
                     | BinaryExpr(And | Or as lop, left, right) -> BinaryExpr(lop, deMorganSimplify <| left, deMorganSimplify <| right)
                     | BinaryExpr _ as bexpr -> bexpr
                     | _ -> failwith "Compiler error: Expression not boolean"

        let rec distributeDisjunction left right =
            let rec distributeDisjunction' left right =
                match right with
                | BinaryExpr(_, AtomicProposition left', AtomicProposition right') -> BinaryExpr(And, BinaryExpr(Or, left, left'), BinaryExpr(Or, left, right'))
                | BinaryExpr(_, AtomicProposition left', right') -> BinaryExpr(And, BinaryExpr(Or, left, left'), distributeDisjunction' left right')
                | BinaryExpr(_, left', AtomicProposition right') -> BinaryExpr(And, distributeDisjunction' left left', BinaryExpr(Or, left, left'))
                | BinaryExpr(_, left', right') -> BinaryExpr(And, distributeDisjunction' left left', distributeDisjunction' left right')
                | AtomicProposition _ -> BinaryExpr(Or, left, right)
                | _ -> failwith "Compiler error: Expression not boolean"

            match left with
            | BinaryExpr(_, AtomicProposition left', AtomicProposition right') -> BinaryExpr(And, distributeDisjunction' left' right, distributeDisjunction right' right)
            | BinaryExpr(_, AtomicProposition left', right') -> BinaryExpr(And, distributeDisjunction' left' right, distributeDisjunction right' right)
            | BinaryExpr(_, left', AtomicProposition right') -> BinaryExpr(And, distributeDisjunction left' right, distributeDisjunction' right' right)
            | BinaryExpr(_, left', right') -> BinaryExpr(And, distributeDisjunction left' right, distributeDisjunction right' right)
            | AtomicProposition _ -> distributeDisjunction' left right
            | _ -> failwith "Compiler error: Expression not boolean"

        let rec normalize =
            function BinaryExpr(And, left, right) -> BinaryExpr(And, normalize left, normalize right)
                     | BinaryExpr(Or, left, right) -> distributeDisjunction (normalize left) (normalize right)
                     | AtomicProposition p -> p
                     | _ -> failwith "CompilerError: Expression not boolean"

        normalize << deMorganSimplify

    let rec private topBottomNormalize expr =
        let normalize = function UnaryExpr(Not, Atom Top) -> Atom Bottom
                                 | UnaryExpr(Not, Atom Bottom) -> Atom Top
                                 | UnaryExpr _ as e -> e
                                 | BinaryExpr(And, Atom Bottom, _)
                                 | BinaryExpr(And, _, Atom Bottom) -> Atom Bottom
                                 | BinaryExpr(And, Atom Top, right) -> right
                                 | BinaryExpr(And, left, Atom Top) -> left
                                 | BinaryExpr(Or, Atom Top, _)
                                 | BinaryExpr(Or, _, Atom Top) -> Atom Top
                                 | BinaryExpr(Or, Atom Bottom, right) -> right
                                 | BinaryExpr(Or, left, Atom Bottom) -> left
                                 | BinaryExpr _ as e -> e
                                 | _ as e -> e
        match expr with
        | Atom _ as a -> a
        | UnaryExpr(Not, Atom Top) -> Atom Bottom
        | UnaryExpr(Not, Atom Bottom) -> Atom Top
        | UnaryExpr(Not, expr) -> normalize <| UnaryExpr(Not, topBottomNormalize expr)
        | BinaryExpr(And, Atom Bottom, _)
        | BinaryExpr(And, _, Atom Bottom) -> Atom Bottom
        | BinaryExpr(And, Atom Top, right) -> topBottomNormalize right
        | BinaryExpr(And, left, Atom Top) -> topBottomNormalize left
        | BinaryExpr(And, left, right) -> normalize <| BinaryExpr(And, topBottomNormalize left, topBottomNormalize right)
        | BinaryExpr(Or, Atom Top, _)
        | BinaryExpr(Or, _, Atom Top) -> Atom Top
        | BinaryExpr(Or, Atom Bottom, right) -> topBottomNormalize right
        | BinaryExpr(Or, left, Atom Bottom) -> topBottomNormalize left
        | BinaryExpr(Or, left, right) -> normalize <| BinaryExpr(Or, topBottomNormalize left, topBottomNormalize right)
        | BinaryExpr _ as e -> e

    let rec relationsInAccessPath ap = seq {
        match ap with
        | RelationScan(_, rel)
        | IndexEq(_, _, rel) -> yield rel
        | SequenceScan(_, ap) -> yield! relationsInAccessPath ap
        | PathJoin(_, (RelationScan(_, rel) | IndexEq(_, _, rel)), other)
        | PathJoin(_, other, (RelationScan(_, rel) | IndexEq(_, _, rel))) ->
            yield rel
            yield! relationsInAccessPath other
        | PathJoin(_, left, right) ->
            yield! relationsInAccessPath left
            yield! relationsInAccessPath right
    }

    let rec private translate relExpr =
        let rec sigmaSplitForJoin sigma joinAttributes =
            match sigma with
            | AtomicProposition _ ->
                let attributes = sigmaAttributes sigma
                if attributes |> Set.isSubset joinAttributes then sigma, Atom Top else Atom Top, sigma
            | BinaryExpr(And, left, right) ->
                let leftJoinCondition, leftScanCondition = sigmaSplitForJoin left joinAttributes
                let rightJoinCondition, rightScanCondition = sigmaSplitForJoin right joinAttributes
                BinaryExpr(And, leftJoinCondition, rightJoinCondition), BinaryExpr(And, leftScanCondition, rightScanCondition)
            | BinaryExpr(Or, left, right) ->
                if (sigmaAttributes left) |> Set.union (sigmaAttributes right) |> Set.isSubset joinAttributes then
                    sigma, Atom Top
                else Atom Top, sigma
            | _ -> failwith "Compiler internal error: Invalid cnf"

        let rec sigmaScanSplit rightAttributes sigma =
            match sigma with
            | AtomicProposition p ->
                let attributes = sigmaAttributes sigma
                if attributes |> Set.isSubset rightAttributes then Atom Top, p else p, Atom Top
            | BinaryExpr(And, left, right) ->
                let leftLeftScanCondition, leftRightScanCondition = sigmaScanSplit rightAttributes left
                let rightLeftScanCondition, rightRightScanCondition = sigmaScanSplit rightAttributes right
                BinaryExpr(And, leftLeftScanCondition, rightLeftScanCondition), BinaryExpr(And, leftRightScanCondition, rightRightScanCondition)
            | BinaryExpr(Or, left, right) ->
                if (sigmaAttributes left) |> Set.isSubset rightAttributes || (sigmaAttributes right) |> Set.isSubset rightAttributes then
                    Atom Top, sigma
                else sigma, Atom Top
            | _ -> failwith "Compiler internal error: Invalid cnf"
                
        let rec sigmaTranslate sigma expr =
            match expr with
            | RelationScan(pred, rel) -> RelationScan(BinaryExpr(And, sigma, pred), rel)
            | IndexEq _ as idxscan -> idxscan
            | SequenceScan(_, ap) -> sigmaTranslate sigma ap
            | PathJoin(joinCondition, left, right) ->
                let leftTopLevelRelation = relationsInAccessPath left |> Seq.head
                let rightTopLevelRelation = relationsInAccessPath right |> Seq.head
                let joinAttributes = (leftTopLevelRelation.Attributes |> Set.ofList) |> Set.union (rightTopLevelRelation.Attributes |> Set.ofList)
                let joinCondition', scanCondition = sigmaSplitForJoin sigma joinAttributes
                let leftScanCondition, rightScanCodition = sigmaScanSplit (relationsInAccessPath left |> Seq.collect (fun rel -> rel.Attributes |> List.toSeq) |> Set.ofSeq) scanCondition
                PathJoin(BinaryExpr(And, joinCondition, joinCondition'), sigmaTranslate leftScanCondition left, sigmaTranslate rightScanCodition right)


        match relExpr with
        | Relation table -> RelationScan(Atom Top, table)
        | Product(left, right) -> PathJoin(Atom Top, translate left, translate right)
        | InnerJoin(joinCondition, left, right) -> PathJoin(joinCondition, translate left, translate right)
        | Sigma(sigma, expr) -> sigmaTranslate (cnf sigma) (translate expr)

    type CompilationUnit = {
        Context: Map<string, obj -> obj>
        Output: Expr
    }

    let compile relExpr =
        let compileSigma s: Expr = invalidOp "Not Implemented"
        
        let compileSigma sigma typeVector context =
            match typeVector with
            | [t] ->
                match sigma with
                | BinaryExpr(Eq, Atom(Attr attrLeft), Atom(Attr attrRight)) ->
                    let parameter = Var("record", t, false)
                    let argument = Expr.Var parameter
                    let leftAttributeExpr = Expr.Value(attrLeft, (typedefof<Attribute<_, _>>).MakeGenericType([| attrLeft.RecordType; attrLeft.ColumnType |]))
                    let rightAttributeExpr = Expr.Value(attrLeft, (typedefof<Attribute<_, _>>).MakeGenericType([| attrRight.RecordType; attrRight.ColumnType |]))
                    Expr.Lambda(parameter, <@@ ((%%leftAttributeExpr).Extract %%argument) = ((%%rightAttributeExpr).Extract %%argument) @@>)

        let getEmptySeqExpr seqType =
            match <@ Seq.empty @> with
            | Call(_, mInfo, _) -> Expr.Call(mInfo.GetGenericMethodDefinition().MakeGenericMethod([| seqType |]), []) //Seq.empty<'T> & typeof<'T> = seqType
            | _ -> failwith "Compiler error."

        let rec compileAccessPath ap = 
            match ap with
            | RelationScan(Atom Top, table) ->
                let tableExpr = Expr.Value(table :> obj, (typedefof<Table<_>>).MakeGenericType([| table.RecordType |]))
                { 
                    Context = table.Attributes |> List.map (fun attr -> (attr.Name + attr.RecordType.Name), attr.RecordExtract) |> Map.ofList
                    Output = <@@ (%%tableExpr).DataMap |> Map.toSeq |> Seq.map snd @@> //Expr<seq<'T>> & Table<'T>
                }
            | RelationScan(Atom Bottom, table) ->
                { 
                    Context = table.Attributes |> List.map (fun attr -> (attr.Name + attr.RecordType.Name), attr.RecordExtract) |> Map.ofList
                    Output = getEmptySeqExpr table.RecordType //Seq.empty<'T> & Table<'T>
                }
            | RelationScan(sigma, table) ->
                let tableExpr = Expr.Value(table :> obj, (typedefof<Table<_>>).MakeGenericType([| table.RecordType |]))
                let filter = compileSigma sigma
                {
                    Context = table.Attributes |> List.map (fun attr -> (attr.Name + attr.RecordType.Name), attr.RecordExtract) |> Map.ofList
                    Output = <@@ (%%tableExpr).DataMap |> Map.toSeq |> Seq.map snd |> Seq.filter (%%filter) @@> //Expr<seq<'T>> & Table<'T>
                }
            | IndexEq(idxName, value, table) ->
                let tableExpr = Expr.Value(table :> obj, (typedefof<Table<_>>).MakeGenericType([| table.RecordType |]))
                {
                    Context = table.Attributes |> List.map (fun attr -> (attr.Name + attr.RecordType.Name), attr.RecordExtract) |> Map.ofList
                    Output = 
                        <@@ 
                            match (%%tableExpr).Indexes.[idxName].IndexMap.TryFind value with
                            | Some k -> (%%tableExpr).DataMap.[k] |> Seq.singleton
                            | None -> Seq.empty
                        @@>  //Expr<seq<'T>> & Table<'T>
                }
            | SequenceScan(Atom Top, seqPath) ->
                compileAccessPath seqPath //Expr<Seq<'T>> & [[seqPath]] = Expr<Seq<'T>>
            | SequenceScan(Atom Bottom, seqPath) ->
                let comp = compileAccessPath seqPath
                let seqType = comp.Output.Type.GetGenericArguments().[0]
                { 
                    Context = comp.Context
                    Output = getEmptySeqExpr seqType //Expr<Seq<'T>> & [[seqPath]] = Expr<Seq<'T>>
                }
            | SequenceScan(sigma, seqPath) ->
                let comp = compileAccessPath seqPath
                let sequenceExpr = comp.Output
                let filter = compileSigma sigma
                {
                    Context = comp.Context
                    Output = <@@ %%sequenceExpr |> Seq.filter %%filter @@> //Expr<Seq<'T>> & [[seqPath]] = Expr<Seq<'T>>
                }
            | PathJoin(sigma, leftPath, rightPath) ->
                let leftComp = compileAccessPath leftPath
                let leftSeqExpr = leftComp.Output
                let rightComp = compileAccessPath rightPath
                let rightSeqExpr = rightComp.Output 
                let filter = compileSigma sigma
                {
                    Context = [leftComp.Context |> Map.map (fun _ extract -> fun o -> extract o) |> Map.toSeq
                               rightComp.Context |> Map.map (fun _ extract -> fun o -> extract o) |> Map.toSeq] 
                              |> Seq.concat
                              |> Map.ofSeq
                    Output =
                        <@@
                            %%leftSeqExpr |> Seq.collect (fun o -> %%rightSeqExpr |> Seq.map (fun o' -> o, o') |> Seq.filter %%filter)
                        @@> //Expr<Seq<'T * 'U> & [[leftPath]] = Expr<Seq<'T> & [[rightPath]] = Expr<Seq<'U>>
                }

        translate relExpr |> compileAccessPath
        

    let from (table: Table<'T>): Query<'T> = { 
        RelationalQuery = Relation table
        Compiler = fun _ -> Seq.empty
    }

    let crossJoin (table: Table<'U>) (query: Query<'T>): Query<'T * 'U> = {
        RelationalQuery = Product(query.RelationalQuery, Relation table)
        Compiler = fun _ -> Seq.empty
    }

//    let naturalJoin (table: Table<'U>) (query: Query<'T>): Query<'T * 'U> = {
//        RelationalQuery = NaturalJoin(query.RelationalQuery, Relation table)
//        Compiler = fun _ -> Seq.empty
//    }

    let on (joinCondition: SigmaExpr) = joinCondition

    let innerJoin (table: Table<'U>) (joinCondition: SigmaExpr) (query: Query<'T>): Query<'T * 'U> = {
        RelationalQuery = Sigma(joinCondition, Product(query.RelationalQuery, Relation table))
        Compiler = fun _ -> Seq.empty
    }

    let where (filter: SigmaExpr) (query: Query<'T>): Query<'T> = {
        RelationalQuery = Sigma(filter, query.RelationalQuery)
        Compiler = fun _ -> Seq.empty
    }

    let toSeq (query: Query<'T>): seq<'T> = query.Compiler query.RelationalQuery
        

//let q =
//    Query.from table
//    |> Query.equiJoin table'.ColumnName
//    |> Query.equiJoin table''.ColumnName
//    |> Query.where (table.ColumnName Op value)
//    |> Query.toSeq
