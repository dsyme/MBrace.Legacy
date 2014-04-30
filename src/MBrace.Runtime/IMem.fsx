#r "FSharp.PowerPack"
#r "bin/debug/Unquote.dll"
#r "bin/debug/Nessos.MBrace.ImemDb.dll"
#r "bin/debug/Nessos.MBrace.Utils.dll"
#r "bin/debug/Nessos.Thespian.dll"
#r "bin/debug/Nessos.Thespian.Remote.dll"
#r "bin/debug/Nessos.Thespian.ClientPack.dll"
#r "bin/debug/Nessos.Thespian.PowerPack.dll"
#r "bin/debug/Nessos.MBrace.Serialization.dll"
#r "bin/debug/Nessos.MBrace.Base.dll"
#r "bin/debug/Nessos.MBrace.Core.dll"
#r "bin/debug/Nessos.MBrace.Runtime.Actors.dll"
#r "bin/debug/Nessos.MBrace.Runtime.dll"


open Nessos.Thespian
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Data
open Nessos.MBrace.ImemDb
open Microsoft.FSharp.Quotations

let environmentDb = EnvironmentDb.Create()

let activator = Actor.sink() |> Actor.ref : ActorRef<Activator>
let runtime = Actor.sink() |> Actor.ref : ActorRef<Runtime>

let nodeData = { 
    AssemblyManager = Actor.sink() |> Actor.ref
    ProcessDomainManager = Actor.sink() |> Actor.ref
}

let extractTable database tableExpr =
    database |> Swensen.Unquote.Operators.eval tableExpr

let updatedTableList (db: 'Db) (table: Table<'Record>) =
    [ for tableProperty in typeof<'Db>.GetProperties() ->
        let tableType = tableProperty.PropertyType
        if tableType = typeof<Table<'Record>> then Expr.Value table
        else Expr.PropertyGet(Expr.Value db, tableProperty)
    ]

let updateDb (database: 'D) (table: Table<'T>) =
    Expr.NewRecord(typeof<'D>, updatedTableList database table)
    |> Expr.Cast 
    |> Swensen.Unquote.Operators.eval

let entry = { 
    Node.Activator = activator 
    Node.Runtime = runtime
    Node.NodeData = nodeData
    Node.NodeType = MasterNode 
}

let tableProperty = typeof<EnvironmentDb>.GetProperties().[0]
let tableType = tableProperty.PropertyType

let foo = updatedTableList environmentDb (environmentDb.Node |> Table.insert entry)

let env' = environmentDb |> Database.insert <@ fun db -> db.Node @> entry

