//#r "Nessos.MBrace.Utils"
//#r "Nessos.MBrace.Actors"
//#r "Nessos.MBrace.Base"
//#r "Nessos.MBrace.Store"
//#r "Nessos.MBrace.Client"

//#r "System.Data"
//#r "System.Data.Linq"


#I "bin/Debug/"
#r "MBrace.SqlServer.dll"
#r "MBrace.Utils.dll"
#r "MBrace.Runtime.Base.dll"

open System
open System.Data.SqlClient
open Nessos.MBrace.SqlServer
open Nessos.MBrace.Runtime.Store

type Async<'T> with
    member this.Run () = Async.RunSynchronously this
    

let conn = @"Data Source=(localdb)\ProjectsV12;Initial Catalog=master;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False"

let sqlstore = new SqlServerStore(conn) :> ICloudStore

sqlstore.CreateImmutable("theFolder", "theFile", (fun x -> async { x.WriteByte(88uy) }), asFile = false).Run()

sqlstore.ReadImmutable("theFolder", "theFile").Run().ReadByte()

sqlstore.GetAllContainers().Run()
sqlstore.GetAllFiles("theFolder").Run()


#r "System.Transactions"

let sqlc = new SqlConnection(conn)
sqlc.Open()

sqlc.Dispose()


typeof<SqlServerStoreFactory>.FullName
typeof<SqlServerStoreFactory>.Assembly.FullName
//IoC.Resolve<IStore>()
//
//let rt = MBrace.InitLocal 3
//
//let x = newRef 84 |> MBrace.RunLocal
//
//x.Value