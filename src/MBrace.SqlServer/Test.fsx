#r "Nessos.MBrace.Utils"
#r "Nessos.MBrace.Actors"
#r "Nessos.MBrace.Base"
#r "Nessos.MBrace.Store"
#r "Nessos.MBrace.Client"

#r "System.Data"
#r "System.Data.Linq"

#r "bin/Debug/Nessos.MBrace.Store.SqlServer.dll"

open System
open System.Data
open Nessos.MBrace.Store
open Nessos.MBrace.Client
open Nessos.MBrace.Utils
open Nessos.MBrace.Store.SqlServer

let sqlstore = new SqlServerStore("Server=CHURCH;Database=MBrace;User Id=sa;Password=12345678;") :> IStore


sqlstore.Create("theFolder", "theFile", (fun x -> x.WriteByte(88uy)))

sqlstore.Read("theFolder", "theFile").ReadByte()

sqlstore.GetFolders()
sqlstore.GetFiles("theFolder")

typeof<SqlServerStoreFactory>.FullName
typeof<SqlServerStoreFactory>.Assembly.FullName
IoC.Resolve<IStore>()

let rt = MBrace.InitLocal 3

let x = newRef 84 |> MBrace.RunLocal

x.Value