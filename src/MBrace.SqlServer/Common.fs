namespace Nessos.MBrace.SqlServer.Common

    open System
    open System.Data.SqlClient
    open System.Collections.Generic
    
    type SqlTransaction (conn : SqlConnection) =
        member this.execQuery (command : string) (parameters : SqlParameter[]) : Async<SqlDataReader> =
            async {
                use command = new SqlCommand(command, conn)
                command.Parameters.AddRange(parameters) |> ignore
                return! command.ExecuteReaderAsync()
                        |> Async.AwaitTask           
            }

        member this.execNonQuery (command : string) (parameters : SqlParameter[]) : Async<int> =
            async {
                use command = new SqlCommand(command, conn)
                command.Parameters.AddRange(parameters) |> ignore
                return! command.ExecuteNonQueryAsync()
                        |> Async.AwaitTask
            }

    type SqlHelper (connectionString : string) =
        member this.execTransaction (continuation : SqlTransaction -> Async<'a>) = 
            async {
                use conn = new SqlConnection(connectionString)
                do! conn.OpenAsync().ContinueWith(ignore)
                    |> Async.AwaitTask
            
                let transaction = conn.BeginTransaction()
                let! ret = continuation <| SqlTransaction(conn)
                transaction.Commit()
                return ret                
            }

        member this.execQuery (command : string) (parameters : SqlParameter[]) (continuation : SqlDataReader -> Async<'a>) : Async<'a> =
            async {
                use conn = new SqlConnection(connectionString)
                do! conn.OpenAsync().ContinueWith(ignore)
                    |> Async.AwaitTask
                use command = new SqlCommand(command, conn)
                command.Parameters.AddRange(parameters) |> ignore
                let! reader = command.ExecuteReaderAsync()
                              |> Async.AwaitTask

                return! continuation reader                
            }

        member this.exec command parameters =
            this.execQuery command parameters (fun _ -> async.Return ())

        member this.collect sql (parameters : SqlParameter[]) =
            async {                
                let ret = new List<string>()
                do! this.execQuery sql parameters (fun r -> async {
                        let ok = ref true
                        let! read = r.ReadAsync() |> Async.AwaitTask
                        ok := read
                        while !ok do
                            ret.Add(r.GetString(0))
                            let! read = r.ReadAsync() |> Async.AwaitTask
                            ok := read
                    })
                return ret.ToArray()
            }

        member this.test command parameters =
            this.execQuery command parameters (fun r -> async { return! r.ReadAsync() |> Async.AwaitTask })

        member this.getStream command parameters =
            this.execQuery command parameters (fun r -> async {
                do! r.ReadAsync() |> Async.AwaitTask |> Async.Ignore
                return r.GetStream(0)
            })

        member this.execWithNewTag command parameters =
            async {
                let tag = Guid.NewGuid().ToString("N")
                let paramsTag = Array.append parameters [|SqlParameter("@tag", tag)|]
                do! this.execQuery command paramsTag (fun _ -> async.Return ())
                return tag                
            }

        member this.getStreamTag command parameters =
            this.execQuery command parameters (fun r -> async {
                do! r.ReadAsync() |> Async.AwaitTask |> Async.Ignore
                return (r.GetStream(0), r.GetString(1))
            })