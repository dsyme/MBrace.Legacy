namespace Nessos.MBrace.SqlServer

    open System
    open System.IO
    open Nessos.MBrace.Runtime.Store
    open System.Data
    open System.Data.SqlClient
    open System.Collections.Generic
    open Nessos.MBrace.SqlServer.Common
    
    type SqlServerStore(connectionString: string, ?name : string) as this =
        let name = defaultArg name "SqlServer"

        let is = this :> IStore

        let helper = new SqlHelper(connectionString)

        do helper.exec """
            if object_id('dbo.Blobs', 'U') is null
                CREATE TABLE [dbo].[Blobs] (
                    [Folder] VARCHAR (50)    NOT NULL,
                    [File]   VARCHAR (50)    NOT NULL,
                    [Tag]    VARCHAR (50)    NULL,
                    [Value]  VARBINARY (MAX) NOT NULL,
                    CONSTRAINT [PK_Path] PRIMARY KEY CLUSTERED ([Folder] ASC, [File] ASC)
                );
        """ [||]
        |> Async.RunSynchronously

        interface IStore with
            override this.Name = name
            override this.UUID = connectionString

            override this.Create (folder, file, serialize, ?asFile) = async {
                    let ms = new MemoryStream()
                    do! serialize ms
                    let bytes = ms.ToArray()
                    return! 
                        helper.exec "insert into Blobs (Folder, [File], Value) values (@folder, @file, @value)"
                           [| new SqlParameter("@folder", folder); new SqlParameter("@file", file); new SqlParameter("@value", bytes) |]
                }

            override this.Read(folder : string, file : string) : Async<Stream> =
                async {
                    return! helper.getStream "select Value from Blobs where Folder = @folder and [File] = @file"
                                [| new SqlParameter("@folder", folder); new SqlParameter("@file", file) |]
                }
                
            override this.CopyTo(folder, file, target) = async {             
                    use! source = is.Read(folder,file)
                    do! source.CopyToAsync(target).ContinueWith(ignore)
                        |> Async.AwaitTask
                }

            override this.CopyFrom(folder, file, stream, ?asFile) =
                is.Create(folder, file, (fun target -> async { return! stream.CopyToAsync(stream).ContinueWith(ignore) |> Async.AwaitTask }), ?asFile = asFile)
                
            override this.GetFiles folder =
                async {
                    return! helper.collect "select [File] from Blobs where Folder = @folder"
                                [| new SqlParameter("@folder", folder) |]
                }
                
            override this.GetFolders () =
                async {
                    return! helper.collect "select distinct Folder from Blobs" [||]
                }
                        
            override this.Exists folder =
                async {
                    return! helper.test "select Folder from Blobs where Folder = @folder"
                                [| new SqlParameter("@folder", folder) |]
                }

            override this.Exists (folder, file) =
                async {
                    return! helper.test "select Folder from Blobs where Folder = @folder and [File] = @file"
                                [| new SqlParameter("@folder", folder); new SqlParameter("@file", file) |]
                }

            override this.Delete folder =
                async {
                    return! helper.exec "delete from Blobs where Folder = @folder"
                                [| new SqlParameter("@folder", folder) |]                    
                }

            override this.Delete (folder, file) =
                async {
                    return! helper.exec "delete from Blobs where Folder = @folder and [File] = @file"
                                [| new SqlParameter("@folder", folder); new SqlParameter("@file", file) |]                    
                }


            override this.CreateMutable(folder, file, serialize) = async {
                    let ms = new MemoryStream()
                    do! serialize ms
                    let bytes = ms.ToArray()

                    return!
                        helper.execWithNewTag "insert into Blobs (Folder, [File], Value, Tag) values (@folder, @file, @value, @tag)"
                            [| new SqlParameter("@folder", folder); new SqlParameter("@file", file); new SqlParameter("@value", bytes) |]
                }

            override this.ReadMutable(folder, file) = async {
                return! helper.getStreamTag "select Value, Tag from Blobs where Folder = @folder and [File] = @file"
                            [| new SqlParameter("@folder", folder); new SqlParameter("@file", file) |]
                }

            override this.UpdateMutable(folder, file, serialize, tag) = async {
                return!
                    helper.execTransaction (fun trans -> async {
                        let ms = new MemoryStream()
                        do! serialize ms
                        let bytes = ms.ToArray()

                        let! i = trans.execNonQuery """
                            update Blobs set Tag = @tag and Value = @value
                            where File = @file and Folder = @folder and Tag = @tag")
                        """      
                                    [| new SqlParameter("@folder", folder); new SqlParameter("@file", file);
                                    new SqlParameter("@tag", tag); new SqlParameter("@value", bytes) |]
                        if i > 0 then
                            return (true, tag)
                        else
                            let! r = trans.execQuery "select Tag from Blobs where File = @file and Folder = @folder"
                                        [| new SqlParameter("@folder", folder); new SqlParameter("@file", file) |]
                            do! r.ReadAsync() |> Async.AwaitTask |> Async.Ignore
                            return (false, r.GetString(0))
                    })
                }
                
            override this.ForceUpdateMutable(folder, file, serialize) = async {
                    let ms = new MemoryStream()
                    do! serialize ms
                    let bytes = ms.ToArray()

                    return! helper.execWithNewTag "update Blobs set Value = @value, Tag = @tag where Folder = @folder and [File] = @file"
                                [| new SqlParameter("@folder", folder); new SqlParameter("@file", file); new SqlParameter("@value", bytes) |]
                }

    type SqlServerStoreFactory () =
        interface ICloudStoreFactory with
            member this.CreateStoreFromConnectionString (connectionString : string) = 
                SqlServerStore(connectionString) :> IStore

    [<AutoOpen>]
    module StoreProvider =
        type Nessos.MBrace.Client.StoreProvider with
            static member SqlServerStore (connectionString : string) =
                Nessos.MBrace.Client.StoreProvider.Define<SqlServerStoreFactory>(connectionString)