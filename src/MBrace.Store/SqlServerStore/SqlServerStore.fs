namespace Nessos.MBrace.Store

    open System
    open System.IO
    open System.Data
    open System.Data.SqlClient
    open System.Collections.Generic
    open System.Runtime.Serialization

    open Nessos.MBrace.Utils
    
    /// An ICloudStore implementation that uses a SQLServer instance as a backend.
    type SqlServerStore private (connectionString: string, sqlHelper : SqlHelper) as this =

        let is = this :> ICloudStore

        static member Create(connectionString : string) =
            let sqlHelper = new SqlHelper(connectionString)
            do sqlHelper.exec """
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

            new SqlServerStore(connectionString, sqlHelper)

        interface ICloudStore with
            override this.Name = "SqlServerStore"
            override this.Id = connectionString

            override this.GetStoreConfiguration () = 
                new SqlServerStoreConfiguration(connectionString) :> ICloudStoreConfiguration           

            override this.CreateImmutable (folder, file, serialize, _) = async {
                    let ms = new MemoryStream()
                    do! serialize ms
                    let bytes = ms.ToArray()
                    return! 
                        sqlHelper.exec "insert into Blobs (Folder, [File], Value) values (@folder, @file, @value)"
                           [| new SqlParameter("@folder", folder); new SqlParameter("@file", file); new SqlParameter("@value", bytes) |]
                }

            override this.ReadImmutable(folder : string, file : string) : Async<Stream> =
                async {
                    return! sqlHelper.getStream "select Value from Blobs where Folder = @folder and [File] = @file"
                                [| new SqlParameter("@folder", folder); new SqlParameter("@file", file) |]
                }
                
            override this.CopyTo(folder, file, target) = async {             
                    use! source = is.ReadImmutable(folder,file)
                    do! source.CopyToAsync(target).ContinueWith(ignore)
                        |> Async.AwaitTask
                }

            override this.CopyFrom(folder, file, stream, asFile) =
                is.CreateImmutable(folder, file, (fun target -> async { return! stream.CopyToAsync(stream).ContinueWith(ignore) |> Async.AwaitTask }), asFile = asFile)
                
            override this.GetAllFiles folder =
                async {
                    return! sqlHelper.collect "select [File] from Blobs where Folder = @folder"
                                [| new SqlParameter("@folder", folder) |]
                }
                
            override this.GetAllContainers () =
                async {
                    return! sqlHelper.collect "select distinct Folder from Blobs" [||]
                }
                        
            override this.ContainerExists folder =
                async {
                    return! sqlHelper.test "select Folder from Blobs where Folder = @folder"
                                [| new SqlParameter("@folder", folder) |]
                }

            override this.Exists (folder, file) =
                async {
                    return! sqlHelper.test "select Folder from Blobs where Folder = @folder and [File] = @file"
                                [| new SqlParameter("@folder", folder); new SqlParameter("@file", file) |]
                }

            override this.DeleteContainer folder =
                async {
                    return! sqlHelper.exec "delete from Blobs where Folder = @folder"
                                [| new SqlParameter("@folder", folder) |]                    
                }

            override this.Delete (folder, file) =
                async {
                    return! sqlHelper.exec "delete from Blobs where Folder = @folder and [File] = @file"
                                [| new SqlParameter("@folder", folder); new SqlParameter("@file", file) |]                    
                }


            override this.CreateMutable(folder, file, serialize) = async {
                    let ms = new MemoryStream()
                    do! serialize ms
                    let bytes = ms.ToArray()

                    return!
                        sqlHelper.execWithNewTag "insert into Blobs (Folder, [File], Value, Tag) values (@folder, @file, @value, @tag)"
                            [| new SqlParameter("@folder", folder); new SqlParameter("@file", file); new SqlParameter("@value", bytes) |]
                }

            override this.ReadMutable(folder, file) = async {
                return! sqlHelper.getStreamTag "select Value, Tag from Blobs where Folder = @folder and [File] = @file"
                            [| new SqlParameter("@folder", folder); new SqlParameter("@file", file) |]
                }

            override this.TryUpdateMutable(folder, file, serialize, tag) = async {
                return!
                    sqlHelper.execTransaction (fun trans -> async {
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

                    return! sqlHelper.execWithNewTag "update Blobs set Value = @value, Tag = @tag where Folder = @folder and [File] = @file"
                                [| new SqlParameter("@folder", folder); new SqlParameter("@file", file); new SqlParameter("@value", bytes) |]
                }

    and internal SqlServerStoreConfiguration (connectionString : string) =

        private new (si : SerializationInfo, _ : StreamingContext) =
            new SqlServerStoreConfiguration(si.Read "connectionString")

        interface ISerializable with
            member __.GetObjectData(si : SerializationInfo, _ : StreamingContext) =
                si.Write "connectionString" connectionString

        interface ICloudStoreConfiguration with
            member __.Name = "SqlServerStore"
            member __.Id = connectionString
            member __.Init () = SqlServerStore.Create connectionString :> ICloudStore