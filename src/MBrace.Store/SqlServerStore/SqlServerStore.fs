namespace Nessos.MBrace.Store

    open System
    open System.IO
    open System.Data
    open System.Data.SqlClient
    open System.Collections.Generic
    
    /// An ICloudStore implementation that uses a SQLServer instance as a backend.
    type SqlServerStore(connectionString: string, ?name : string) as this =
        let name = defaultArg name "SqlServer"

        let is = this :> ICloudStore

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

        interface ICloudStore with
            override this.Name = name
            override this.EndpointId = connectionString

            override this.CreateImmutable (folder, file, serialize, _) = async {
                    let ms = new MemoryStream()
                    do! serialize ms
                    let bytes = ms.ToArray()
                    return! 
                        helper.exec "insert into Blobs (Folder, [File], Value) values (@folder, @file, @value)"
                           [| new SqlParameter("@folder", folder); new SqlParameter("@file", file); new SqlParameter("@value", bytes) |]
                }

            override this.ReadImmutable(folder : string, file : string) : Async<Stream> =
                async {
                    return! helper.getStream "select Value from Blobs where Folder = @folder and [File] = @file"
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
                    return! helper.collect "select [File] from Blobs where Folder = @folder"
                                [| new SqlParameter("@folder", folder) |]
                }
                
            override this.GetAllContainers () =
                async {
                    return! helper.collect "select distinct Folder from Blobs" [||]
                }
                        
            override this.ContainerExists folder =
                async {
                    return! helper.test "select Folder from Blobs where Folder = @folder"
                                [| new SqlParameter("@folder", folder) |]
                }

            override this.Exists (folder, file) =
                async {
                    return! helper.test "select Folder from Blobs where Folder = @folder and [File] = @file"
                                [| new SqlParameter("@folder", folder); new SqlParameter("@file", file) |]
                }

            override this.DeleteContainer folder =
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

            override this.TryUpdateMutable(folder, file, serialize, tag) = async {
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

    /// <summary>
    ///     SqlServer Store factory implementation.
    /// </summary>
    type SqlServerStoreFactory () =
        interface ICloudStoreFactory with
            member this.CreateStoreFromConnectionString (connectionString : string) = 
                SqlServerStore(connectionString) :> ICloudStore