#I @"../Installer/bin/Debug"
#r "Nessos.MBrace.Utils.dll"
#r "Nessos.MBrace.Actors.dll"
#r "Nessos.MBrace.Base.dll"
#r "Nessos.MBrace.Store.dll"
#r "Nessos.MBrace.Client.dll"
#r "Nessos.MBrace.Core.dll"
#r "Nessos.MBrace.Serialization.dll"

open Nessos.MBrace.Client

// end of init

// connection string 
//<add key="store provider" value="Nessos.MBrace.Store.Azure.AzureStoreFactory, Nessos.MBrace.Store.Azure, Version=0.4.2.29545, Culture=neutral, PublicKeyToken=e3142f36c45d3d3d"/>
//<add key="store endpoint" value="DefaultEndpointsProtocol=http;AccountName=portalvhdstccdhxmdkg097;AccountKey=6DoWCOd9ekaPkDa4Hh4ThHm4wYuDWEQ5/WSHBnq8aUk0mj4Ipf/gpRzfO+N2aU0RTFntGZ4ke7avuq7FftQeuA=="/>

////////////////// 30 nodes //////////////////


// Boot time
Real: 00:01:06.761, CPU: 00:00:00.359, GC gen0: 2, gen1: 1, gen2: 0 | Cold


// rt.Run <@ cloud { return "Hello" } @>
Real: 00:00:21.848, CPU: 00:00:00.437, GC gen0: 5, gen1: 1, gen2: 0 | Cold
Real: 00:00:21.848, CPU: 00:00:00.437, GC gen0: 5, gen1: 1, gen2: 0
Real: 00:00:05.974, CPU: 00:00:00.093, GC gen0: 3, gen1: 0, gen2: 0
Real: 00:00:21.616, CPU: 00:00:00.437, GC gen0: 14, gen1: 1, gen2: 0
Real: 00:00:23.412, CPU: 00:00:00.703, GC gen0: 16, gen1: 2, gen2: 0
Real: 00:00:06.511, CPU: 00:00:00.140, GC gen0: 2, gen1: 0, gen2: 0
Real: 00:00:06.485, CPU: 00:00:00.078, GC gen0: 4, gen1: 0, gen2: 0
      00:00:03.008 (after bin 10)

//[<Cloud>]
//let rec bin (depth : int) = cloud {
//    if depth = 0 then return ()
//    else let! _ =  bin (depth-1) <||> bin(depth-1) in return ()
//}

bin 3  :  00:01:01.9218166
bin 10 :  00:01:25.3006183 
          00:01:38.2457823


 Name  Process Id  Status     #Workers  #Tasks  Start Time             Execution Time    Result Type 
 ----  ----------  ------     --------  ------  ----------             --------------    ----------- 
             6619  Completed         0       0  7/18/2013 10:43:51 AM  00:00:04.4201978  string      
             8901  Completed         0       0  7/18/2013 10:45:00 AM  00:00:03.1934683  string      
             7832  Completed         0       0  7/18/2013 10:45:13 AM  00:00:18.9569138  string      
             6228  Completed         0       0  7/18/2013 10:45:41 AM  00:00:21.3106756  string      
             4285  Completed         0       0  7/18/2013 10:47:07 AM  00:00:02.4970189  string      
             6125  Completed         0       0  7/18/2013 10:47:37 AM  00:00:04.1120367  string      
 bin          735  Completed         0       0  7/18/2013 10:52:27 AM  00:01:01.9218166  unit        
 bin         7320  Completed         0       0  7/18/2013 10:54:22 AM  00:01:25.3006183  unit        
 bin         1024  Completed         0       0  7/18/2013 10:57:07 AM  00:01:38.2457823  unit        
             7198  Completed         0       0  7/18/2013 10:59:34 AM  00:00:00.5020275  string      
              959  Completed         0       0  7/18/2013 10:59:42 AM  00:00:01.2633403  string      
             3043  Completed         0       0  7/18/2013 10:59:48 AM  00:00:01.0193993  string      
             6215  Completed         0       0  7/18/2013 10:59:56 AM  00:00:00.8837551  string      


// rt.Reboot()
Real: 00:00:37.156, CPU: 00:00:00.125, GC gen0: 3, gen1: 0, gen2: 0
Real: 00:00:12.463, CPU: 00:00:00.078, GC gen0: 3, gen1: 1, gen2: 0
Real: 00:00:13.143, CPU: 00:00:00.109, GC gen0: 2, gen1: 1, gen2: 0

// rt.Shutdown()
Real: 00:00:02.010, CPU: 00:00:00.015, GC gen0: 0, gen1: 0, gen2: 0



////////////////// 50 nodes //////////////////

// boot
Real: 00:01:09.343, CPU: 00:00:00.375, GC gen0: 2, gen1: 1, gen2: 0

// hello
Real: 00:00:17.532, CPU: 00:00:00.515, GC gen0: 4, gen1: 0, gen2: 0
Real: 00:00:06.530, CPU: 00:00:00.093, GC gen0: 2, gen1: 0, gen2: 0
Real: 00:00:06.918, CPU: 00:00:00.062, GC gen0: 3, gen1: 1, gen2: 0
Real: 00:00:06.158, CPU: 00:00:00.062, GC gen0: 1, gen1: 0, gen2: 0


// bin 3
00:00:31.9106128


// bin 10
00:01:35.2892845
00:01:23.2932731


 Name  Process Id  Status     #Workers  #Tasks  Start Time             Execution Time    Result Type 
 ----  ----------  ------     --------  ------  ----------             --------------    ----------- 
             3414  Completed         0       0  7/18/2013 12:15:17 PM  00:00:03.7833810  string      
             5995  Completed         0       0  7/18/2013 12:15:45 PM  00:00:02.4553042  string      
             5844  Completed         0       0  7/18/2013 12:16:14 PM  00:00:02.8779046  string      
             8377  Completed         0       0  7/18/2013 12:16:32 PM  00:00:01.7481548  string      
 bin         2461  Completed         0       0  7/18/2013 12:18:44 PM  00:00:31.9106128  unit        
 bin         5173  Completed         0       0  7/18/2013 12:20:10 PM  00:01:35.2892845  unit        
 bin         9276  Completed         0       0  7/18/2013 12:22:15 PM  00:01:23.2932731  unit        
             7315  Completed         0       0  7/18/2013 12:24:06 PM  00:00:00.4069264  string      
             6146  Completed         0       0  7/18/2013 12:24:24 PM  00:00:00.7537582  string      
             6817  Completed         6       6  7/18/2013 12:24:31 PM  00:00:03.3587696  string      


// reboot
Real: 00:00:53.535, CPU: 00:00:00.265, GC gen0: 4, gen1: 1, gen2: 0
Real: 00:00:23.965, CPU: 00:00:00.296, GC gen0: 4, gen1: 0, gen2: 0
Real: 00:00:18.767, CPU: 00:00:00.203, GC gen0: 4, gen1: 1, gen2: 0


// shutdown
Real: 00:00:02.009, CPU: 00:00:00.031, GC gen0: 0, gen1: 0, gen2: 0


//////////////////////////////////////////////////////////////////////


let nodes = [0..29] |> List.map (fun n -> MBraceNode("virtual" + string n, 2675))

nodes |> List.iter (fun n -> printf "%A " n.Uri; try printfn "%A" <| n.Ping() with _ -> printfn "err")

#time

let rt = MBrace.Boot nodes


rt.Run <@ cloud { return "Hello" } @>

rt.ShowInfo(true)



rt.Reboot()


rt.ShowProcessInfo()


[<Cloud>]
let rec bin (depth : int) = cloud {
    if depth = 0 then return ()
    else let! _ =  bin (depth-1) <||> bin(depth-1) in return ()
}

let ps = rt.CreateProcess <@ bin 10 @>

ps.ShowInfo()

ps.AwaitResult()
ps.ExecutionTime





