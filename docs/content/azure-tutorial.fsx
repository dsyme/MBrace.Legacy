(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../bin"

#r "Thespian.dll"
#r "Vagrant.dll"
#r "MBrace.Core.dll"
#r "MBrace.Utils.dll"
#r "MBrace.Lib.dll"
#r "MBrace.Store.dll"
#r "MBrace.Runtime.Base.dll"
#r "MBrace.Client.dll"

open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Client

MBraceSettings.MBracedExecutablePath <- System.IO.Path.Combine(__SOURCE_DIRECTORY__, "../../bin/mbraced.exe")

(**

# Using MBrace on Windows Azure IaaS [Draft]

In this tutorial you will learn how to setup MBrace on Windows Azure IaaS. 
As a prerequisite you need to have a Windows Azure account, basic knowledge of the Azure computing/virtual machine services and
optionally Microsoft Visual Studio (or any environment supporting F#).

Typically you need some virtual machines to run the MBrace daemons/nodes and a client
that manages the runtime (usually the client runs on a F# interactive instance). Any nodes in a MBrace runtime
as well as any clients, should be part of the same network in order to work properly. 

Because of this restriction in this tutorial you have two choices: you can either use one of the virtual machines as a client or you can use
the Windows Azure VPN service to join the virtual network created in Azure and access the runtime from a remote client (your on-premises computer).

## Creating a virtual network
At this point you should decide if you are going to use a remote client or one of
the virtual machines as a client. If you choose the first 
option you need to create a virtual network, create and upload certificates and finally configure your VPN client. 
The process is described in [here](http://msdn.microsoft.com/en-us/library/azure/dn133792.aspx).

You can skip this step if you want to use a client in one of your virtual machines, as a virtual network will be automatically created
for you during the virtual machine creation.

## Creating a virtual machine
Now you need to create your virtual machines. You can follow 
[this](https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-windows-tutorial/) tutorial
to create a virtual machine (you can skip the 'How to attach a data disk to the new virtual machine' section).
If you have created a virtual network then you __must__ specify 
the virtual network when you create the virtual machine. 
You can't join the virtual machine to a virtual network after you have created the VM.

Note that although there are no minimal requirements for the VM size it's recommented to use at least 
a Medium (A2) (2 cores, 3.5GB Memory) instance. After the virtual machine is created log on to it.

## Installing the MBrace Runtime

In order to get a MBrace node running on a machine you need to download 
the [MBrace.Runtime](http://nuget.org/packages/MBrace.Runtime) package.
Any dependencies and executables are located in the `tools` subdirectory. Alternatively you can 
use the [Install-MBrace.ps1](http://github.com/nessos/MBrace/raw/master/nuget/installer/Install-MBrace.ps1) powershell script that helps you install 
MBrace by downloading `.NET 4.5` if it's needed, downloading the
runtime nuget package, adding firewall exceptions for the MBrace executables and finally 
installing the MBrace runtime as a Windows Service.

Open a PowerShell prompt as administrator, download and run `Install-MBrace.ps1` script.
Before running the installation script you probably need to enable script execution in PowerShell;
you can do this using the [Set-ExecutionPolicy](http://technet.microsoft.com/en-us/library/ee176961.aspx) cmdlet.

    [lang=batchfile]
    c:\mbrace > help .\Install-MBrace.ps1
    
    NAME
        C:\mbrace\Install-MBrace.ps1
    
    SYNOPSIS
        Installation script for the MBrace runtime.
    
    SYNTAX
        C:\mbrace\Install-MBrace.ps1 [-AddToPath] [-Service] [[-Directory] <String>] [<CommonParameters>]


    DESCRIPTION
        This script implements the following workflow:
    	    * Install .NET 4.5 if it's needed.
    	    * Download the NuGet standalone and the latest MBrace.Runtime package.
    	    * Add firewall exceptions for the mbraced and mbrace.worker executables.
    	    * Install and starts the MBrace Windows service.
    	Note that administrator rights are required.


    RELATED LINKS
        http://github.com/Nessos/MBrace
    	http://nessos.github.io/MBrace
    	http://www.m-brace.net/

            
        c:\mbrace > Set-ExecutionPolicy Unrestricted
        c:\mbrace > .\Install-MBrace.ps1
    
    * Checking for admin permissions...
    * Checking if .NET 4.5 installed...
    * Downloading NuGet...
    * Downloading MBrace.Runtime...
    * Adding firewall rules...
    * Installing MBrace service...


At this point you should have the `MBrace Runtime` service running. You can confirm this by using the `Get-Service` cmdlet or the Services tool.

    [lang=batchfile]
    c:\mbrace > Get-Service MBrace
            
    Status   Name               DisplayName
    ------   ----               -----------
    Running  MBrace             MBrace Runtime


## Configuring MBrace
Now you need to make any configurations to the MBrace daemon by changing the `mbraced.exe.config` file.
You can configure settings like the working directory, ports used by the runtime, etc. 

Normally you don't need to change this file and you can skip this step, unless you are using a remote client and a VPN.
In this case you need to change the `hostname` setting from its default value to the internal IP of the virtual machine 
or the subnet of the previously virtual network in CIDR format.

    <add key="hostname" value="10.0.1.0/27" />

Leaving the _hostname_ setting to it's default value means that the runtime will use the machine's
hostname as a node identifier. In the case of VPN your computer (the client) might not be able to
resolve this hostname to an IP (unless you use a DNS server). Using the VM's IP or a subnet instead of its hostname
makes sure that the client will be able to communicate with this node.

Finally you need to restart the service in order to load the new configuration.
    
    [lang=batchfile]
    c:\mbrace > Restart-Service MBrace


## Creating your cluster

After setting up the first virtual machine you need to replicate it in order to create a cluster of nodes.
You can either create the new virtual machines just like the first one and then install the runtime in each of them,
or you can use the first machine as a template and create your cluster.
For the second option you need to run `sysprep` on the virtual machine and then create a custom
virtual machine. This process is described [here](https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-capture-image-windows-server/)
for the sysprep part and [here](https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-create-custom/) for the custom virtual machine creation.
You just need to make sure that during the creation you choose the same _Cloud Service_ and _Virtual Network_ for all of your machines.

## Booting the MBrace Runtime

At this point your virtual machines are running and the MBrace nodes are idle.
At your client machine, if you use VPN make sure that you are connected and you can ping some of the remote machines.

In any case you need to install the F# interactive in your client (or optionally Visual Studio or any environment supporting F#)
as well as the [MBrace.Runtime](http://www.nuget.org/packages/MBrace.Runtime) package.

In your solution open the `mbrace-tutorial.fsx`. In the _initialize a runtime of remote nodes_ section
change nodes hostname and ports.
In case you are using a remote client (without DNS resolution) use the internal IPs of the virtual machines:
*)

let nodes =
    [
        MBraceNode.Connect("10.0.0.4", 2675)
        MBraceNode.Connect("10.0.0.5", 2675)
        MBraceNode.Connect("10.0.0.6", 2675)
    ]

(**
Or use the hostnames
*)
let nodes = [1..3] |> List.map (fun i -> MBraceNode.Connect(sprintf "clusterVM%d" i, 2675))

(**
Now ping the nodes to ensure connectivity
*)

nodes |> List.iter (fun n -> printfn "Node %A : %s" n <| try n.Ping().ToString() with _ -> "failed")

(**
At this point, before actually booting a runtime you need to configure which store
will be used by the runtime. By default each node will use its local filesystem, 
but in our case we are going the use Windows Azure Storage as our backend. You need
to download the [MBrace.Azure](http://www.nuget.org/packages/MBrace.Azure) package.

Now create a new `StoreDefinition` and set it as default for your client. 
You can find your storage account name and key using the Azure Management Portal, in the [Storage](https://azure.microsoft.com/en-us/documentation/articles/storage-manage-storage-account/#regeneratestoragekeys) page.

*)

#r "../bin/MBrace.Azure.dll"

open Nessos.MBrace.Azure

let name = "yourAccountName"
let key = "yourAccountKey"

let azureStore = AzureStore.Create(accountName = name, accountKey = key)

MBraceSettings.DefaultStore <- azureStore

(**
Finally boot the runtime (the MBrace.Azure package will be uploaded to the runtime)
and send your first cloud computation.
*)

let runtime = MBrace.Boot nodes

runtime.Run <@ cloud { return 42 } @>

(**

## Useful references
* [Configure a Point-to-Site VPN in the Management Portal](http://msdn.microsoft.com/en-us/library/azure/dn133792.aspx)
* [Create a Virtual Machine Running Windows Server](https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-windows-tutorial/)
* [How to Capture a Windows Virtual Machine to Use as a Template](https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-capture-image-windows-server/)
* [How to Create a Custom Virtual Machine](https://azure.microsoft.com/en-us/documentation/articles/virtual-machines-create-custom/)
* [How to install and configure Azure PowerShell](http://azure.microsoft.com/en-us/documentation/articles/install-configure-powershell/)
*)
