<#  
.SYNOPSIS  
	Installation script for the MBrace runtime.
.DESCRIPTION  
	This script implements the following workflow:
		* Install .NET 4.5 if it's needed.
		* Download the NuGet standalone and the latest MBrace.Runtime package.
		* Add firewall exceptions for the mbraced and mbrace.worker executables.
		* Install and starts the MBrace Windows service.
	Note that administrator rights are required.
.PARAMETER AddToPath
	Add the MBrace.Runtime/tools directory to PATH environment variable. This parameter defaults to false.
.PARAMETER Service
	Install MBrace Windows service. This parameter defaults to true.
.PARAMETER Directory
	Installation directory. This parameter defaults to the Program Files directory.
.NOTES  
	File Name  : Install-MBrace.ps1  
	Requires   : PowerShell 3.0
.LINK  
	http://github.com/Nessos/MBrace
	http://nessos.github.io/MBrace
	http://www.m-brace.net/
#>

param([switch]$AddToPath = $false, [switch]$Service = $true, [string]$Directory = "$($env:ProgramFiles)\MBrace")

function SetUp-Directory
{
	if(!(Test-Path $Directory)) {
		write-host "Creating directory $Directory"
		$_ = mkdir $Directory
	}
}

function Has-Net45
{
	if (Test-Path 'HKLM:\SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full')
	{
		if (Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full' -Name Release -ErrorAction SilentlyContinue) { return $True }
	}
	return $False
}

function Download-Nuget
{
	$url = 'http://nuget.org/nuget.exe'
	$target = "$Directory\nuget.exe"
	write-host "Downloading file $url to $target"
	(New-Object net.webclient).DownloadFile($url, $target)
}

function CheckIf-Admin
{
	return ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
}

function Install-Net45
{
	$url = "https://github.com/nessos/MBrace/raw/master/nuget/installer/dotNetFx45_Full_setup.exe"
	$target = "$Directory\dotNetFx45_Full_setup.exe"
	$(New-Object net.webclient).DownloadFile($url, $target)
	write-host "Running $target"
	Start-Process -FilePath $target -ArgumentList /q, /norestart 
}

function Download-MBrace
{
	#Start-Process -FilePath ./nuget.exe -ArgumentList "install", "MBrace.Runtime", "-Prerelease" -Wait -NoNewWindow
	& "$Directory\nuget.exe" install MBrace.Runtime -Prerelease -ExcludeVersion -OutputDirectory $Directory
}

function Add-FirewallRules
{
	$path = "$Directory\MBrace.Runtime\tools" 
	write-host "Deleting existing rules"
	netsh.exe advfirewall firewall delete rule name = 'MBrace Daemon' | out-null
	netsh.exe advfirewall firewall delete rule name = 'MBrace Worker' | out-null
	write-host "Adding rules"
	netsh.exe advfirewall firewall add rule name = 'MBrace Daemon' dir=in action=allow program="$path\mbraced.exe" enable=yes       | out-null
	netsh.exe advfirewall firewall add rule name = 'MBrace Worker' dir=in action=allow program="$path\mbrace.worker.exe" enable=yes | out-null
}

function Install-MBraceService
{
	$path = "$Directory\MBrace.Runtime\tools\mbracesvc.exe"
	write-host "Stopping any existing MBrace services"
	Stop-Service -Name 'MBrace' -Force -ErrorAction SilentlyContinue
	write-host "Deleting any existing MBrace services"
	sc.exe delete 'MBrace' | out-null
	write-host "Creating new service"
	$svc = New-Service  -Name 'MBrace' -DisplayName 'MBrace Runtime' -BinaryPathName $path -StartupType Automatic -Description "MBrace Runtime Service. Initializes a MBrace daemon with the given arguments and the mbraced configuration file."
	write-host "Starting MBrace"
	Start-Service -Name 'MBrace'
}

function Execute-Step([string]$message, [scriptblock]$block)
{
	try { 
		Write-Host "* $message . . . " 
		return $block.Invoke()
	}
	catch {
		Write-Host "failed"
		exit
	}
}

function Add-ToPath 
{
	$oldPath=(Get-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name PATH).Path
	$toolsDir = "$Directory\MBrace.Runtime\tools\"
	if(($ENV:PATH | Select-String -SimpleMatch $toolsDir) -eq $null) {
		$newPath=$oldPath+";$toolsDir"
		Set-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name PATH -Value $newPath
	} else {
		write-host "Found in PATH"
	}
}

$isAdmin = Execute-Step "Checking for admin permissions" { CheckIf-Admin }
if(!$isAdmin) { Write-Host "Admin permissions required"; exit }
$net45 = Execute-Step "Checking if .NET 4.5 installed" { Has-Net45 }
if(!$net45) { Execute-Step "Installing .NET 4.5" { Install-Net45 } }
Execute-Step "Checking installation directory" { SetUp-Directory }
Execute-Step "Downloading NuGet" { Download-Nuget }
Execute-Step "Downloading MBrace.Runtime" { Download-MBrace }
Execute-Step "Adding firewall rules" { Add-FirewallRules }
if($Service) { Execute-Step "Installing MBrace service" { Install-MBraceService } }
if($AddToPath) { Execute-Step "Adding to PATH" { Add-ToPath } }

write-host "Done..."