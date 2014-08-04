<#  
.SYNOPSIS  
    Installation script for the MBrace runtime.
.DESCRIPTION  
    This script implements the following workflow:
        * Installs .NET 4.5 if it's needed.
        * Downloads the NuGet standalone and the latest MBrace.Runtime package.
        * Adds firewall exceptions for the mbraced and mbrace.worker executables.
        * Installs and starts the MBrace Windows service.
    Note that administrator rights are required.
.PARAMETER AddToPath
    Add the MBrace.Runtime/tools directory to PATH environment variable. This parameter defaults to false.
.PARAMETER Service
    Install MBrace Windows service. This parameter defaults to true.
.NOTES  
    File Name  : Install-MBrace.ps1  
    Requires   : PowerShell 4.0.0
.LINK  
    http://github.com/Nessos/MBrace
    http://nessos.github.io/MBrace
#>

param([switch]$AddToPath = $false, [switch]$Service = $true)

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
    Invoke-WebRequest "http://nuget.org/nuget.exe" -OutFile nuget.exe 
}

function CheckIf-Admin
{
    return ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
}

function Install-Net45
{
     Start-Process -FilePath .\dotNetFx45_Full_setup.exe -ArgumentList /q, /norestart 
}

function Download-MBrace
{
    #Start-Process -FilePath ./nuget.exe -ArgumentList "install", "MBrace.Runtime", "-Prerelease" -Wait -NoNewWindow
    .\nuget.exe install MBrace.Runtime -Prerelease -ExcludeVersion
}

function Add-FirewallRules
{
    $path = "$PSScriptRoot\MBrace.Runtime\tools" 
    netsh.exe advfirewall firewall delete name = 'MBrace Daemon' | Out-Null
    netsh.exe advfirewall firewall delete name = 'MBrace Worker' | Out-Null
    netsh.exe advfirewall firewall add rule name = 'MBrace Daemon' dir=in action=allow program="$path\mbraced.exe" enable=yes        | out-null
    netsh.exe advfirewall firewall add rule name = 'MBrace Worker' dir=in action=allow program="$path\mbrace.worker.exe" enable=yes  | out-null
}

function Install-MBraceService
{
    $path = "$PSScriptRoot\MBrace.Runtime\tools\mbracesvc.exe"
    Stop-Service -Name 'MBrace' -Force -ErrorAction SilentlyContinue
    sc.exe delete 'MBrace' | Out-Null
    New-Service  -Name 'MBrace' -DisplayName 'MBrace Runtime' -BinaryPathName $path -StartupType Automatic | Out-Null
    Start-Service -Name 'MBrace'
}

function Execute-Step([string]$message, [scriptblock]$block)
{
    Write-Host "* $message . . . " 
    return $block.Invoke()
}

function Add-ToPath 
{
    $oldPath=(Get-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name PATH).Path
    $toolsDir = "$PSScriptRoot\MBrace.Runtime\tools\"
    if(($ENV:PATH | Select-String -SimpleMatch $toolsDir) -eq $null) {
        $newPath=$oldPath+";$toolsDir"
        Set-ItemProperty -Path 'Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment' -Name PATH –Value $newPath
    }
}

Start-Transcript -Path "mbraceinstaller$(Get-Date -Format "ddMMyyyyHHmmss").txt"

$isAdmin = Execute-Step "Checking for admin permissions" { CheckIf-Admin }
if(!$isAdmin) { Write-Host "Admin permissions required"; exit }
$net45 = Execute-Step "Checking if .NET 4.5 installed" { Has-Net45 }
if(!$net45) { Execute-Step "Installing .NET 4.5" { Install-Net45 } }
Execute-Step "Downloading NuGet" { Download-Nuget }
Execute-Step "Downloading MBrace.Runtime" { Download-MBrace }
Execute-Step "Adding firewall rules" { Add-FirewallRules }
if($Service) { Execute-Step "Installing MBrace service" { Install-MBraceService } }
if($AddToPath) { Execute-Step "Adding to PATH" { Add-ToPath } }

Stop-Transcript 
