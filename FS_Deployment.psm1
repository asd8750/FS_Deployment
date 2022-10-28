#
#   Setup the debug output file
#
if ($FSDebug -gt 0) { 
    "Open file" | Out-File -FilePath C:\Temp\PSDebug.txt 
    "Debug: '$($FSDebug)'" | Out-File -Append -FilePath C:\Temp\PSDebug.txt 
    }

if ($FSDebug -gt 0) { $PSScriptRoot | Out-File -Append -FilePath C:\Temp\PSDebug.txt }

#  
#   Import modules that we commonly use in our cmdlets
#
if (-not (Get-Module SqlServer)) {Import-Module SqlServer}

#
#   Load all component script files with the "." (dot source) mechanism.
#   Using this method is better than combining all the source scripts into one giant .psm1 file
#
$FSDeploymentIsLoading = $true
foreach ($file in (Get-ChildItem -Path "$psScriptRoot\functions\private" -Recurse -Filter *.ps1)) {
    if ($FSDebug -gt 0)  { "Loading (private): " + $file.FullName | Out-File -Append -FilePath C:\Temp\PSDebug.txt }
    . $file.FullName
}
foreach ($file in (Get-ChildItem -Path "$psScriptRoot\functions\public" -Recurse -Filter *.ps1)) {
    if ($FSDebug -gt 0)  { "Loading (public): " + $file.FullName | Out-File -Append -FilePath C:\Temp\PSDebug.txt }
    . $file.FullName
}

$FSDeploymentIsLoading = $false
