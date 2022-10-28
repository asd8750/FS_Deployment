#   Get-FSFileToCompressedStr
  
function Get-FSFileToCompressedBin {

	[CmdletBinding()]
    Param (
	[Parameter(Mandatory, ValueFromPipeline, ValueFromPipelineByPropertyName)]
        [string] $FullFilePath = $(Throw('-FullFilePath path is required'))
    )
	Process {
        [byte[]] $inFileBytes = Get-Content -Path $FullFilePath -Encoding Byte

       	[System.IO.MemoryStream] $output = New-Object System.IO.MemoryStream
        $gzipStream = New-Object System.IO.Compression.GzipStream $output, ([IO.Compression.CompressionMode]::Compress)
      	$gzipStream.Write( $inFileBytes, 0, $inFileBytes.Length )
        $gzipStream.Close()
        $output.Close()
        #$tmp = $output.ToArray()
        Write-Output $output.ToArray()
        $output.Dispose()
    }
}


function Get-DecompressedByteArray {

	[CmdletBinding()]
    Param (
		[Parameter(Mandatory,ValueFromPipeline,ValueFromPipelineByPropertyName)]
        [byte[]] $byteArray = $(Throw("-byteArray is required"))
    )
	Process {
	    Write-Verbose "Get-DecompressedByteArray"
        $input = New-Object System.IO.MemoryStream( , $byteArray )
	    $output = New-Object System.IO.MemoryStream
        $gzipStream = New-Object System.IO.Compression.GzipStream $input, ([IO.Compression.CompressionMode]::Decompress)
	    $gzipStream.CopyTo( $output )
        $gzipStream.Close()
		$input.Close()
		[byte[]] $byteOutArray = $output.ToArray()
        Write-Output $byteOutArray
    }
}

if (-not $FSDeploymentIsLoading){
#$ba = Get-FSFileToCompressedStr ("C:\Downloads\SetSqlPermissionOnMountPointFolders.ps1")
#$ba.Length
}