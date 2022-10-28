# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
function New-FSTableCopyCtl  {
    <#
.SYNOPSIS

Build a PSOBJECT containing information needed by the table copy/transform cmdlets to create and populate tables.

.DESCRIPTION

Create a custom PSOBJECT containing the source and destination table information needed by subsequent cmdlets.
The cmdlets will create output tables, triggers and optionally transfer row data from a source table to
the destination table.

.EXAMPLE

PS> New-FSTableTransform

#>
    [OutputType('FSCopyTableControl')]
    [CmdletBinding()]
    Param (
        # Source Table Information
        #
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $SrcInstance,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $SrcDatabase,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $SrcTableSchema,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $SrcTableName,

        # Destination Table Information
        #
        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] 
        $DestInstance,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] 
        $DestDatabase ,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] 
        $DestTableSchema ,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] 
        $DestTableName,

        # Placement Information

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] 
        $DestFileGroup,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] 
        $DestPtScheme,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] 
        $DestPtColumn
    )

    Begin {

        if ((Get-Module | Where-Object { $_.Name -ilike "*dbatools*" }).Count -eq 0) {
            Write-Verbose "Importing module: 'dbatools'"
            Import-Module -Name dbatools
        }

        if ((Get-Module | Where-Object { $_.Name -ilike "*dbatools*" }).Count -eq 0) {
            Write-Verbose "Importing module: 'dbatools'"
            Import-Module -Name dbatools
        }

        #   Initialize default values
        #
        if (-not $DestInstance) { 
            $DestInstance = $SrcInstance
            Write-Verbose " -DestInstance defaulted --> '$($SrcInstance)'"
        }
        if (-not $DestDataBase) { 
            $DestDatabase = $SrcDatabase
            Write-Verbose " -DestDatabase defaulted --> '$($SrcDatabase)'"
        }
        if (-not $DestTableSchema) { 
            $DestTableSchema = $SrcTableSchema
            Write-Verbose " -DestTableSchema defaulted --> '$($SrcTableSchema)'"
        }
        if (-not $DestTableName) { 
            $DestTableName = $SrcTableName
            Write-Verbose " -DestTableName defaulted --> '$($SrcTableName)'"
        }
        
        #
        #   Information queried from the source instance and database
        #
        $srcInstInfo = (Get-DbaDatabase -SqlInstance $srcInstance -Database $SrcDatabase)
        if ($srcInstInfo) {
            Write-Verbose "Source-Instance:   [$($SrcInstance)]"
            $srcDBInfo =  ($srcInstInfo | WHERE-Object Name -ieq $SrcDatabase)
            if ($srcDBInfo) {
                Write-Verbose "Source-Database:   [$($SrcInstance)].[$($SrcDatabase)]"
                $srcDBGuid = $srcDBInfo.DatabaseGuid
                $srcTableInfo = ($srcDBInfo.Tables | Where-Object {($_.Schema -ieq $SrcTableSchema) -and ($_.Name -ieq $SrcTableName)})
                if ($srcTableInfo) {
                    Write-Verbose "Source-Table:      [$($SrcInstance)].[$($SrcDatabase)].[$($srcTableInfo.Schema)].[$($srcTableInfo.Name)]"
                    #$srcTableInfo | FT  
                    $srcPtInfo = Get-FSSqlPartitionInfo -InstanceName $SrcInstance -Database $SrcDatabase
                    [double]$srcRows = $srcTableInfo.RowCountAsDouble
                    $SrcFileGroup = $srcTableInfo.FileGroup
                    $srcPtScheme = $srcTableInfo.PartitionScheme
                    if ($srcPtScheme) {
                        $srcPtColumn = $srcTableInfo.PartitionSchemeParameters[0].Name
                    }
                }
                else {
                    Write-Error "Cannot find table: [$($SrcInstance)].[$($SrcDatabase)].[$($SrcTableSchema)].[$($SrcTableName)]"
                }
            }
            else {
                Write-Error "Cannot find database: [$($SrcInstance)].[$($SrcInstance)].[$($SrcDatabase)]"
            }
        }
        else {
            Write-Error "Cannot find instance: '$($SrcInstance)'"
        }
        
        #
        #   Information queried from the destination instance and database
        #
        $destInstInfo = (Get-DbaDatabase -SqlInstance $DestInstance -Database $DestDatabase)
        if ($destInstInfo) {
            Write-Verbose "Destination-Instance:   [$($DestInstance)]"
            $destDBInfo =  ($destInstInfo | WHERE-Object Name -ieq $DestDatabase)
            if ($destDBInfo) {
                Write-Verbose "Destination-Database:   [$($DestInstance)].[$($DestDatabase)]"
                $destDBGuid = $destDBInfo.DatabaseGuid
                $destTableInfo = ($destDBInfo.Tables | Where-Object {($_.Schema -ieq $DestTableSchema) -and ($_.Name -ieq $DestTableName)})
                Write-Verbose "Destination-Table:      [$($DestInstance)].[$($DestDatabase)].[$($DestTableSchema)].[$($DestTableName)]"
                if ($destTableInfo) {  # Does the table exist?
                    $destTableExists = $true   
                }
                else {
                    $destTableExists = $false
                }
                if ($DestPtScheme) { # Partitioning takes precedence over simple file group placement
                    if ($DestPtScheme -ieq "None") { # No partitioning wanted?
                        $DestPtScheme = $null
                        $DestPtColumn = $null
                        if (-not $DestFileGroup) { $DestFileGroup = $srcFileGroup } # If no filegroup specified, then use FG of src table
                        if (-not $DestFileGroup) { $DestFIleGroup = "PRIMARY" }  # If still no FG, just use PRIMARY
                    }
                    else {
                        $DestFileGroup = $null  # Clear FG if partitioning specified
                    }
                }
                else { # No specific partitioning instructions?
                    if ($srcPtScheme) {
                        $DestPtScheme = $srcPtScheme  # Use src table partitioning specs if present
                        $DestPtColumn = $srcPtColumn
                        $DestFileGroup = $null
                    }
                    else { # No partition specified or copied from src table
                        if (-not $DestFileGroup) { $DestFileGroup = $srcFileGroup }  # Use src table filegroup
                        if (-not $DestFileGroup) { $DestFIleGroup = "PRIMARY" }
                    }
                }

                #   Display the selected storage selection
                if ($DestPtScheme) {
                    Write-Verbose "Destination-Partition Scheme:  [$($DestPtScheme)]"
                    Write-Verbose "Destination-Partition Column:  [$($DestPtColumn)]"
                }
                else {
                    Write-Verbose "Destination-FileGroup:  [$($DestFileGroup)]"
                }

            }
            else {
                Write-Error "Cannot find database: [$($DestInstance)].[$($DestInstance)].[$($DestDatabase)]"
            }
        }
        else {
            Write-Error "Cannot find instance: '$($DestInstance)'"
        }
        
        #
        #   Calculated Values
        #
        $IsSameInstance = if (($srcInstance -ieq $destInstance)) {$true} else {$false}
        $IsSameDB   = if ($srcDBGuid -eq $destDBGuid) {$true} else {$false}
        $IsSameTable = $false
        if ($IsSameDB -and ($null -ne $destTabInfo)) {
            $IsSameTable = if ($srcTabInfo.ID -eq $destTabInfo.ID) {$true} else {$false}
        }
    }

    Process { }    

    End {
        # Create the new PSOBJECT to hold all the information
        #
        $ttObj = [PSCustomObject]@{
            PSTypeName = 'FSCopyTableControl'
            # Source table information
            SrcInstance = $SrcInstance
            SrcDatabase = $SrcDatabase
            SrcTableSchema = $SrcTableSchema
            SrcTableName = $SrcTableName
            SrcFullTableName = "[$($SrcTableSchema)].[$($SrcTableName)]"
            SrcDBGuid = $srcDBGuid

            SrcFileGroup = $SrcFileGroup
            SrcPtScheme = $srcPtScheme
            SrcPtColumn = $srcPtColumn
            SrcTableInfo = $srcTableInfo
            SrcRowCount = $srcRows

            SrcPtInfo = $srcPtInfo

            # Destination table
            DestInstance = $DestInstance
            DestDatabase = $DestDatabase
            DestTableSchema = $DestTableSchema
            DestTableName = $DestTableName
            DestFullTableName = "[$($DestTableSchema)].[$($DestTableName)]"
            DestDBGuid = $destDBGuid

            DestFileGroup = $DestFileGroup
            DestPtScheme = $DestPtScheme
            DestPtColumn = $DestPtColumn
            DestTableExsts = $destTableExists
            DestTableInfo = $destTableInfo

            # General information
            IsSameInstance = $IsSameInstance
            IsSameDB = $IsSameDB
            IsSameTable = $IsSameTable
        }
        Write-Output $ttObj
    }
}

# Test statements

if (-not $FSDeploymentIsLoading){
    $obj = New-FSTableCopyCtl -SrcInstance "PBG1SQL01V105.fs.local" -SrcDatabase "ClipProcessData" -SrcTableSchema "DBA-Post" -SrcTableName "CIIVRaw" `
                               -DestTableSchema "DBA-SWitch" -Verbose
    $obj | Format-List -Property *
}