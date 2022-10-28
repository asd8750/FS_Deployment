function Get-FSSqlDatabases  {
    <#
.SYNOPSIS

Returns a list of database information objects from a SQL instance

.DESCRIPTION

Returns a data structure containing a list of SQL databases from the supplied instance

.PARAMETER FullInstanceName
Specifies the Fully qualified SQL Server instance name of the global master job server 

.PARAMETER SkipSystemDB

.INPUTS

None. You cannot pipe objects to Checkpoint-FSDeployDirectories

.OUTPUTS

PS Object

.EXAMPLE

PS> Get-FSSqlPartitionInfo -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $FullInstanceName,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [boolean] 
        $SkipSystemDB = $false
    )
    Begin {
        $sqlGetDB = @"
        SELECT	SDB.[name] AS DatabaseName,
                SDB.[database_id],
                CASE	WHEN (SDB.replica_id IS NOT NULL) THEN CONVERT(VARCHAR(20), DATABASEPROPERTYEX(SDB.[name],'Updateability'))
                ELSE 'NONE'
                END AS AGRole,
                ISNULL(SDBM.mirroring_role_desc,'NONE') AS MirrorRole
                --,SDB.*, SDBM.*
            FROM sys.databases SDB
                INNER JOIN sys.database_mirroring SDBM
                    ON (SDB.database_id = SDBM.database_id)
            ORDER BY SDB.database_id
"@;
    }


    #   1) Connect to the instance and get the list of databases
    #
    Process {
        $DbList = @{};
        
        $rsGetDB = Invoke-Sqlcmd -ServerInstance $FullInstanceName -Database 'master' -query $sqlGetDB 

        foreach ($rowDB in ($rsGetDB | Where-Object {(-not $SkipSystemDB) -or ($_.database_id -gt 4) } )) {
            $DbList[$rowDB.DatabaseName] = New-Object PSObject -Property @{
                DatabaseName = $rowDB.DatabaseName
                ID       = $rowDB.database_id
                AGRole   = $rowDB.AGRole
                MirrorRole = $rowDB.MirrorRole
            }
        }
    }


    End {
        #$rsGetPtInfo | FT
        Write-Output $DbList.Values;
    }
}

if (-not $FSDeploymentIsLoading){
    #$dbInfo = Get-FSSqlDatabases -FullInstanceName "PBG1SQL01V105.fs.local" -Verbose -SkipSystemDB $true

    #$dbInfo | ft

    #$ptInfo.PtFunc[0]
}
