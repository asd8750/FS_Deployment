function Invoke-FSSqlPartitionAction  {
    <#
.SYNOPSIS

Perform an "action" on a partition object controlled by the Set-FSSqlPtManager

.DESCRIPTION

Based on the configuration items set by the Set-FSSqlPtManager cmdlet on a partition object.  
The action is will perform a predetermined set of steps on the object based on what action is
requested and the configuration parameters for the related partition objects.

.PARAMETER FullInstanceName
Specifies the Fully qualified SQL Server instance name of the global master job server 

.PARAMETER Database
Specifies the database name of the deployment control database.  
Default is 'FSDeployDB'

.PARAMETER Action
Specific "action" requested:
    - PreAlloc - Create future partitions
    - TruncateOld - Truncate the contents of "expired" partitions
    - MergeOld - Merge or eliminate old partitions
    - Custom - Execute a custom procedure


.PARAMETER Force
Invoke the generated TSQL commands

.INPUTS

None. You cannot pipe objects to Checkpoint-FSDeployDirectories

.OUTPUTS

PS Object (table)
    - Action description
    - Generated TSQL command(s) to perform end actions

.EXAMPLE

PS> Get-FSSqlPartitionInfo -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $InstanceName,

        [Parameter(Mandatory=$true)]
        [string]
        $Database,

        [Parameter(Mandatory=$true)]
        [string]
        $Action,

        [Parameter(Mandatory=$false)]
        [switch]
        $Force
    )

    #
    #   Local functions
    #
    function ParseTimePeriod {
        param (
            [Parameter(Mandatory=$true)] [string] $periodCfg
        )
        
        $parts = $periodCfg.Split(' ')
        if ([string]::TryParse()) {}

        $pVal = [int]::Parse($parts[0])
        if ([string]::IsNullOrEmpty($parts[1])) {
            $pUnit = "day"
        } 
        else {
            $pUnit = $parts[1]
        }


    }


    #   1) Connect to the targeted database and get all partitioning info 
    #

    if ((Get-Module | Where-Object { $_.Name -ilike "*fs_deployment" }).Count -eq 0) {
        Write-Verbose "Importing module: 'fs_deployment'"
        #Import-Module -Name fs_deployment
        #Import-Module C:\Projects\DBA-Deployments\FS_Deployment
    }

    $pti = Get-FSSqlPartitionInfo -InstanceName $InstanceName -Database $Database
    if (( -not $pti.DbConfig.Mode ) -or ($pti.DbConfig.Mode -ieq "Disable")) {
        Write-Verbose "Partition Manager is not active for this database"
        return
    }


    #$ptiActive = $pti.  
    
    #
    #   2)  Spin through 
    #

    if ($Action -ieq "PreAllocate") {
        $pti.Functions.Values | Where-Object { ($_.Config).Width -and ($_.Config.PreAlloc) } | Select-Object { 
            $lastBnd = [Datetime]::Parse($_.BndList[$_.BndList.Count - 1]);
            $dateNow = ([Datetime]::Today).Date;
            $daysPreAlloc = ($lastBnd.Subtract($dateNow)).TotalDays
            $rowsInLast = ($_.PfRows)[($_.PfRows).Count - 1]
        }   
    }

    elseif ($Action -ieq "TruncateOld") {

    }


    #   Return the assembled information as an object collection
    #

}

if (-not $FSDeploymentIsLoading){
    $ptInfo = Invoke-FSSqlPartitionAction -InstanceName "PBG1SQL01V105.fs.local" -Database "ProcessData" -Action "PreAllocate" -Verbose

    $test = 0
}