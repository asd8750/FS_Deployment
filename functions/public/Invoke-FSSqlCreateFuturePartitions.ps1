

function Invoke-FSSqlCreateFuturePartitions  {
    <#
.SYNOPSIS

Create empty partitions in the future.

.DESCRIPTION

Returns a data structure containing a list of partition functions.  Schemes using the functions,
And tables ties to the schemes.

.PARAMETER FullInstanceName
Specifies the Fully qualified SQL Server instance name of the global master job server 

.PARAMETER Database
Specifies the database name of the deployment control database.  
Default is 'FSDeployDB'

.INPUTS

None. You cannot pipe objects to Checkpoint-FSDeployDirectories

.OUTPUTS

PS Objectlimbaugh


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

        # [Parameter(Mandatory=$false)]
        # [string[]]
        # $PtFunction = @(),

        [Parameter(Mandatory=$false)]
        [string]
        $PtFunctionMask = 'PtFunc*',

        [Parameter(Mandatory=$false)]
        [datetime]
        $IgnoreAfterDate = (Get-Date).AddMonths(12+6),  #  Ignore boundaries more than this date

        [Parameter(Mandatory=$false)]
        [int]
        $DaysAhead = 90,

        [Parameter(Mandatory=$false)]
        [switch]
        $AvoidPrimary,          # If set, Avoid the PRIMARY Filegroup

        [Parameter(Mandatory=$false)]
        [int]
        $DebugLevel = 0
    )

    #   1) Connect to the FSDeployDB database and get the list of registered deployment directories
    #
    Begin {

        $PtCfg = Get-FSSqlPartitionInfo -InstanceName $InstanceName -Database $Database
    }


    Process {

        $FutureLimit = ((Get-Date).Date).AddDays($DaysAhead);

        #   Spin through all partition functions and look for the last defined partition boundary that is 1) less than 1 year 
        #   in the future and 2) determine the relative period of time to the previous boundary.  This will 
        #   be default period going forward.
        #
        foreach ($pfName in ($PtCfg.Functions.Keys | Where-Object { $_ -ilike 'PtFunc*'} | Sort-Object )) {
            $ptFN = $PtCfg.Functions[$pfName]           # Function Info
            if ( -not ($ptFN.Datatype -match '^.*?DATE.*?') ) {
                Write-Verbose "Pt Function: $($pfName)  -- Skipped: Not paftitioned by date"   
                continue;
            }
            
            [int64]$RowCnt = 0
            $RowCnt = ($ptFn.PFRows | Select-Object | Measure-Object -Sum).Sum

            #   Spin through the boundary list and make note of various boundary positions
            #
            $PtIdxToday =  0        # Index of the boundary just prior to today
            $PtIdxIgnore = 0        # Index of the boundary just prior to the ignore date
            $PtIdxLast = 0          # Index of the last boundary (Same as $ptFN.Fanout-2)
            for ($PtIdx = 0; $PtIdx -lt $ptFN.Fanout-1; ++$PtIdx) {
                if ((Get-Date $ptFN.BndList[$PtIdx]) -le (Get-Date).Date) {$PtIdxToday = $PtIdx}
                if ((Get-Date $ptFN.BndList[$PtIdx]) -le ($IgnoreAfterDate).Date) {$PtIdxIgnore = $PtIdx}
                $PtIdxLast = $PtIdx
            }

            $PtIdxELast = $PtIdxLast;   # Assume the final list boundary if the last effective boundary
            if ($PtIdxIgnore -lt $PtIdxELast) {$PtIdxELast = $PtIdxIgnore} # Unless $IgnoreAfterDate boundary precedes this one 

            $PtDays = ([datetime]$ptFN.BndList[$PtIdxELast] - [datetime]$ptFN.BndList[$PtIdxELast-1]).Days
            
            #   Get the default partition width
            #
            $LastBoundary = [datetime]$ptFN.BndList[$PtIdxELast]
            $PtDays = ($LastBoundary - [datetime]$ptFN.BndList[$PtIdxELast-1]).Days
            if ($PtDays -eq 1) { $PtWidth = "DAY"}
            elseif (($PtDays -eq 7) -or ($PtDays -eq 8)) { $PtWidth = "WEEK"}
            elseif (($PtDays -ge 28) -and ($PtDays -le 31)) { $PtWidth = "MONTH"}
            elseif ($PtDays -le 27) { $PtWidth = "DAY"}            
            else { $PtWidth = "YEAR"}
           
            if ($ptFN.Fanout -le 4) {
                Write-Verbose "Pt Function: $($pfName)  -- Skipped: Fanout = $($ptFN.Fanout)"   
                continue;      
            }
            elseif ($null -eq $ptFN.PFRows) {
                Write-Verbose "Pt Function: $($pfName)  -- Skipped: No tables/indexes"   
                continue;                 
            }
            elseif ($ptFN.PFRows[$PtIdxELast] -gt 0) {
                Write-Verbose "Pt Function: $($pfName)  -- Skipped: Rows in last partition"   
                continue;                 
            }

            Write-Verbose "Pt Function: $($pfName)  -- [$($PtWidth) $($PtDays)] --  Last PT Rows: $($ptFN.PFRows[$PtIdxELast])"


            #   Loop to add all needed future partitions to this function
            #
            $LastFutureDateNeeded = (Get-Date).Date.AddDays($DaysAhead);

            while ($LastFutureDateNeeded -gt $LastBoundary) {

                #   Now Spin through all partition schemes attached to this function to set the NEXT FILEGROUP USED
                #
                foreach ($psName in $ptFN.Schemes.Keys) {
                    $ptSCH = $PtCfg.Schemes[$psName]
                    $curSchFG = $ptSCH.FGList[$PtIdxELast]
                    $FGs = ($ptSCH.FGList | Select-Object -Unique)
                    if ($AvoidPrimary) {
                        $FGs = $FGs | Where-Object {$_ -ne 'PRIMARY'} | Select-Object -First 1
                        if ($FGs) {
                            $curSchFG = $FGs
                        }
                    }

                    $sqlAlterPS = "ALTER PARTITION SCHEME [$($PtSch.SchemeName)] NEXT USED [$($curSchFG)]"
                    Write-Verbose ">>> SQL >>> $($sqlAlterPS)"
                    if ($DebugLevel -le 1) {
                        $results = Invoke-Sqlcmd -ServerInstance $InstanceName -Database $Database -Query $sqlAlterPS
                    }

                }

                $FirstDayInMonth = $LastBoundary.AddDays($LastBoundary.Day-1)
                $DaysInMonth = ($FirstDayInMonth.AddMonths(1) - $FirstDayInMonth).Days
                $DaysRemaining =  $DaysInMonth - ($LastBoundary.Day-1)
                switch ($PtWidth) {
                    'DAY' {
                        $NextBndDate = $LastBoundary.AddDays($PtDays)
                    }

                    'WEEK' {
                        if ($DaysRemaining % 7 -eq 0) {
                            $NextBndDate = $LastBoundary.AddDays(7)
                        }
                        else {
                            $NextBndDate = $LastBoundary.AddDays(8)
                        }
                    }

                    'MONTH' {
                        $NextBndDate = $LastBoundary.AddDays($LastBoundary.Day-1).AddMonths(1)
                    }

                    'YEAR' {
                        $NextBndDate = $LastBoundary.AddDays($LastBoundary.DayOfYear-1).AddYears(1)
                    }
                }
                if (($LastBoundary.Month -ne $NextBndDate.Month) -and ($NextBndDate.Day -ne 1)) {
                    $NextBndDate = $NextBndDate.AddDays($NextBndDate.Day-1)     # Adjust the boundary to the first of the month
                }
                
            $DateString = Get-Date -Date  $NextBndDate  -Format "yyyy-MM-ddTHH:mm:ss"
            Write-Verbose " -- Next Boundary = $($DateString)"
            $sqlAlterPF = "ALTER PARTITION FUNCTION [$($ptFN.FuncName)]() SPLIT RANGE (N'$($DateString)')"
            Write-Verbose ">>> SQL >>> $($sqlAlterPF)"
            if ($DebugLevel -le 1) {
                $results = Invoke-Sqlcmd -ServerInstance $InstanceName -Database $Database -Query $sqlAlterPF
            }

            $LastBoundary = $NextBndDate
            }

            $a = 4
        }

        $a = 5
    }

}

if (-not $FSDeploymentIsLoading){

    # $results = Invoke-FSSqlCreateFuturePartitionsByDate -InstanceName "PBG1SQL01L205.fs.local" -Database "ModuleAssembly" -Verbose -AvoidPrimary # -IgnoreAfterDate '2040-1-1' 
     $results = Invoke-FSSqlCreateFuturePartitionsByDate -InstanceName "PBG1SQL01V001.fs.local" -Database "ReliabilityDB" -Verbose -AvoidPrimary # -IgnoreAfterDate '2040-1-1' 

    $a = 3

}