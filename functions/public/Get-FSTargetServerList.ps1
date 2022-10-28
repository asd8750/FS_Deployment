function Get-FSTargetServerList  {
    <#
.SYNOPSIS

Returns the list of target servers in the MSX servers

.DESCRIPTION

Returns a DataRow array from the "msdb.dbo.sp_help_targetserver" stored procedure

.PARAMETER MasterServer
Specifies the Fully qualified SQL Server instance name of a SQL Server Agent master job server 

.INPUTS

None. You cannot pipe objects to Checkpoint-FSDeployDirectories

.OUTPUTS

DataRow list

.EXAMPLE

PS> Get-FSTargetServerList -MasterServer 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $MasterServer
    )

    #   1) Connect to the FSDeployDB database and get the list of registered deployment directories
    #
    Begin {

        $sqlHelpTargetServer = @"
        EXEC msdb.dbo.sp_help_targetserver
"@;
    }


    Process {
        $rsTrgtList = Invoke-DbaQuery -query $sqlHelpTargetServer -SqlInstance $MasterServer -Database "msdb"

        $trgts = @{}
        foreach ($srv in $rsTrgtList) {
            $trgts[$srv.server_name] = New-Object PSObject -Property @{
                Server       = $srv.server_name
                Location     = $srv.Location
                TimeAdjust   = $srv.time_zone_adjustment
                EnlistDate   = $srv.enlist_date
                LastPollDate = $srv.last_poll_date
                Status       = $srv.status
                UnreadInst   = $srv.unread_instructions
                EnlistedBy   = $srv.enlisted_by_nt_user
                PollIntv     = $srv.poll_interval
            }
        }
    }


    End {
        Write-Output $trgts
    }
}

if (-not $FSDeploymentIsLoading){
    # $ptTargets = Get-FSTargetServerList -MasterServer "EDR1SQL01S003.fs.local\DBA" 
    # $ptTargets
}