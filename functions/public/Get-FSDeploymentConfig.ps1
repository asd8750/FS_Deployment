function Get-FSDeploymentConfig  {
    <#
.SYNOPSIS

Connect to the local domain master server and retrieve the deployment configuration information

.DESCRIPTION

Return a PSObject with deployment configuration information

.PARAMETER FullInstanceName
Specifies the Fully qualified SQL Server instance name of the domain master job server 

.PARAMETER Database
Specifies the database name of the deployment control database.  
Default is 'FSDeployDB'

.INPUTS

None. You cannot pipe objects to Checkpoint-FSDeployDirectories

.OUTPUTS

Return a PSObject with deployment configuration information

.EXAMPLE

PS> Get-FSDeploymentConfig -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $FullInstanceName,

        [Parameter(Mandatory=$false)]
        [string]
        $Database = "FSDeployDB"
    )

    #   1) Connect to the FSDeployDB database and get the list of registered deployment directories
    #
    Begin {
        $ConfigDirsSQL = @"
        SELECT  [DeployDirID],
                [DirectoryPath]
            FROM [dbo].[DeployDirectories]
            WHERE ([DateDeletedUtc] IS NULL);
"@

        $ConfigMSvrsSQL = @"
        SELECT  [MasterServerID],
                [IsMaster],
                [ServerName],
                [DomainName],
                [InstanceName],
                [FullInstanceName],
                [FQDNServerName],
                [ShortInstanceName]
            FROM [dbo].[vwMasterServers];
"@

         $Config = New-Object -TypeName psobject  # Create the returned custom object container  
    }

    #   2) Spin through the returned rows and extract the proper columns
    #
    Process{
        $DResults = Invoke-Sqlcmd -ServerInstance $FullInstanceName -Database $Database -Query $ConfigDirsSQL
        #$DResults.Rows | FT
        $DirList = @()
        foreach ($DDir in $DResults) {
            $DirList += $DDir["DirectoryPath"]
        }
        $Config | Add-Member NoteProperty DirList $DirList

    #   3) Get the lisy of master servers
    #
        $MSrvResults = Invoke-Sqlcmd -ServerInstance $FullInstanceName -Database $Database -Query $ConfigMSvrsSQL
        $MSvrList = @()
        foreach ($MSvr in $MSrvResults) {
            $MSvrInfo = @{
                ServerName = $MSvr["ServerName"]
                DomainName = $MSvr["DomainName"]
                InstanceName = $MSvr["InstanceName"]
                FullInstanceName = $MSvr["FullInstanceName"]
                FQDNServerName = $MSvr["FQDNServerName"]
                ShortInstanceName = $MSvr["ShortInstanceName"]
                IsMaster = [int]$MSvr["IsMaster"]
            }
            #$MSvrInfo
            $MSvrList += $MSvrInfo
        }
        $Config | Add-Member NoteProperty ServerList $MSvrList

    }

    #   Last) Return the collected data to the caller
    #
    End{
        Return $Config
    }
}
