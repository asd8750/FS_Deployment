


## Local FUNCTION --  Submit SQL to Instance
function local:SubmitSqlCommand {
    Param (
        [parameter(Mandatory=$true)]
        [string] 
        $ServerInstance,

        [Parameter(Mandatory=$true)]
        [string]
        $Query
    )

    Process {
        try {
            $sResult = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $Query
            Write-Output $true
        }
        catch {
            Write-Error $_.Exception.Message
            Write-Error $_.StackTrace
            Write-Output $false
        }         
    }
}


## LOCAL FUNCTION --  SetupAG

function local:SetupAG {
    Param (
        [parameter(Mandatory=$true)]
        [PSCustomObject]
        $AGSetup,        
        
        [parameter(Mandatory=$true)]
        [PSCustomObject]
        $AGName,

        [parameter(Mandatory=$false)]
        [int]
        $DebugLevel = 2
    )

    Process {
        $CheckForListener = $false          # No listener needed yet
        
        #   Check for primary AG replica
        #
        $idx = 0;
        if (-not $AGSetup.Primary) {
            $AGSetup.Primary = $AGSetup.Others[$idx++]
           
            Write-Verbose "-- Server ($($AGSetup.Primary.HADRConfig.ServerFQDN)) Configuring the PRIMARY for AG '$($AGName)'"    

            $sqlAGPrimary = "
                        CREATE AVAILABILITY GROUP [$($AGName)]
                            WITH (AUTOMATED_BACKUP_PREFERENCE = PRIMARY,
                                DB_FAILOVER = ON,
                                DTC_SUPPORT = PER_DB,
                                HEALTH_CHECK_TIMEOUT = 30000)
                            FOR 
                                REPLICA ON N'$($AGSetup.Primary.HADRConfig.CurrentServer)' 
                                    WITH (ENDPOINT_URL = N'TCP://$($AGSetup.Primary.HADRConfig.ServerFQDN):$($AGSetup.Primary.HADRConfig.Endpoint.TCPPort)', 
                                        FAILOVER_MODE = AUTOMATIC, 
                                        AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, 
                                        PRIMARY_ROLE(ALLOW_CONNECTIONS = ALL), 
                                        SECONDARY_ROLE(ALLOW_CONNECTIONS = NO),
                                        SEEDING_MODE = AUTOMATIC);";   

            Write-Verbose "-- Server ($($AGSetup.Primary.HADRConfig.ServerFQDN)) -- AG: $($AGName) -- >>> SQL: $($SqlAGPrimary)"    
            if ($DebugLevel -le 1) {
                    $results = SubmitSqlCommand -ServerInstance $AGSetup.Primary.HADRConfig.ServerFQDN -Query $sqlAGPrimary
                    Write-Verbose "-- Server ($($AGSetup.Primary.HADRConfig.ServerFQDN)) -- AG: $($AGName) -- >>> Waiting..."    
                    Start-Sleep -Seconds 5
            }

            $CheckForListener = $true       # A new PRIMARY needs a listener.  Signal for follow on test         
        }

        else {
            #   The PRIMARY is configured.  Is a listener present?  If not, we need to check the function parameters for info
            Write-Verbose "-- Server ($($AGSetup.Primary.HADRConfig.ServerFQDN)) -- AG: $($AGName) -- PRIMARY already configured"  
            if (($null -eq $AGSetup.Primary.AG.Listener) -and ($null -ne $AGSetup.Listener)) {
                $CheckForListener = $true
            }
        }

        #   Listener Check -- No is present on the PRIMARY AG.  Create one if the info supplied to this function

        if ($CheckForListener) {
            #   Now create the listener
            #
            if ($AGSetup.Listener) {
                Write-Verbose "-- Server ($($AGSetup.Primary.HADRConfig.ServerFQDN)) -- AG: $($AGName) -- Configuring Listener [$($AGSetup.Listener.Name)]"    
                $sqlAddListener = "ALTER AVAILABILITY GROUP [$($AGName)] 
                        ADD LISTENER N'$($AGSetup.Listener.Name)' ( WITH IP ((N'$($AGSetup.Listener.IPAddr)', N'$($AGSetup.Listener.Subnet)')), PORT=1433)";
                Write-Verbose "-- Server ($($AGSetup.Primary.HADRConfig.ServerFQDN)) -- AG: $($AGName) --  >>> SQL: $($sqlAddListener)"    
                if ($DebugLevel -le 1) {
                    $results = SubmitSqlCommand -ServerInstance $AGSetup.Primary.HADRConfig.ServerFQDN -Query $sqlAddListener
                    Write-Verbose "-- Server ($($AGSetup.Primary.HADRConfig.ServerFQDN)) -- AG: $($AGName) --  >>> Waiting..."    
                    Start-Sleep -Seconds 10
                }
            }
        }


        #   Now configure the secondaries
        #
        for (; $idx -lt $AGSetup.Others.Count; $idx++) {
            $svr = $AGSetup.Others[$idx]
            $sqlAddSecondary = "
                ALTER AVAILABILITY GROUP [$($AGName)]
                        ADD REPLICA ON N'$($svr.HADRConfig.CurrentServer)' 
                            WITH (ENDPOINT_URL = N'TCP://$($svr.HADRConfig.ServerFQDN):$($svr.HADRConfig.Endpoint.TCPPort)', 
                                FAILOVER_MODE = AUTOMATIC, 
                                AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, 
                                PRIMARY_ROLE(ALLOW_CONNECTIONS = ALL), 
                                SECONDARY_ROLE(ALLOW_CONNECTIONS = NO),
                                SEEDING_MODE = AUTOMATIC);"

            Write-Verbose "-- Server ($($AGSetup.Primary.HADRConfig.ServerFQDN)) -- AG: $($AGName) -- Add new replica -- ($($svr.HADRConfig.CurrentServer))"    

            Write-Verbose "-- Server ($($AGSetup.Primary.HADRConfig.ServerFQDN)) -- AG: $($AGName) -- >>> SQL: $($sqlAddSecondary)"    
            if ($DebugLevel -le 1) {
                $results = SubmitSqlCommand -ServerInstance $AGSetup.Primary.HADRConfig.ServerFQDN -Query $sqlAddSecondary 
                Write-Verbose "-- Server ($($AGSetup.Primary.HADRConfig.ServerFQDN)) -- AG: $($AGName) -- >>> Waiting..."    
                Start-Sleep -Seconds 10
            }

            $sqlJoinSecondary = "ALTER AVAILABILITY GROUP [$($AGName)] JOIN";
            Write-Verbose "-- Server ($($svr.HADRConfig.ServerFQDN)) -- AG: $($AGName) -- >>> SQL: $($sqlJoinSecondary )"    
            if ($DebugLevel -le 1) {
                $results = SubmitSqlCommand -ServerInstance $svr.HADRConfig.ServerFQDN -Query $sqlJoinSecondary 
                Write-Verbose "-- Server ($($svr.HADRConfig.ServerFQDN)) -- AG: $($AGName) -- >>> Waiting..."    
                Start-Sleep -Seconds 5
            }

            $sqlAGGrant = "ALTER AVAILABILITY GROUP [$($AGName)] GRANT CREATE ANY DATABASE;"
            Write-Verbose "- Server ($($svr.HADRConfig.ServerFQDN)) -- AG: $($AGName) -- >>> SQL: $($sqlAGGrant)"    
            if ($DebugLevel -le 1) {
                $results = SubmitSqlCommand -ServerInstance $svr.HADRConfig.ServerFQDN -Query $SqlAGGrant    
            }
        }

    }

}



## LOCAL FUNCTION --  GetCheckServerInfo

function local:GetCheckServerInfo {
    Param (
        [parameter(Mandatory=$true)]
        [string] 
        $ServerInstance,

        [parameter(Mandatory=$false)]
        [switch]
        $IsLocal,

        [parameter(Mandatory=$false)]
        [string]
        $ClusterName,

        [parameter(Mandatory=$false)]
        [string]
        $AGNameRoot
    )

    Process {
        #   Prepare the return object
        $srvInfo = [PSCustomObject]@{
            ServerInstance = $ServerInstance
            IsLocal = $true
            HADRConfig = $null
            ClusterName = $null
            AG = $null
            AGReplica = $null
            AGRole = $null
            Errors = @()
        }

        #   Now go after the config info
        #
        try {
            $sInfo = Get-FSHadrConfig -InstanceName $ServerInstance    

            if ($sInfo) {
                #   Test Cluster membership
                
                if ($null -eq $sInfo.ClusterName) { 
                    $srvInfo.Errors += "-- ERROR -- Server ($($sInfo.CurrentServer)) is NOT a Cluster Member"
                } 
                elseif ((-not [string]::IsNullOrEmpty($ClusterName)) -and ($ClusterName -ne $sInfo.ClusterName)) {
                    $srvInfo.Errors += "-- ERROR -- Server ($($sInfo.CurrentServer)) is NOT part of '$($ClusterName)' cluster"   
                }
                elseif (-not $sInfo.HadrEnabled) {
                    $srvInfo.Errors +=  "-- ERROR -- Server ($($sInfo.CurrentServer)) does not have HADR enabled"       
                }
    
                #   Test valid endpoint
                if ($null -eq $sInfo.EndPoint) {
                    $srvInfo.Errors +=  "-- ERROR -- Server ($($sInfo.CurrentServer)) Requires mirroring endpoint"
                } elseif ($null -eq $sInfo.EndPoint.CertName) {
                    $srvInfo.Errors += "-- ERROR -- Server ($($sInfo.CurrentServer)) Requires endpoint certificate preparation"    
                }

                $srvInfo.HADRConfig = $sInfo
                $srvInfo.ClusterName = $sInfo.ClusterName
                $thisAG = $sInfo.AGList.Values | Where-Object GenericAGName -eq $AGNameRoot | Select-Object -First 1 
                $srvInfo.AG = $thisAG
                $srvInfo.AGReplica = $thisAG.Replicas | Where-Object AGReplServer -eq $sInfo.CurrentServer | Select-Object -First 1
                $srvInfo.AGRole = ($thisAG.Replicas | Where-Object AGReplServer -eq $sInfo.CurrentServer | Select-Object -First 1).AGReplRole
                $a = 2
            }
        }
        catch {
            $srvInfo.Errors += "-- Exception -- $($_.Exception.Message)"
            $srvInfo.Errors += "-- Exception -- $($_.ScriptStackTrace)"
        }   

        Write-Output $srvInfo
    }
}


# =========================================================================
function Deploy-FSAvailabilityGroup {
    <#
.SYNOPSIS

Create/configure the SQL HADR infrastructure required by FS plants

.DESCRIPTION

Create and deploy the SQL objects and settings needed for mirroring traffic between endpoints
to use credential authentication.

Version History
- 2022-08-22 - 1.0 - F.LaForest - Initial version
- 2022-09-08 - 1.1 - F.LaForest - (Phase 1) - Create source AG only

.PARAMETER InstanceList
Specifies an array of FQDN instance names wich will be the endpoints of this mirror/availability group constellation

.PARAMETER Command


.INPUTS

None. You cannot pipe objects to Checkpoint-FSDeployDirectories

.OUTPUTS

Return a PSObject with deployment configuration information

.EXAMPLE

PS> Get-FSDeploymentConfig -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(Mandatory=$true)]
        [string] 
        $PlantCode,

        [parameter(Mandatory=$true)]
        [string] 
        $AGNameRoot,

        [parameter(Mandatory=$true)]
        [string[]] 
        $LocalServers,
    
        [parameter(Mandatory=$false)]
        [string[]] 
        $RmtServers,    
    
        [parameter(Mandatory=$false)]
        [string[]] 
        $Listeners,    

        [Parameter(Mandatory=$false)]
        [switch]
        $Force,

        [Parameter(Mandatory=$false)]
        [switch]
        $CreateDAG,

        [parameter(Mandatory=$false)]
        [string] 
        $GeneratedOutputDir,

        [Parameter(Mandatory=$false)]
        [int32]
        $DebugLevel = 3
    )

    #   Loop through each supplied SQL instance name.. Querying each one to find the current state
    #
    begin {

        $ErrorCnt = 0
        $SrvNames = @{}

        # Generate the 3 full AG names: MFG-side, ODS-side and the distributed name

        $AGNamePrefix  = "AG_$($PlantCode)"
        $AG_Name_MFG = "$($AGNamePrefix)_$($AGNameRoot)"
        $AG_Name_ODS = "$($AG_Name_MFG)_ODS"
        $AG_Name_DAG = "D$($AG_Name_MFG)"


        $AGLocal = [PSCustomObject]@{
            ClusterName = $null
            Servers     = @()
            Primary     = $Null
            Secondaries = @()
            Others       = @()
            Listener    = $null
        }

        $AGRemote = [PSCustomObject]@{
            ClusterName = $null
            Servers     = @()
            Primary     = $Null
            Secondaries = @()
            Others       = @()
            Listener    = $null
        }

        #   Query each listed server for the HADR configuration already in place
        #
        $ClstrLocal = $null
        foreach ($srv in $LocalServers) {
            Write-Verbose " -- Query HADR Info Local Server: $($srv)"
            $sInfo = GetCheckServerInfo -ServerInstance $srv -IsLocal -Cluster $ClstrLocal -AGNameRoot $AGNameRoot

            if ($SrvNames[$sInfo.HADRConfig.CurrentServer]) {
                $sInfo.Errors += "-- ERROR -- Server ($($srv)) is used more than once"    
            }
            $AGLocal.Servers += $sInfo
            $AGLocal.ClusterName = $sInfo.ClusterName
            if ($sInfo.AGrole -eq "PRIMARY") { $AGLocal.Primary = $sInfo}
            elseif ($sInfo.AGrole -eq "SECONDARY") { $AGLocal.Secondaries += $sInfo}
            else { $AGLocal.Others += $sInfo }

            foreach ($errmsg in $sInfo.Errors) {
                Write-Error $errmsg
                $ErrorCnt++
            }
        }

        #   Query servers across the firewall for DAG setup
        #
        if ($CreateDAG) {
            $ClstrRemote = $null
            foreach ($srv in $RmtServers) {
                Write-Verbose " -- Query HADR Info Remote Server: $($srv)"                        
                $sInfo = GetCheckServerInfo -ServerInstance $srv -Cluster $ClstrRemote -AGNameRoot $AGNameRoot

                if ($SrvNames[$sInfo.HADRConfig.CurrentServer]) {
                    $sInfo.Errors += "-- ERROR -- Server ($($srv)) is used more than once"    
                }
                $AGRemote.Servers += $sInfo
                $AGRemote.ClusterName = $sInfo.Cluster
                if ($sInfo.AGrole -eq "PRIMARY") { $AGRemote.Primary = $sInfo}
                elseif ($sInfo.AGrole -eq "SECONDARY") { $AGRemote.Secondaries += $sInfo}
                else { $AGRemote.Others += $sInfo }
                    
                foreach ($errmsg in $sInfo.Errors) {
                    Write-Error $errmsg
                    $ErrorCnt++
                }
            }
        }

        
        #   Decode the Listener parameter
        #   @("ListenerName1,IPAddr1/CIDR") or 
        #   @("ListenerName1,IPAddr1/CIDR1","ListenerName2,IPAddr2/CIDR2") or 
        #   Listener names are simple names, NOT FQDN
        #
        $Lstnrs = @($Null, $null)   # Listeners for the Local and Remote side
        $idx = 0
        foreach ($lst in $Listeners) {
            $lstParts = ($lst -split ',') 
            if ($lstParts) {
                if ($lstParts.count -gt 1) {
                    $lNet = $lstParts[1] -split '/'
                    if ($lNet.count -eq 1) {
                        $IP = $lNet[0]
                        $subnet = '255.255.254.0'   # FS standard
                    } else {
                        $IP = $lNet[0]
                        switch ($lNet[1]) {
                            '23'  {$subnet = '255.255.254.0'}
                            '24'  {$subnet = '255.255.255.0'}
                            '25'  {$subnet = '255.255.255.128'}
                            '26'  {$subnet = '255.255.254.192'}
                            '27'  {$subnet = '255.255.254.224'}
                            '28'  {$subnet = '255.255.254.240'}
                        }
                    }
                }
                $LObj = [PSCustomObject]@{
                    Name    = $lstParts[0]
                    IPAddr  = $IP
                    Subnet  = $subnet
                }
                if ($idx -eq 0) { $AGLocal.Listener = $LObj; }
                elseif ($idx -eq 1) { $AGRemote.Listener = $LObj; }
            }
            $idx++
        }


        if ($ErrorCnt -gt 0) {
            Write-Error "-- ERROR -- Encountered $($ErrorCnt) error(s) - Terminated"  
            return
        }

        #   1) -- Create the local AG
        #
        SetupAG -AGSetup $AGLocal -AGName $AG_Name_MFG -DebugLevel $DebugLevel 

        #   2) -- Create the remote AG
        #
        if ($CreateDAG) {
            SetupAG -AGSetup $AGRemote -AGName $AG_Name_ODS -DebugLevel $DebugLevel  
            
            #   Now create the DAG

            if (($null -eq $AGLocal.Primary.AG.DAG) -and ($null -eq $AGRemote.Primary.AG.DAG)) {
                $sqlCreateDAG = "
                    CREATE AVAILABILITY GROUP [$($AG_Name_DAG)]   
                        WITH (DISTRIBUTED) AVAILABILITY GROUP ON
                        N'$($AG_Name_MFG)'  -- 
                        WITH (LISTENER_URL = N'tcp://$($AGLocal.Listener.Name).$($AGLocal.Primary.HADRConfig.CurrentDomain):$($AGLocal.Primary.HADRConfig.EndPoint.TCPPort)', 
                            FAILOVER_MODE = MANUAL,  
                            AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                            SEEDING_MODE = AUTOMATIC),  
                        N'$($AG_Name_ODS)' --
                        WITH (LISTENER_URL = N'tcp://$($AGRemote.Listener.Name).$($AGRemote.Primary.HADRConfig.CurrentDomain):$($AGRemote.Primary.HADRConfig.EndPoint.TCPPort)',   
                            FAILOVER_MODE = MANUAL,   
                            AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                            SEEDING_MODE = AUTOMATIC)";

                Write-Verbose "-- DAG: $($AG_Name_DAG) -- >>> SQL: $($sqlCreateDAG)"    
                if ($DebugLevel -le 1) {
                    $results = SubmitSqlCommand -ServerInstance "$($AGLocal.Listener.Name).$($AGLocal.Primary.HADRConfig.CurrentDomain)" -Query $sqlCreateDAG 
                    Write-Verbose "-- AG: $($AG_Name_DAG) -- >>> Waiting..."    
                    Start-Sleep -Seconds 10
                }
        
                $sqlJoinDAG = "
                    ALTER AVAILABILITY GROUP [$($AG_Name_DAG)]   
                        JOIN AVAILABILITY GROUP ON
                            N'$($AG_Name_MFG)' 
                            WITH (LISTENER_URL = N'tcp://$($AGLocal.Listener.Name).$($AGLocal.Primary.HADRConfig.CurrentDomain):$($AGLocal.Primary.HADRConfig.EndPoint.TCPPort)', 
                                FAILOVER_MODE = MANUAL,  
                                AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                                SEEDING_MODE = AUTOMATIC),  
                            N'$($AG_Name_ODS)' 
                            WITH (LISTENER_URL = N'tcp://$($AGRemote.Listener.Name).$($AGRemote.Primary.HADRConfig.CurrentDomain):$($AGRemote.Primary.HADRConfig.EndPoint.TCPPort)',   
                                FAILOVER_MODE = MANUAL,   
                                AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT,   
                                SEEDING_MODE = AUTOMATIC);
                    PRINT 'AG [$($AG_Name_DAG)] Join Setup on Primary'
                    PRINT '... Sleeping 10 sec ...'
                    WAITFOR DELAY '00:00:10'
                    PRINT 'New:  Grant Create All Databases permission on $($AG_Name_DAG)'
                    ALTER AVAILABILITY GROUP [$($AG_Name_DAG)] GRANT CREATE ANY DATABASE;
                    PRINT '... Sleeping 10 sec ...'
                    WAITFOR DELAY '00:00:10'";

                Write-Verbose "-- DAG: $($AG_Name_DAG) -- >>> SQL: $($sqlJoinDAG)"    
                if ($DebugLevel -le 1) {
                    $results = SubmitSqlCommand -ServerInstance "$($AGRemote.Primary.HADRConfig.CurrentServer).$($AGRemote.Primary.HADRConfig.CurrentDomain)" -Query $sqlJoinDAG 
                    Write-Verbose "-- AG: $($AG_Name_DAG) -- >>> Waiting..."    
                    Start-Sleep -Seconds 5
                }

                #  $results = Submit-FSSQLCmd -Instance "$($AGRemote.Primary.HADRConfig.CurrentServer).$($AGRemote.Primary.HADRConfig.CurrentDomain)" -Query $sqlJoinDAG 

                $sqlAllowConnDAG = "
                ALTER AVAILABILITY GROUP [$($AG_Name_ODS))]  
                    MODIFY REPLICA ON  N'$($AGRemote.Primary.HADRConfig.CurrentServer)' WITH   
                        (SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL)); ";
 
                Write-Verbose "-- DAG: $($AG_Name_DAG) -- >>> SQL: $($sqlJoinDAG)"    
                if ($DebugLevel -le 1) {
                    $results = SubmitSqlCommand -ServerInstance "$($AGRemote.Primary.HADRConfig.CurrentServer).$($AGRemote.Primary.HADRConfig.CurrentDomain)" -Query $sqlJoinDAG 
                    Write-Verbose "-- AG: $($AG_Name_DAG) -- >>> Waiting..."    
                    Start-Sleep -Seconds 5
                }
                        
            }

        }




        #   SQL commands to enable the XE AlwaysOn health session
        #
        # $sqlXEHealth = "
        #     IF EXISTS(SELECT * FROM sys.server_event_sessions WHERE name='AlwaysOn_health')
        #         BEGIN
        #             ALTER EVENT SESSION [AlwaysOn_health] ON SERVER WITH (STARTUP_STATE=ON);
        #         END
        #         IF NOT EXISTS(SELECT * FROM sys.dm_xe_sessions WHERE name='AlwaysOn_health')
        #         BEGIN
        #             ALTER EVENT SESSION [AlwaysOn_health] ON SERVER STATE=START;
        #         END;"

        # #   Now search the collected HADR config for the $AGNameRoot
        # #
        # $rplPri = $Repls[0]     # Primary replica

        # Write-Verbose "-- ($($rplPri.sInfo.CurrentServer)) -- Enable AlwaysOn XE session"    
        # if ($DebugLevel -le 1) {
        #     try {
        #         $sResult = Invoke-Sqlcmd -ServerInstance $rplPri.sInfo.ServerFQDN -Query $sqlXEHealth
        #     }
        #     catch {
        #         Write-Error $_.Exception.Message
        #         Write-Error $_.StackTrace
        #     }
        # }

        # ToDo: Test existance in MFG then ODS then DAG

        # ToDo: Create primary and then secondaries (Test existance)

            $a = 4

    }

}

if (-not $FSDeploymentIsLoading){

    # $AGNodes_PMT1_Ods             = @("PKM1SQL01T104.fs.local", "PKM2SQL01T104.fs.local")
    # $PMT1_Listeners_Ods           = @("PKM1SQL01L105,10.41.9.243", $null )
    # Deploy-FSAvailabilityGroup -PlantCode 'PMT1' -AGNameRoot 'Local' -LocalServers $AGNodes_PMT1_ODS -RmtServers @() `
    #                            -Listeners $PMT1_Listeners_Ods -DebugLevel 1 -Verbose -Force
    # 

    # $AGNodes_PMT1_Ods             = @("PKM1SQL01T104.fs.local", "PKM2SQL01T104.fs.local")

    ##############################################
      
    # $AGNodes_PMT1_Mfg_SqlProd     = @("PKM1SQL20T101.mfg.fs",       "PKM2SQL20T101.mfg.fs")
    # $PMT1_Listeners_SqlProd       = @("PKM1SQL20L101,10.41.35.131", "PKM1SQL01L106,10.41.9.242")

    # Deploy-FSAvailabilityGroup -PlantCode 'PMT1' -AGNameRoot 'MesSqlProd' -LocalServers $AGNodes_PMT1_Mfg_SqlProd -RmtServers $AGNodes_PMT1_Ods `
    #                                 -Listeners $PMT1_Listeners_SqlProd      -CreateDAG -DebugLevel 1  -Verbose  

    ##############################################

    # $AGNodes_PMT1_Mfg_SqlSpc      = @("PKM1SQL20T102.mfg.fs",       "PKM2SQL20T102.mfg.fs")
    # $PMT1_Listeners_SqlSpc        = @("PKM1SQL20L102,10.41.35.129", "PKM1SQL01L107,10.41.9.241")
 
    # Deploy-FSAvailabilityGroup -PlantCode 'PMT1' -AGNameRoot 'MesSqlSpc'  -LocalServers $AGNodes_PMT1_Mfg_SqlSpc  -RmtServers $AGNodes_PMT1_Ods `
    #                               -Listeners $PMT1_Listeners_SqlSpc       -CreateDAG -DebugLevel 1 -Verbose 

    # $PMT1_Listeners_SqlSpc_SqlSentry = @("PKM1SQL20L112,10.41.35.128", $null)
    # Deploy-FSAvailabilityGroup -PlantCode 'PMT1' -AGNameRoot 'LocalSpc'   -LocalServers $AGNodes_PMT1_Mfg_SqlSpc `
    #                              -Listeners $PMT1_Listeners_SqlSpc_SqlSentry -DebugLevel 1 -Verbose 

    ##############################################

    # $AGNodes_PMT1_Mfg_SqlMisc     = @("PKM1SQL20T103.mfg.fs",       "PKM2SQL20T103.mfg.fs")
    # $PMT1_Listeners_SqlMisc       = @("PKM1SQL20L103,10.41.35.123", "PKM1SQL01L108,10.41.9.240")

    # Deploy-FSAvailabilityGroup -PlantCode 'PMT1' -AGNameRoot 'MesSqlMisc' -LocalServers $AGNodes_PMT1_Mfg_SqlMisc  -RmtServers $AGNodes_PMT1_Ods `
    #                               -Listeners $PMT1_Listeners_SqlMisc        -CreateDAG -DebugLevel 1 -Verbose 

    # $PMT1_Listeners_SqlMisc_Logs  = @("PKM1SQL20L113,10.41.35.124", $null)
    # Deploy-FSAvailabilityGroup -PlantCode 'PMT1' -AGNameRoot 'MesLogging'   -LocalServers $AGNodes_PMT1_Mfg_SqlMisc `
    #                              -Listeners $PMT1_Listeners_SqlMisc_Logs    -DebugLevel 1 -Verbose 

    ##############################################

    # $AGNodes_PMT1_Mfg_SqlPrcData  = @("PKM1SQL20T104.mfg.fs",       "PKM2SQL20T104.mfg.fs")
    # $PMT1_Listeners_SqlPrcData    = @("PKM1SQL20L104,10.41.35.119", "PKM1SQL01L109,10.41.9.239")

    # Deploy-FSAvailabilityGroup -PlantCode 'PMT1' -AGNameRoot 'MesSqlPrcData' -LocalServers $AGNodes_PMT1_Mfg_SqlPrcData  -RmtServers $AGNodes_PMT1_Ods `
    #                               -Listeners $PMT1_Listeners_SqlPrcData      -CreateDAG  -DebugLevel 1 -Verbose 

    ####################################################

    # $AGNodes_PMT1_PDR_0   = @("PKM1SQL20T140.mfg.fs",       "PKM2SQL20T140.mfg.fs")
    # $PMT1_Listeners_PDR_0 = @("PKM1SQL20L140,10.41.35.115", $null)

    # $AGNodes_PMT1_PDR_1   = @("PKM1SQL20T141.mfg.fs",       "PKM2SQL20T141.mfg.fs")
    # $PMT1_Listeners_PDR_1 = @("PKM1SQL20L141,10.41.35.111", $null)

    # $AGNodes_PMT1_PDR_2   = @("PKM1SQL20T142.mfg.fs",       "PKM2SQL20T142.mfg.fs")
    # $PMT1_Listeners_PDR_2 = @("PKM1SQL20L142,10.41.35.107", $null)

    # $AGNodes_PMT1_PDR_3   = @("PKM1SQL20T143.mfg.fs",       "PKM2SQL20T143.mfg.fs")
    # $PMT1_Listeners_PDR_3 = @("PKM1SQL20L143,10.41.35.103", $null)

    # Deploy-FSAvailabilityGroup -PlantCode 'PMT1' -AGNameRoot 'LocalPDR' -LocalServers $AGNodes_PMT1_PDR_0 `
    #                               -Listeners $PMT1_Listeners_PDR_0      -DebugLevel 2 -Verbose 


  }
