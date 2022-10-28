# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


function global:Get-FSDagStatus  {
    <#
.SYNOPSIS

Obtain the full AG/DAG HA status of the S6 server/instance status

.DESCRIPTION

When executed from an S6 plant ODS instance, this script will determine the AG/DAG structure in this cluster.  Then a remote query 
is submitted to each component SQL instance for additional status information.  
Once obtained, the combination status is saved to a central database.
/

.EXAMPLE

PS> Get-FSDagStatus -FullInstanceName 'EDR1SQL01S004.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $S6OdsInstance,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $RepoInstance = "",

        # [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        # [System.Management.Automation.PSCredential] 
        # $RepoCredential = $null,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $RepoUserName = "",

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $RepoDatabase = ""
    )

    Begin {
        $sqlConfig1 = "SELECT [name], [value] FROM sys.configurations WHERE ([name] = 'Ad Hoc Distributed Queries')"

        try {   
            $agRmtConf = Invoke-SqlCmd -query $sqlConfig1 -ServerInstance $S6OdsInstance 
            $adHocValue = $agRmtConf["value"]
            $resetAdHoc = $false
            if ($adHocValue -eq 0) {
                Write-Verbose "Enable AdHoc Distributed Queries  : $($fetchSvr.Instance)"

                $sqlSetAdvanced = "sp_configure 'show advanced options', 1;  RECONFIGURE;"
                $agRmtConf = Invoke-SqlCmd -query $sqlSetAdvanced  -ServerInstance $S6OdsInstance 
            
                $sqlSetAdHoc = "sp_configure 'Ad Hoc Distributed Queries', 1;  RECONFIGURE;"
                $agRmtConf = Invoke-SqlCmd -query $sqlSetAdHoc -ServerInstance $S6OdsInstance 
                $resetAdHoc = $true
            
                $agRmtConf = Invoke-SqlCmd -query $sqlConfig1 -ServerInstance $S6OdsInstance 
                $adHocValue = $agRmtConf["value"]    
            }
        }
        catch {
            Write-Warning "Error checking Distributed Query flag: $($S6OdsInstance)"
            Write-Warning "Exception: $($_.Exception.Message)"
        }


        $rpPasswd = ConvertTo-SecureString $RepoPassword -AsPlainText -Force
        $rpCredential = New-Object System.Management.Automation.PSCredential ($RepoUserName, $rpPasswd)     
    }

    Process {
         $VerbosePreference="SilentlyContinue"

        $FetchList = @{};   # Array for servers to query
        $FetchCompleteCnt = 0 # Number of completed fetchs

        $FetchList[$S6OdsInstance] = New-Object PSObject -Property @{
            Instance = $S6OdsInstance
            Location = "ODS"
            Status = "Fetch"
        }

        $InstCompleted = @{};
        $DBRows = @();
        #   Loop until all queued fetch requests are processed
        #
        while ($FetchList.Count -gt $FetchCompleteCnt) {
            $FetchList.GetEnumerator().Where({($_.Value).Status -ieq "Fetch" } ) | ForEach-Object {
                $fetchSvr = $_.Value
                #$fetchSvr
                try {
                    Write-Verbose "Fetching  : $($fetchSvr.Instance)"
                    $SourceInst = $fetchSvr.Instance
                    if ($fetchSvr.Location -eq "ODS") {
                        $instResults = Get-HADRInfo -OdsLabel $S6OdsInstance -Instance $fetchSvr.Instance 
                    }
                    else {
                        $instResults = Get-HADRInfo -OdsLabel $S6OdsInstance -Instance $S6OdsInstance `
                                    -RemoteInstance $fetchSvr.Instance -Remote $true -Username $RepoUserName -Password $RepoPassword               
                    }
                    $fetchSvr.Status = "Completed"
                    $fetchSvr | Add-Member -MemberType NoteProperty -Name "ResultSet" -Value $instResults

                    #   If rows were returned from the query, check the SourceInst column for the name of the instance.
                    if ($instResults -ne $null) {
                        $rowCount = $instResults.Count  
                        if ($rowCount -gt 0) { $SourceInst = $instResults[0].SourceInst }             
                        Write-Verbose "Processing: $($fetchSvr.Instance) ==> $($SourceInst) ($($rowCount) Rows)" 
                        if (($rowCount -gt 0) -and ($InstCompleted[$SourceInst] -eq $null)) {
                            $InstCompleted[$SourceInst] = $SourceInst # $instResults[0].EndPointServer
                            foreach ($db in $instResults) {
                                #   Check for the other AG replicas 
                                if (($FetchList[$db.AGReplServer] -eq $null) -and (-not $db.DBIsLocal)) {
                                    Write-Verbose "Queue     : $($db.AGReplServer) ==> $($db.EndPointServer) - ($($fetchSvr.Location))"
                                    $FetchList[$db.AGReplServer] = New-Object PSObject -Property @{
                                        Instance = $db.EndPointServer
                                        Location = $fetchSvr.Location
                                        Status = "Fetch"
                                    }
                                }
            
                                #   Check for other DAG replicas on the MFG side of the firewall
                                if ($db.InDAG) {
                                    if (($fetchSvr.Location -eq "ODS") -and ($FetchList[$db.DAGRmtSvr] -eq $null)) {
                                        Write-Verbose "Queue     : $($db.DAGRmtSvr) -- (MFG)"
                                        $FetchList[$db.DAGRmtSvr] = New-Object PSObject -Property @{
                                            Instance = $db.DAGRmtSvr
                                            Location = "MFG"
                                            Status = "Fetch"
                                        }
                                    }
                                }
                        
                                if ($db.DBIsLocal) { $DBRows += $db}
                            }
                        }
            
                    }
                }
                catch {
                    $fetchSvr.Status = "Error"
                    $fetchSvr | Add-Member -MemberType NoteProperty -Name "ErrorMsg" -Value $_.Exception.Message
                    Write-Error "DB Error: $($fetchSvr)"
                }
                $FetchCompleteCnt += 1
            }
            
        }

        #$DBRows | Sort-Object -Property InDAG, DAGName, AGName, DatabaseName, SourceInst | FT

        Write-DbaDataTable -ServerInstance $RepoInstance -Database $RepoDatabase -InputObject $DBRows `
                            -Table "Status.AGStatusOds_Stage" -AutoCreateTable -FireTriggers -KeepNulls `
                            -SqlCredential $rpCredential

        $sqlCopyToStatus = @"
        USE [FSReporting];
        DECLARE @OdsGroupID INT = NEXT VALUE FOR [Status].[seqAgStatusOds];
        BEGIN TRANSACTION;
        INSERT INTO [FSReporting].[Status].[AGStatusODS] 
            ([OdsInst]
            ,[SourceInst]
            ,[AGName]
            ,[AGID]
            ,[AGReplServer]
            ,[AGFailMode]
            ,[AGAvlMode]
            ,[AGSeeding]
            ,[AGReplRole]
            ,[EndPointServer]
            ,[InODS]
            ,[InDAG]
            ,[DatabaseID]
            ,[DatabaseName]
            ,[IsFailoverReady]
            ,[DBIsLocal]
            ,[DBSyncHealth]
            ,[DBSyncState]
            ,[DBLogSendQueueSize]
            ,[DBLogSendRate]
            ,[DBRedoQueueSize]
            ,[DBRedoRate]
            ,[DBLowWaterMark]
            ,[DBLastHardenedLsn]
            ,[DBLastReceivedLsn]
            ,[DAGName]
            ,[DAGID]
            ,[DAGRmtSvr]
            ,[DAGRmtAG]
            ,[DAGReplRole]
            ,[DAGAvlMode]
            ,[DAGFailMode]
            ,[DAGSeeding]
            ,[DAGSyncHealth]
            ,[ClusterName]
            ,[DateCollected]
            ,OdsGroupID)

SELECT  [OdsInst]
      ,[SourceInst]
      ,[AGName]
      ,[AGID]
      ,[AGReplServer]
      ,[AGFailMode]
      ,[AGAvlMode]
      ,[AGSeeding]
      ,[AGReplRole]
      ,[EndPointServer]
      ,[InODS]
      ,[InDAG]
      ,[DatabaseID]
      ,[DatabaseName]
      ,[IsFailoverReady]
      ,[DBIsLocal]
      ,[DBSyncHealth]
      ,[DBSyncState]
      ,[DBLogSendQueueSize]
      ,[DBLogSendRate]
      ,[DBRedoQueueSize]
      ,[DBRedoRate]
      ,[DBLowWaterMark]
      ,[DBLastHardenedLsn]
      ,[DBLastReceivedLsn]
      ,[DAGName]
      ,[DAGID]
      ,[DAGRmtSvr]
      ,[DAGRmtAG]
      ,[DAGReplRole]
      ,[DAGAvlMode]
      ,[DAGFailMode]
      ,[DAGSeeding]
      ,[DAGSyncHealth]
      ,[ClusterName]
      ,[DateCollected]
      ,@OdsGroupID AS OdsGroupID
  FROM [FSReporting].[Status].[AGStatusODS_Stage]
  WHERE ([OdsInst] = '$($S6OdsInstance)');

  DELETE FROM [FSReporting].[Status].[AGStatusODS_Stage] WHERE ([OdsInst] = '$($S6OdsInstance)');
  COMMIT TRANSACTION;
"@

        Invoke-SqlCmd -query $sqlCopyToStatus -ServerInstance $RepoInstance -SqlCredential $rpCredential
    }    

    End {
        if ($resetHocValue ) {
            $sqlSetAdHoc = "sp_configure 'show advanced options', 0;  RECONFIGURE;"
            $agRmtConf = Invoke-SqlCmd -query $sqlSetAdHoc -ServerInstance $S6OdsInstance 
            $resetAdHoc = $false
            Write-Verbose "Disable AdHoc Distributed Queries  : $($fetchSvr.Instance)"
        }
    }
}


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#   Private function to query a SQL Server about HA/DR settings
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
function Get-HADRInfo {
    Param (
            [string] $OdsLabel,
            [string] $Instance,
            [string] $RemoteInstance, 
            [bool] $Remote = $false,
            [System.Management.Automation.PSCredential] $credential = $null,
            [string] $Username = '',
            [string] $Password
            )

    $sqlRemote =@"
    SELECT  *
        FROM OPENROWSET('SQLNCLI','server=$($RemoteInstance);UID=$($Username);PWD=$($Password)', 'XXX')
"@

    $sqlAgInfo = @"
        SELECT	TOP (1000000)
                CASE LEFT(@@SERVERNAME,3) 
                            WHEN 'PBG' THEN 
                                IIF((RIGHT(@@SERVERNAME,4) LIKE 'T7%'), 'PBG3',   
                                            'PGT' + LEFT(RIGHT(@@SERVERNAME,3),1))
                            WHEN 'KLM' THEN 'KMT' + LEFT(RIGHT(@@SERVERNAME,3),1)
                            WHEN 'DMT' THEN 'DMT' + LEFT(RIGHT(@@SERVERNAME,3),1)
                            ELSE '----' END  AS PlantCode,
                CONVERT(VARCHAR(128), @@SERVERNAME) AS SourceInst,
                AG.[name] AS AGName,
                AG.[group_id] AS AGID,
                AR.replica_server_name AS AGReplServer,
                AR.failover_mode_desc AS AGFailMode,
                AR.availability_mode_desc AS AGAvlMode,
                AR.seeding_mode_desc AS AGSeeding,
                ISNULL(HARS.role_desc, 'UNKNOWN') AS AGReplRole,
                SUBSTRING(LEFT(AR.[endpoint_url], CHARINDEX(':', AR.[endpoint_url], 6) - 1), 7, 128) AS EndPointServer,
                IIF(DAG.[name] IS NULL, 0, 1) AS InDAG,
                ISNULL(HDRS.database_id, 0) AS DatabaseID,
                HDRCS.[database_name] AS DatabaseName,
                HDRCS.is_failover_ready AS IsFailoverReady,
                ISNULL(HDRS.is_local, 0) AS DBIsLocal,
                HDRS.synchronization_health_desc AS DBSyncHealth,
                HDRS.synchronization_state_desc AS DBSyncState,
                ISNULL(HDRS.log_send_queue_size, 0) AS DBLogSendQueueSize,
                ISNULL(HDRS.log_send_rate, 0) AS DBLogSendRate,
                ISNULL(HDRS.redo_queue_size, 0) AS DBRedoQueueSize,
                ISNULL(HDRS.redo_rate, 0) AS DBRedoRate,
                ISNULL(HDRS.low_water_mark_for_ghosts, 0) AS DBLowWaterMark,
                HDRS.last_hardened_lsn AS DBLastHardenedLsn,
                HDRS.last_sent_time,
                HDRS.last_received_time,
                HDRS.last_received_lsn AS DBLastReceivedLsn,
                DAG.[name] AS DAGName,
                DAG.group_id AS DAGID,
                SUBSTRING(LEFT(DARRmt.[endpoint_url], CHARINDEX(':', DARRmt.[endpoint_url], 6) - 1), 7, 128) AS DAGRmtSvr,
                DARRmt.replica_server_name AS DAGRmtAG,
                --CASE WHEN DARPS.role_desc IS NULL THEN 'PRIMARY' ELSE DARPS.role_desc END AS DAGReplRole,
                IIF(DAG.[name] IS NULL, NULL, ISNULL(DARPS.role_desc, 'PRIMARY')) AS DAGReplRole,
                DARRmt.availability_mode_desc AS DAGAvlMode,
                DARRmt.failover_mode_desc AS DAGFailMode,
                DARRmt.seeding_mode_desc AS DAGSeeding,
                DAGState.synchronization_health_desc AS DAGSyncHealth,
                SYSUTCDATETIME() AS DateCollectedUtc,
                ISNULL((SELECT TOP (1) cluster_name FROM sys.dm_hadr_cluster), '') AS ClusterName
                --,DARRmt.*
            FROM sys.availability_groups AG
                INNER JOIN sys.availability_replicas AR
                    ON (AG.[group_id] = AR.[group_id])
                LEFT OUTER JOIN sys.dm_hadr_availability_replica_states HARS
                    ON ( AG.group_id = HARS.group_id )
                        AND ( AR.replica_id = HARS.replica_id )
                LEFT OUTER JOIN (
                        sys.availability_groups DAG
                    INNER JOIN sys.availability_replicas DAR
                        ON (DAG.[group_id] = DAR.[group_id])
                    INNER JOIN sys.availability_replicas DARRmt
                        ON (DAG.[group_id] = DARRmt.[group_id]) AND (DAR.[replica_id] <> DARRmt.[replica_id])
                    INNER JOIN sys.dm_hadr_availability_group_states DAGState
                        ON (DAG.group_id = DAGState.group_id)
                    INNER JOIN sys.dm_hadr_availability_replica_states DARPS
                        ON (DAR.replica_id = DARPS.replica_id)
                        )
                    ON (AG.[name] = DAR.replica_server_name)
                LEFT OUTER JOIN (
                    sys.dm_hadr_database_replica_states HDRS
                    INNER JOIN sys.dm_hadr_database_replica_cluster_states HDRCS
                        ON (HDRS.group_database_id = HDRCS.group_database_id)
                            AND (HDRCS.replica_id = HDRS.replica_id)
                        )
                    ON (HDRS.group_id = AR.group_id) AND (HDRS.replica_id = AR.replica_id)

            WHERE (AG.is_distributed = 0)
            ORDER BY AG.[name], DatabaseName, AR.replica_server_name;
"@

    # if ($Username -ne '') {
    #     $secpasswd = ConvertTo-SecureString $Password -AsPlainText -Force
    #     $credential = New-Object System.Management.Automation.PSCredential ($Username, $secpasswd) 
    # }

    if ($Remote) {
        $sqlQuery = $sqlAgInfo -replace "<InODS>", "0"
        $sqlQuery = $sqlRemote -replace "XXX", ($sqlQuery -replace "'", "''")
    }
    else {
        $sqlQuery = $sqlAgInfo -replace "<InODS>", "1"
    }

    $agInfo = Invoke-SqlCmd -query $sqlQuery -ServerInstance $Instance  # -SqlCredential $rpCredential
    Write-Output $agInfo
}


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

if (-not $FSDeploymentIsLoading){

    #Import-Module -Name dbatools

    $Username = 'zSvc_AG_Status'
    $Password = 'R7ejZ86we4w3RkDFdMLMNwLY'
    $secpasswd = ConvertTo-SecureString $Password -AsPlainText -Force
    $credential = New-Object System.Management.Automation.PSCredential ($Username, $secpasswd) 
    #
    Get-FSDagStatus -S6OdsInstance "PBG1SQL01V105.fs.local" -RepoInstance "EDR1SQL01S003\DBA" -RepoUserName $Username -RepoPassword $Password -RepoDatabase "FSReporting" 

    Get-FSDagStatus -S6OdsInstance "PBG1SQL01L205.fs.local" -RepoInstance "EDR1SQL01S003\DBA" -RepoUserName $Username -RepoPassword $Password -RepoDatabase "FSReporting"

    Get-FSDagStatus -S6OdsInstance "PBG1SQL01V705.fs.local" -RepoInstance "EDR1SQL01S003\DBA" -RepoUserName $Username -RepoPassword $Password -RepoDatabase "FSReporting"

    Get-FSDagStatus -S6OdsInstance "KLM1SQL01V105.fs.local" -RepoInstance "EDR1SQL01S003\DBA" -RepoUserName $Username -RepoPassword $Password -RepoDatabase "FSReporting" 

    Get-FSDagStatus -S6OdsInstance "KLM1SQL01L205.fs.local" -RepoInstance "EDR1SQL01S003\DBA" -RepoUserName $Username -RepoPassword $Password -RepoDatabase "FSReporting" 

    Get-FSDagStatus -S6OdsInstance "DMT1SQL01V105.fs.local" -RepoInstance "EDR1SQL01S003\DBA" -RepoUserName $Username -RepoPassword $Password -RepoDatabase "FSReporting" 

    Get-FSDagStatus -S6OdsInstance "DMT1SQL01V205.fs.local" -RepoInstance "EDR1SQL01S003\DBA" -RepoUserName $Username -RepoPassword $Password -RepoDatabase "FSReporting" 

    }

