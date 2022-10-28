

# =========================================================================
function Get-FSHadrConfig  {
    <#
.SYNOPSIS

Get the current HADR config.

.DESCRIPTION

Get the current configuration details of all AlwaysOn Availability Groups.  Return as a custom PS object or a JSON string

Version History
- 2022-08-26 - 1.0 - F.LaForest - Initial version

.PARAMETER InstanceName
Specifies an array of FQDN instance names to use as the root of the configuration build.

.PARAMETER CrossFW
Flag variable to indicate crossing the firewall

.PARAMETER JsonOut
Plag variable to output a JSON formatted string 

.INPUTS

None. You cannot pipe objects to Checkpoint-FSDeployDirectories

.OUTPUTS

Return JSON string or a custom PS object
.EXAMPLE

PS> Get-FSDeploymentConfig -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(Mandatory=$true)]
        [string] 
        $InstanceName,
    
        [parameter(Mandatory=$false)]
        [switch]
        $CrossFW,
    
        [parameter(Mandatory=$false)]
        [switch]
        $JsonOut
    )

    #   Setup the variables and collections
    #
    begin {

        $sqlServerInfo = "
            SELECT  CASE LEFT(@@SERVERNAME, 3)
                    WHEN 'DMT' THEN
                        'DMT' + LEFT(RIGHT(@@SERVERNAME, 3), 1)
                    WHEN 'KLM' THEN
                        'KMT' + LEFT(RIGHT(@@SERVERNAME, 3), 1)
                    WHEN 'PBG' THEN
                        IIF(( RIGHT(@@SERVERNAME, 4) LIKE 'T7%' ), 'PBG3', 'PGT' + LEFT(RIGHT(@@SERVERNAME, 3), 1))
                    WHEN 'PKM' THEN
                        'PMT' + LEFT(RIGHT(@@SERVERNAME, 3), 1)
                    ELSE
                        '----'
                END AS PlantCode,
                UPPER(CONVERT (VARCHAR(128), @@SERVERNAME)) AS CurrentServer,
                CASE DEFAULT_DOMAIN() 
                    WHEN 'FS'  THEN 'fs.local'
                    WHEN 'MFG' THEN 'mfg.fs'
                    WHEN 'DEV' THEN 'dev.fs'
                    WHEN 'NPQ' THEN 'npq.mfg'
                    WHEN 'QA'  THEN 'qa.fs'
                    ELSE DEFAULT_DOMAIN() END AS  CurrentDomain,
                ISNULL (( SELECT TOP ( 1 ) UPPER (cluster_name) FROM sys.dm_hadr_cluster ), '') AS ClusterName,
                SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS NetBIOSName,
                SERVERPROPERTY('MachineName') AS MachineName,
                SERVERPROPERTY('IsClustered') AS IsClustered,
                ISNULL(SERVERPROPERTY('IsHadrEnabled'), 'FALSE') AS [HadrEnabled]
            ";

        $sqlGetAGConf = "
        SELECT TOP ( 1000000 )
                CASE LEFT(@@SERVERNAME, 3)
                    WHEN 'DMT' THEN
                        'DMT' + LEFT(RIGHT(@@SERVERNAME, 3), 1)
                    WHEN 'KLM' THEN
                        'KMT' + LEFT(RIGHT(@@SERVERNAME, 3), 1)
                    WHEN 'PBG' THEN
                        IIF(( RIGHT(@@SERVERNAME, 4) LIKE 'T7%' ), 'PBG3', 'PGT' + LEFT(RIGHT(@@SERVERNAME, 3), 1))
                    WHEN 'PKM' THEN
                        'PMT' + LEFT(RIGHT(@@SERVERNAME, 3), 1)
                    ELSE
                        '----'
                END AS PlantCode,
                UPPER(CONVERT (VARCHAR(128), @@SERVERNAME)) AS SourceInst,
				ISNULL(HARS.is_local,0) AS IsLocal,
                AG.[name] AS AGName,
                AG.[group_id] AS AGID,
                ISNULL (( SELECT TOP ( 1 ) UPPER (cluster_name) FROM sys.dm_hadr_cluster ), '') AS ClusterName,
                UPPER(AR.replica_server_name) AS AGReplServer,
                AR.failover_mode_desc AS AGFailMode,
                AR.availability_mode_desc AS AGAvlMode,
                AR.seeding_mode_desc AS AGSeeding,
                AG.failure_condition_level AS AGFailCondLevel,
                AG.health_check_timeout AS AGHealthTimeout,
                UPPER(AG.automated_backup_preference_desc) AS AGBackupPref,
                AR.backup_priority,
                AG.dtc_support AS AGDtcEnabled,
                AG.db_failover AS AGHealthFailover,
                AGL.dns_name AS AGListenerName,
                AGLIP.ip_address AS AGLIPAddr,
                AGLIP.ip_subnet_mask AS AGLIPSubnet,
                AGLIP.state_desc AS AGLState,
                ISNULL (HARS.role_desc, 'UNKNOWN') AS AGReplRole,
                UPPER(SUBSTRING (LEFT(AR.[endpoint_url], CHARINDEX (':', AR.[endpoint_url], 6) - 1), 7, 128)) AS EndPointServer,
                IIF(DAG.[name] IS NULL, 0, 1) AS InDAG,
                '{`"DB`":[' + STUFF (
                                        (
                                            SELECT ',`"' + HDRCS.[database_name] + '`"'
                                                FROM sys.dm_hadr_database_replica_states HDRS
                                                    INNER JOIN sys.dm_hadr_database_replica_cluster_states HDRCS
                                                    ON ( HDRS.group_database_id = HDRCS.group_database_id )
                                                        AND ( HDRCS.replica_id = HDRS.replica_id )
                                                WHERE
                                                ( HDRS.group_id = AR.group_id )
                                                AND ( HDRS.replica_id = AR.replica_id )
                                                ORDER BY HDRCS.[database_name]
                                            FOR XML PATH (''), TYPE
                                        ).value ('.', 'varchar(128)'), 1, 1, '') + ']}' AS DbList,
                DAG.[name] AS DAGName,
                DAG.group_id AS DAGID,
                SUBSTRING (LEFT(DARRmt.[endpoint_url], CHARINDEX (':', DARRmt.[endpoint_url], 6) - 1), 7, 128) AS DAGRmtSvr,
                DARRmt.replica_server_name AS DAGRmtAG,
                IIF(DAG.[name] IS NULL, NULL, ISNULL (DARPS.role_desc, 'PRIMARY')) AS DAGReplRole,
                DARRmt.availability_mode_desc AS DAGAvlMode,
                DARRmt.failover_mode_desc AS DAGFailMode,
                DARRmt.seeding_mode_desc AS DAGSeeding,
                DAGState.synchronization_health_desc AS DAGSyncHealth,
                SYSUTCDATETIME () AS DateCollectedUtc
            --,DARRmt.*
            FROM sys.availability_groups AG
                INNER JOIN sys.availability_replicas AR
                    ON ( AG.[group_id] = AR.[group_id] )
                LEFT OUTER JOIN sys.dm_hadr_availability_replica_states HARS
                    ON ( AG.group_id = HARS.group_id )
                        AND ( AR.replica_id = HARS.replica_id )
                LEFT OUTER JOIN sys.availability_group_listeners AGL
                    ON ( AG.group_id = AGL.group_id )
                LEFT OUTER JOIN sys.availability_group_listener_ip_addresses AGLIP
                    ON ( AGL.listener_id = AGLIP.listener_id )
                LEFT OUTER JOIN(sys.availability_groups DAG
                        INNER JOIN sys.availability_replicas DAR
                            ON ( DAG.[group_id] = DAR.[group_id] )
                        INNER JOIN sys.availability_replicas DARRmt
                            ON ( DAG.[group_id] = DARRmt.[group_id] )
                                AND ( DAR.[replica_id] <> DARRmt.[replica_id] )
                        LEFT OUTER JOIN sys.dm_hadr_availability_group_states DAGState
                            ON ( DAG.group_id = DAGState.group_id )
                        INNER JOIN sys.dm_hadr_availability_replica_states DARPS
                            ON ( DAR.replica_id = DARPS.replica_id ))
                    ON ( AG.[name] = DAR.replica_server_name )
            WHERE
                ( AG.is_distributed = 0 )
				AND (ISNULL(HARS.is_local,0) = 1)

            ORDER BY
                AG.[name],
                AR.replica_server_name;
        ";


        #   Get the endpoint configuration
        #
        $sqlEndpoint = "
        WITH MP AS
        (
            SELECT ISNULL (DME.[name], 'NONE') AS [MPortName],
                    DME.endpoint_id AS MEndpointID,
                    TE.[port] AS [MPortNo],
                    DME.[state_desc] AS [MPortState],
                    REPLACE (DME.connection_auth_desc, ' ', '') AS [MPortConnAuth], -- 7 = Negotiate, Certificate or 10 = Certificate, Negotiate
                    DME.[role_desc] AS [MPortRole],                                 -- 3=ALL
                    DME.is_encryption_enabled AS [MPortEncState],                   -- 1 = enabled
                    REPLACE (DME.encryption_algorithm_desc, ' ', '') AS [MPortEnc],
                    DME.certificate_id,
                    --CT.[name] AS CertName,
                    SPR.[name] AS EndpointPrincipalName,
                    DME.certificate_id AS EndpointPrincipalID,
                    CERT.principal_id AS CertPrincipalId,
                    CERT.[name] AS CertName,
                    DPRC.[name] AS CertOwner,
                    CERT.cert_serial_number
                FROM sys.database_mirroring_endpoints DME
                    INNER JOIN sys.tcp_endpoints TE
                        ON ( DME.endpoint_id = TE.endpoint_id )
                            AND ( DME.protocol_desc = 'TCP' )
                    LEFT OUTER JOIN sys.certificates CT
                        ON ( DME.certificate_id = CT.certificate_id )
                    LEFT OUTER JOIN(sys.certificates CERT
                    INNER JOIN master.sys.database_principals DPRC
                        ON ( CERT.principal_id = DPRC.principal_id ))
                        ON ( DME.certificate_id = CERT.certificate_id )
                    LEFT OUTER JOIN sys.server_principals SPR
                        ON ( DME.principal_id = SPR.principal_id )
                WHERE
                ( DME.type_desc = 'DATABASE_MIRRORING' )
        )
        SELECT SERVERPROPERTY ('MachineName') AS Name,
                SERVERPROPERTY ('productversion') AS SqlVersion,
                ISNULL (MP.MEndpointID, 0) AS MEndpointID,
                ISNULL (MP.MPortName, 'HADR_EndPoint') AS MPortName,
                ISNULL (MP.MPortNo, 5022) AS MPortNo,
                ISNULL (MP.MPortState, 'STARTED') AS MPortState,
                ISNULL (MP.MPortConnAuth, 'CERTIFICATE,NEGOTIATE') AS MPortConnAuth,
                ISNULL (MP.MPortRole, 'ALL') AS MPortRole,
                ISNULL (MP.MPortEncState, 1) AS MPortEncState,
                ISNULL (MP.MPortEnc, 'AES') AS MPortEnc,
                ISNULL (MP.certificate_id, 0) AS MPortCertNo,
                MP.EndpointPrincipalName,
                MP.EndpointPrincipalID,
                MP.CertPrincipalId,
                MP.CertName,
                MP.CertOwner,
                MP.cert_serial_number
            FROM ( SELECT 1 AS ONE ) J1
                LEFT OUTER JOIN MP
                    ON ( 1 = 1 );
        ";


        #   Lists and Collections
        #


        #   Read the HA config and place it into a custom PS object for earier use
        #
        $srvInfo = Invoke-Sqlcmd -ServerInstance $InstanceName -Database 'master' -query $sqlServerInfo
        $dsInfo = Invoke-Sqlcmd -ServerInstance $InstanceName -Database 'master' -query $sqlGetAGConf
        $epInfo = Invoke-Sqlcmd -ServerInstance $InstanceName -Database 'master' -query $sqlEndpoint

        $haConf = [PSCustomObject]@{
            Plant       = $srvInfo.PlantCode
            AGList        = @{}    # List of AG
            ClusterName   = $srvInfo.ClusterName
            CurrentServer = $srvInfo.CurrentServer
            CurrentDomain = $srvInfo.CurrentDomain
            ServerFQDN    = "$($srvInfo.CurrentServer).$($srvInfo.CurrentDomain)"
            HadrEnabled   = $srvInfo.HadrEnabled
            EndPoint    = [PSCustomObject]@{
                EndpointName  = $epInfo.MPortName
                EndpointID    = $epInfo.MEndpointID
                TCPPort       = $epInfo.MPortNo
                PortState     = $epInfo.MPortState
                AuthConn      = $epInfo.MPortConnAuth
                Role          = $epInfo.MPortRole
                EncryptState  = $epInfo.MPortEncState
                EncryptMethod = $epInfo.MPortEnc
                CertNo        = $epInfo.MPortCertNo
                CertName      = $epInfo.CertName
                CertOwner     = $epInfo.CertOwner
                CertSerialNo  = $epInfo.cert_serial_number
            }
        }

        #   Now collect info on each AG
        foreach ($ag in $dsInfo) {
            $agName = $ag.AGName
            $GenericAGName = ((((($agName -replace 'AG_','') -replace 'S6_','' ) -replace '_ODS','') -replace $haConf.Plant,'') -replace '_','')
            if ($GenericAGName -eq '') { $GenericAGName = 'Local'}
            
            if (-Not $haConf.AGList[$GenericAGName]) { 
                $haConf.AGList[$GenericAGName] = [PSCustomObject]@{
                    AGName = $agName
                    Replicas = @()
                    Listener = $null
                    DAG = $null
                    GenericAGName = $GenericAGName
                }
            }
            # Add each replica and associated details
            $haConf.AGList[$GenericAGName].Replicas += [PSCustomObject]@{
                AGReplServer = $ag.AGReplServer
                AGReplRole   = $ag.AGReplRole
                AGName       = $ag.AGName
                IsLocal      = $ag.IsLocal
                AGFailMode   = $ag.AGFailMode
                AGAvlMode    = $ag.AGAvlMode
                AGSeeding    = $ag.AGSeeding
                AGFailLevel  = $ag.AGFailCondLevel
                AGBackupPref = $ag.AGBackupPref
                EndpointServer = $ag.EndPointServer
                AGDtcEnabled   = $ag.AGDtcEnabled
                AGHealthFailover = $ag.AGHealthFailover
                AGHealthTimeout  = $ag.AGHealthTimeout
                DB    = ($ag.DbList | ConvertFrom-Json).DB
            } 
            #  Collect info on any listener
            if ((-not [string]::IsNullOrEmpty($ag.AGListenerName)) -and ($null -eq $haConf.AGList[$GenericAGName].Listener)) {
                $haConf.AGList[$GenericAGName].Listener = [PSCustomObject]@{
                    Name   = $ag.AGListenerName
                    IPAddr = $ag.AGLIPAddr
                    Subnet = $ag.AGLIPSubnet
                }
            }
            #   Collect info on any associated distributed AG
            if ((-not ($ag.DAGName.GetType().Name -like 'DBNull')) -and ($null -eq $haConf.AGList[$GenericAGName].DAG)) {
                $haConf.AGList[$GenericAGName].DAG = [PSCustomObject]@{
                    Name     = $ag.DAGName
                    RmtSvr   = $ag.DAGRmtSvr
                    RmtAG    = $ag.DAGRmtAG
                    ReplRole = $ag.DAGReplRole
                    AvlMode  = $ag.DAGAvlMode
                    FailMode = $ag.DAGFailMode
                    Seeding  = $ag.DAGSeeding
                }
            }
        }

        if ($JsonOut) {
            Write-Output (Convertto-Json $haConf -depth 5)
        }
        else {
            Write-Output $haConf
        }

    }

    #   Start querying the list of instances 
}

# Get-ClusterResource -Cluster 'PBG1SQL01C204' | Where ResourceType -eq 'SQL Server Availability Group' | Format-List -Property *

if (-not $FSDeploymentIsLoading){

   # Get-FSHadrConfig -InstanceName 'PBG1SQL01T214.fs.local' -JsonOut
    Get-FSHadrConfig -InstanceName 'PBG1SQL01V001.fs.local' -JsonOut
}