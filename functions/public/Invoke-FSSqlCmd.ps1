
function Invoke-FSSqlCmd {

    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $Instance,

        [Parameter(Mandatory=$false)]
        [string]
        $Database = 'master',

        [Parameter(Mandatory=$false)]
        [switch]
        $CaptureMsgs,

        [Parameter(Mandatory=$false)]
        [switch]
        $GetSchema,

        [Parameter(Mandatory=$true)]
        [string]
        $Query
    )

    Begin {
        try {
            $Results = [PSCustomObject]@{
                InstanceName = $Instance
                Database = $Database
                Rows = 0
                Schema = @()
                Data = $null
                OOBMsgs = $null
                ExMsg = ''
                ExStackTrace = $null
            }
            $sqlConn = New-Object System.Data.SqlClient.SqlConnection
            $sqlConn.ConnectionString = "Server=$($Instance);Database=$($Database);Integrated Security=True;"
            $sqlConn.Open();

            #   Setup OOB message capture if selected

            if ($CaptureMsgs) {
                $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {param($sender, $event) 
                    if ($null -eq $Results.OOBMsgs) { $Results.OOBMsgs = @(); }
                    $Results.OOBMsgs += $event.Message
                };
                $sqlConn.add_InfoMessage($handler);
                $sqlConn.FireInfoMessageEventOnUserErrors = $true; 
            }

            $sqlCmd = $sqlConn.CreateCommand()
            $sqlCmd.CommandText = $Query
            $sqlRdr = $sqlCmd.ExecuteReader()

            #   Should we capture the returned schema information

            $qrySchema = $null
            if ($GetSchema) {
                $qrySchema = New-Object System.Data.Datatable
                $qrySchema = $sqlRdr.GetSchemaTable()
            }

            #   Read the returned data rows if present

            $qryData = New-Object System.Data.Datatable
            $LoadResults = $qryData.Load($sqlRdr);
            $Results.Data = $qryData

            $Results.Rows = $Results.Data.Rows.Count

            $sqlConn.Close();
            $sqlCmd.Dispose();
            $sqlConn.Dispose();

            if ($GetSchema) {
                $qrySchema.Rows | Foreach-Object -Process { 

                    $CInfo = [PSCustomObject]@{
                        ColumnName = $_.ColumnName
                        ColumnOrdinal = $_.ColumnOrdinal
                        ColumnSize = $_.ColumnSize
                        Precision = $_.NumericPrecision
                        Scale = $_.NumericScale
                        ProvDataType = "System."+$_.ProviderSpecificDataType.Name -replace 'Sql',''
                        DataType = $_.DataTypeName
                        SqlDef =  $_.DataTypeName
                    }
                    switch ($CInfo.DataType) {
                        'decimal' {$CInfo.SqlDef = "decimal($($CInfo.Precision),$($CInfo.Scale))"}
                        'numeric' {$CInfo.SqlDef = "numeric($($CInfo.Precision),$($CInfo.Scale))"}
                        'float' {$CInfo.SqlDef = "float($($CInfo.Precision))"}
                        'real' {$CInfo.SqlDef = "real"}
                        'varchar' { if ($CInfo.ColumnSize -le 8000) {
                                $CInfo.SqlDef = "varchar($($CInfo.ColumnSize))" 
                            } else {
                                $CInfo.SqlDef = "varchar(MAX)" 
                            }
                        }
                        'char' { $CInfo.SqlDef = "char($($CInfo.ColumnSize))" }
                        'nchar' { $CInfo.SqlDef = "char($($CInfo.ColumnSize))" }
                        'nvarchar' { if ($CInfo.ColumnSize -le 4000) {
                                $CInfo.SqlDef = "varchar($($CInfo.ColumnSize))" 
                            } else {
                                $CInfo.SqlDef = "varchar(MAX)" 
                            }
                        }
                        'varbinary' { if ($CInfo.ColumnSize -le 8000) {
                                $CInfo.SqlDef = "varbinary($($CInfo.ColumnSize))" 
                            } else {
                                $CInfo.SqlDef = "varbinary(MAX)" 
                            }
                        }
                        'binary' {$CInfo.SqlDef = "binary($($CInfo.ColumnSize))"}
                        'datetime2' {$CInfo.SqlDef = "datetime2($($CInfo.Scale))"}
                        'datetimeoffset' {$CInfo.SqlDef = "datetimeoffset($($CInfo.Scale))"}
                        'rowversion' {$CInfo.SqlDef = "bigint" }
                        'timestamp' {$CInfo.SqlDef = "bigint" }
                        }
                    $Results.Schema += $CInfo
                }
            }
        }

        Catch {
            $Results.ExMsg = $_.Exception.Message -replace "'","''"
            $Results.ExStackTrace = $_.ScriptStackTrace -replace "'","''"
        }
        
        finally {
            if ($sqlCmd) {
                $sqlCmd.Dispose();
            }
            if ($sqlConn) {
                $sqlConn.Close();
                $sqlConn.Dispose();
            }
        }
        Write-Output $Results
    }
}


if (-not $FSDeploymentIsLoading) {
    $BugQry = @"
    SELECT	@@SERVERNAME AS ServerName,
    DB_NAME() AS database_name,
    DPM.[state] AS PermState,
    DPM.[state_desc] AS PermStateDesc,
    DPM.[type] AS PermType,
    DPM.[permission_name] AS PermName,
    DPM.grantee_principal_id,
    DP.[type_desc] AS grantee_principal_type,
    DP.[name] AS grantee_principal_name,
    DPM.grantor_principal_id,
    GDP.[name] AS grantor_principal_name,
    DPM.[class],
    DPM.[class_desc],
    DPM.[major_id],
    DPM.minor_id,
    CASE WHEN DPM.class = 1 THEN (SELECT [type_desc] FROM sys.objects WHERE [object_id] = DPM.major_id)
        END AS ObjType,
    CASE DPM.[class]
        WHEN 0 THEN NULL -- Database
        WHEN 1 THEN CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(DPM.major_id)),'.',QUOTENAME(OBJECT_NAME(DPM.major_id)))
        WHEN 3 THEN SCHEMA_NAME(DPM.major_id)
        WHEN 4 THEN DP.[name]
        WHEN 5 THEN (SELECT [name] FROM sys.assemblies ASM WHERE ASM.assembly_id = DPM.major_id )
        WHEN 6 THEN (SELECT [name] FROM sys.types TYP WHERE TYP.system_type_id = DPM.major_id)
        WHEN 10 THEN (SELECT [name] FROM sys.xml_schema_collections XC WHERE XC.xml_collection_id = DPM.major_id )
        WHEN 15 THEN (SELECT [name] FROM sys.service_message_types	WHERE message_type_id = DPM.major_id )
        WHEN 16 THEN (SELECT [name] FROM sys.service_contracts SC	WHERE SC.service_contract_id = DPM.major_id )
        WHEN 17 THEN (SELECT [name] FROM sys.services				WHERE service_id = DPM.major_id )
        WHEN 18 THEN (SELECT [name] FROM sys.remote_service_bindings WHERE remote_service_binding_id = DPM.major_id )
        WHEN 19 THEN (SELECT [name] FROM sys.routes WHERE route_id = DPM.major_id )
        WHEN 23 THEN (SELECT [name] FROM sys.fulltext_catalogs WHERE fulltext_catalog_id = DPM.major_id )
        WHEN 24 THEN (SELECT [name] FROM sys.symmetric_keys WHERE symmetric_key_id = DPM.major_id )
        WHEN 25 THEN (SELECT [name] FROM sys.certificates WHERE certificate_id = DPM.major_id )
        WHEN 26 THEN (SELECT [name] FROM sys.asymmetric_keys WHERE asymmetric_key_id = DPM.major_id )
        WHEN 29 THEN (SELECT [name] FROM sys.fulltext_stoplists WHERE stoplist_id = DPM.major_id )
        WHEN 31 THEN (SELECT [name] FROM sys.registered_search_property_lists WHERE property_list_id = DPM.major_id )
        ----WHEN 32 THEN (SELECT [name] FROM sys.database_scoped_credentials WHERE = DPM.major_id )
        --WHEN 34 THEN (SELECT [language] FROM sys.external_languages WHERE external_language_id = DPM.major_id )
        ELSE 'UNKNOWN'
    END COLLATE SQL_Latin1_General_CP1_CI_AS AS PermObject

FROM sys.database_permissions DPM
    LEFT OUTER JOIN sys.database_principals DP
        ON (DPM.grantee_principal_id = DP.principal_id)
    LEFT OUTER JOIN sys.database_principals GDP
        ON (DPM.grantor_principal_id = GDP.principal_id)
    WHERE ((DPM.[class] =1) AND (DPM.major_id >= 0)) OR (DPM.[class] <> 1)

"@

#$BugQry
#$QryResults = Invoke-FSSqlCmd -Instance 'PBG1SQL01T114.fs.local' -Database 'ODS' -GetSchema -CaptureMsgs -Query $BugQry
$a = 3
}

