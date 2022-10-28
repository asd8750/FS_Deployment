function Get-FSSqlPartitionInfo  {
    <#
.SYNOPSIS

Returns information about the partition schemes and functions within a database

.DESCRIPTION

Returns a data structure containing a list of partition functions.  Schemes using the functions,
And tables ties to the schemes.

.PARAMETER InstanceName
Specifies the Fully qualified SQL Server instance name of the global master job server 

.PARAMETER Database
Specifies the database name of the deployment control database.  
Default is 'FSDeployDB'

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
        $InstanceName,

        [Parameter(Mandatory=$true)]
        [string]
        $Database
    )

    #   1) Connect to the FSDeployDB database and get the list of registered deployment directories
    #
    Begin {

        #   Prepare the return object 
        #
        $Tables = @{}
        $PtFunc = @{};
        $PtSchemes = @{};
        $PtInfo = New-Object PSCustomObject -Property @{
            DatabaseName = $Database
            DbConfig = @{}
            Functions = $PtFunc
            Schemes = $PtSchemes
            Tables = $Tables
        }

        #
        #  Define the complex T-SQL statement needed to assemble all partitioning information for this database.
        #
        $sqlGetPt2 = @"
        SET ANSI_WARNINGS OFF;
		IF (OBJECT_ID('tempdb..#RCnt') IS NOT NULL) DROP TABLE #RCnt;

		SELECT  IX2.[object_id],	
				IX2.[index_id],
				PT2.partition_number,
				IX2.data_space_id,
				PS2.function_id,
				PT2.[rows],
				SUM(PT2.[rows]) OVER (PARTITION BY PS2.function_id, PT2.partition_number ) AS FRows
		INTO #Rcnt
		FROM sys.partitions PT2
			INNER JOIN sys.indexes IX2
				ON (PT2.[object_id] = IX2.[object_id]) AND (PT2.[index_id] = IX2.[index_id])
			INNER JOIN sys.partition_schemes PS2
				ON (IX2.data_space_id = PS2.data_space_id);

		CREATE UNIQUE CLUSTERED INDEX [IDX_#Rcnt] ON [#RCnt]([object_id], [index_id], [partition_number]);

        ;WITH PtFuncs
        AS (SELECT TOP (2000000) PTF.[name] AS PtFunc, 
                                 PTF.function_id, 
                                 PTF.boundary_value_on_right AS [RR], 
                                 PTF.fanout,
                                 CASE
                                     WHEN TYP.[name] = 'datetime2'
                                     THEN TYP.[name] + '(' + CONVERT(NVARCHAR(3), PTP.scale) + ')'
                                     WHEN(TYP.[name] LIKE 'var%')
                                         OR (TYP.[name] LIKE 'nvar%')
                                     THEN TYP.[name] + '(' + CASE
                                                                 WHEN (PTP.max_length = -1)
                                                                 THEN 'MAX'
                                                                 ELSE CONVERT(NVARCHAR(4), PTP.max_length)
                                                             END + ')'
                                     WHEN(TYP.[name] IN('char', 'nchar', 'binary', 'time'))
                                     THEN TYP.[name] + '(' + CONVERT(NVARCHAR(4), PTP.max_length) + ')'
                                     WHEN(TYP.[name] IN('decimal', 'numeric'))
                                     THEN TYP.[name] + '(' + CONVERT(NVARCHAR(4), PTP.[precision]) + ',' + CONVERT(NVARCHAR(4), PTP.[scale]) + ')'
                                     WHEN(TYP.[name] IN('float'))
                                     THEN TYP.[name] + CASE
                                                           WHEN PTP.[precision] < 53
                                                           THEN '(' + CONVERT(NVARCHAR(4), PTP.[precision]) + ')'
                                                           ELSE ''
                                                       END
                                     WHEN(TYP.[name] IN('datetimeoffset'))
                                     THEN TYP.[name] + '(' + CONVERT(NVARCHAR(4), PTP.[scale]) + ')'
                                     ELSE TYP.[name]
                                 END AS Datatype, 
                                 '{"PTB":[' + STUFF(
                (
                    SELECT ', "' + CASE
                                       WHEN PTRV.[value] IS NULL
                                       THEN NULL
                                       WHEN SQL_VARIANT_PROPERTY(PTRV.[value], 'BaseType') = 'date'
                                       THEN CONVERT(VARCHAR, PTRV.[value], 102)
                                       WHEN SQL_VARIANT_PROPERTY(PTRV.[value], 'BaseType') = 'datetime'
                                       THEN CONVERT(VARCHAR, PTRV.[value], 120)
                                       WHEN SQL_VARIANT_PROPERTY(PTRV.[value], 'BaseType') = 'datetime2'
                                       THEN CONVERT(VARCHAR, PTRV.[value], 120)
                                       WHEN SQL_VARIANT_PROPERTY(PTRV.[value], 'BaseType') = 'datetimeoffset'
                                       THEN CONVERT(VARCHAR, PTRV.[value], 127)
                                       ELSE CONVERT(VARCHAR, PTRV.[value])
                                   END + '"'
                        FROM sys.partition_range_values PTRV
                        WHERE (PTF.function_id = PTRV.function_id)
                        ORDER BY PTRV.boundary_id FOR XML PATH('')
                ), 1, 2, '') + ']}' AS PTB
                FROM sys.partition_functions PTF
                     INNER JOIN sys.partition_parameters PTP
                         ON (PTF.function_id = PTP.function_id) 
                     INNER JOIN sys.types TYP
                         ON (PTP.user_type_id = TYP.user_type_id) ),
        PtSchemes
        AS (SELECT TOP (2000000) PTS.[name] AS PtScheme, 
                                 PTS.data_space_id, 
                                 PTS.function_id, 
                                 '{"FGList":[' + STUFF(
                (
                    SELECT ', "' + DSP.[name] + '"'
                        FROM sys.destination_data_spaces DDSP
                             INNER JOIN sys.data_spaces DSP
                                 ON (DDSP.data_space_id = DSP.data_space_id)
                        WHERE (DDSP.partition_scheme_id = PTS.data_space_id)
                        ORDER BY DDSP.destination_id FOR XML PATH('')
                ), 1, 2, '') + ']}' AS FGList
                FROM sys.partition_schemes PTS
				),

        PTB
        AS (SELECT TOP (2000000) OBJECT_SCHEMA_NAME(TBL.[object_id]) AS TableSchema, 
                                 OBJECT_NAME(TBL.[object_id]) AS TableName, 
                                 TBL.[object_id],
                                 COUNT(DISTINCT IDX.data_space_id) AS PtSchs
                FROM sys.tables TBL
                     INNER JOIN sys.indexes IDX
                         ON (TBL.[object_id] = IDX.[object_id]) 
                     LEFT OUTER JOIN sys.partition_schemes PS
                         ON (IDX.data_space_id = PS.data_space_id)
                GROUP BY TBL.[object_id]
                HAVING (COUNT(PS.data_space_id) > 0) ),
        PtTables
        AS (SELECT TOP (2000000) TBL.TableSchema, 
                                 TBL.TableName, 
                                 ISNULL(IDX.[name], '[Heap]') AS IndexName, 
                                 PTS.data_space_id, 
                                 IIF(TBL.PtSchs = 1, 1, 0) AS isAligned, 
                                 IDX.index_id AS IndexID, 
                                 IDX.[type] AS IndexType, 
                                 TBL.[object_id] AS TableObjID,
                                 CASE
                                     WHEN(IDXC.partition_ordinal IS NOT NULL)
                                         AND (IDXC.partition_ordinal = 1)
                                     THEN COL.[name]
                                     ELSE NULL
                                 END AS PtColumn
                FROM PTB TBL
                     INNER JOIN(sys.indexes IDX
                     INNER JOIN sys.partition_schemes PTS
                         ON (IDX.[data_space_id] = PTS.data_space_id) 
                     LEFT OUTER JOIN(sys.index_columns IDXC
                     INNER JOIN sys.columns COL
                         ON (IDXC.[object_id] = COL.[object_id])
                            AND (IDXC.column_id = COL.column_id) )
                         ON (IDXC.[object_id] = IDX.[object_id])
                            AND (IDXC.index_id = IDX.index_id)
                            AND (IDXC.partition_ordinal = 1) )
                         ON (TBL.[object_id] = IDX.[object_id]) ),
        XP
        AS (SELECT TOP (2000000)
                   EX.class_desc, 
                   EX.major_id, 
                   EX.value
                FROM sys.extended_properties EX
                WHERE (EX.[name] = 'FSPtManager') )
        SELECT PF.PtFunc, 
               PF.function_id, 
               PF.RR, 
               PF.fanout, 
               PF.Datatype, 
               PF.PTB,
			   ISNULL(XDB.[value],'') AS DbConfig,
               CASE
                   WHEN XPF.major_id IS NULL
                   THEN ''
                   WHEN XPF.major_id = PF.function_id
                   THEN XPF.[value]
                   ELSE ''
               END AS PFConfig, 
               PS.PtScheme, 
               PS.data_space_id, 
               PS.FGList,
               CASE
                   WHEN XPS.major_id IS NULL
                   THEN ''
                   WHEN XPS.major_id = PS.data_space_id
                   THEN XPS.[value]
                   ELSE ''
               END AS PSConfig, 
               PT.TableSchema, 
               PT.TableName, 
               PT.IndexName, 
               PT.PtColumn, 
               PT.IndexID, 
               PT.IndexType, 
               PT.TableObjID, 
               PT.isAligned,
               CASE
                   WHEN XPT.major_id IS NULL
                   THEN ''
                   WHEN XPT.major_id = PT.TableObjID
                   THEN XPT.[value]
                   ELSE ''
               END AS TabConfig, 
               '{"Rows":[' + STUFF(
            (
                SELECT ', "' + CONVERT(VARCHAR(15), PT2.[rows]) + '"'
                    --FROM sys.partitions PT2
					FROM #RCnt PT2
                    WHERE (PT2.[object_id] = PT.TableObjID)
                          AND (PT2.index_id = PT.IndexID)
                    ORDER BY PT2.partition_number FOR XML PATH('')
            ), 1, 2, '') + ']}' AS [Rows], 
               '{"PFRows":[' + STUFF(
            (
                SELECT ', "' + CONVERT(VARCHAR(15), PT2.[FRows]) + '"'
                    --FROM sys.partitions PT2
					FROM #RCnt PT2
                    WHERE (PT2.[object_id] = PT.TableObjID)
                          AND (PT2.index_id = PT.IndexID)
                    ORDER BY PT2.partition_number FOR XML PATH('')
            ), 1, 2, '') + ']}' AS [PFRows]
            FROM PtFuncs PF
                 LEFT OUTER JOIN PtSchemes PS
                     ON (PF.function_id = PS.function_id) 
                 LEFT OUTER JOIN PtTables PT
                     ON (PS.data_space_id = PT.data_space_id) 
                 LEFT OUTER JOIN XP AS XDB
                     ON (XDB.major_id = 0)
                        AND (XDB.class_desc = 'DATABASE') 
                 LEFT OUTER JOIN XP AS XPF
                     ON (XPF.major_id = PF.function_id)
                        AND (XPF.class_desc = 'PARTITION_FUNCTION') 
                 LEFT OUTER JOIN XP AS XPS
                     ON (XPS.major_id = PS.data_space_id)
                        AND (XPS.class_desc = 'DATASPACE') 
                 LEFT OUTER JOIN XP AS XPT
                     ON (XPT.major_id = PT.TableObjID)
                        AND (XPT.class_desc = 'OBJECT_OR_COLUMN');

		IF (OBJECT_ID('tempdb..#RCnt') IS NOT NULL) DROP TABLE #RCnt;

"@;
    }

    #
    #   Request the partitioning information from specified database
    #
    Process {
        # $rsGetPtInfo = Invoke-Sqlcmd  -query $sqlGetPt2 -ServerInstance $InstanceName -Database $Database
        $rsGetPtInfo = Invoke-Sqlcmd -ServerInstance $InstanceName -Database $Database -MaxCharLength 1000000 -query $sqlGetPt2 

        foreach ($rowPtInfo in $rsGetPtInfo) {
            if (($rowPtInfo.DbConfig.Length -gt 0) -and ($PtInfo.DbConfig.Count -eq 0)) {
                $dbCfg = ConvertFrom-Json $rowPtInfo.DbConfig
                $PtInfo.DbConfig = $dbCfg.Cfg
            }
            if ($null -eq $PtFunc[$rowPtInfo.PtFunc]) {
                # if ($rowPtInfo.PtFunc -ieq "PtFunc_Vision_V1") {
                #     $tempa = "s"
                # }
                $ptb = ConvertFrom-Json $rowPtInfo.PTB
                $PfRows = ConvertFrom-Json $rowPtInfo.PFRows
                $ptfCfg = ConvertFrom-Json $rowPtInfo.PFConfig
                $PtFunc[$rowPtInfo.PtFunc] = New-Object PSObject -Property @{
                    FuncName = $rowPtInfo.PtFunc
                    Config   = $ptfCfg.Cfg
                    DataType = $rowPtInfo.Datatype
                    RR       = $rowPtInfo.RR
                    Fanout   = $rowPtInfo.fanout
                    BndList  = $ptb.PTB
                    PFRows   = $PfRows.PfRows
                    Schemes  = @{}
                }
            }
            $curPtFunc = $PtFunc[$rowPtInfo.PtFunc];

            if ($rowPtInfo.PtScheme -ne $null -and $rowPtInfo.PtScheme.GetType().Name -ine "DBNull") {
                if ($null -eq $curPtFunc.Schemes[$rowPtInfo.PtScheme]) {
                    $fgList = ConvertFrom-Json $rowPtInfo.FGList
                    $ptsCfg = ConvertFrom-Json $rowPtInfo.PSConfig
                    $newPtFunc = New-Object PSObject -Property @{
                        SchemeName = $rowPtInfo.PtScheme
                        Config     = $ptsCfg.Cfg.Cfg
                        FGList     = $fgList.FGList
                        Indexes    = @{}
                    }
                    $curPtFunc.Schemes[$rowPtInfo.PtScheme] = $newPtFunc
                    $PtSchemes[$rowPtInfo.PtScheme] = $newPtFunc
                }
                $curPtScheme = $curPtFunc.Schemes[$rowPtInfo.PtScheme];
            }
            
            if ($rowPtInfo.TableObjId -ne $null -and $rowPtInfo.TableObjId.GetType().Name -ine "DBNull") {
                $tableObjectId = $rowPtInfo.TableObjId
                $fullTableName = "[$($rowPtInfo.TableSchema)].[$($rowPtInfo.TableName)]"
                if ($Tables[$fullTableName] -eq $null) {
                    $Tables[$fullTableName] = New-Object PSObject -Property @{
                        FullTableName = $fullTableName
                        TableSchema   = $rowPtInfo.TableSchema
                        TableName     = $rowPtInfo.TableName
                        TableObjID    = $rowPtInfo.TableObjID
                        IsAligned     = $rowPtInfo.isAligned
                        Indexes       = @{}
                    }
                }
                $curTable = $Tables[$fullTableName];     
                $idxName = $rowPtInfo.IndexName
                if ($curPtScheme.Indexes[$idxName] -eq $null) {
                    $rows = ConvertFrom-JSON $rowPtInfo.Rows
                    $tbCfg = ConvertFrom-Json $rowPtInfo.TabConfig
                    $curIndex = New-Object PSObject -Property @{
                        TableObj    = $curTable
                        IndexName   = $rowPtInfo.IndexName
                        IndexID     = $rowPtInfo.IndexID
                        IndexType   = $rowPtInfo.IndexType
                        Config      = $tbCfg.Cfg
                        PtColumn    = $rowPtInfo.PtColumn
                        PtFunction  = $curPtFunc
                        PtScheme    = $curPtScheme
                        Rows        = $rows.Rows
                    }
                    
                $curPtScheme.Indexes[$rowPtInfo.IndexName] = $curIndex
                $curTable.Indexes[$idxName] = $curIndex
                }
            }

            #   Now assemble the FS partition manager extended properties into a configuration collectio
            #   Database -> Function -> Scheme -> Table -> Index
            #
        }

    }


    #   Return the assembled information as an object collection
    #
    End {
        #   Add GetFunction($fName) script method
        Add-Member -InputObject $PtInfo -MemberType ScriptMethod -Name "GetFunction" -Value {
            param( $fName )
            $fInfo = $this.Functions[$fname]
            return $fInfo
        }

        #   Add GetScheme($sName) script method
        Add-Member -InputObject $PtInfo -MemberType ScriptMethod -Name "GetScheme" -Value {
            param( $sName )
            $sInfo = $this.Schemes[$sname]
            return $sInfo
        }

        #   Add GetTable($tSchema, $tName) script method
        Add-Member -InputObject $PtInfo -MemberType ScriptMethod -Name "GetTable" -Value {
            param( $tSchema, $tName )
            $tblName = "[$($tSchema)].[$($tname)]"
            $tInfo = $this.Tables[$tblName]
            return $tInfo
        }

        #   Add GetIndex($tObjId, $idxName) script method
        Add-Member -InputObject $PtInfo -MemberType ScriptMethod -Name "GetIndex" -Value {
            param( $tInfo, $idxName )
            $idxInfo = $tInfo.Indexes[$idxName]
            return $idxInfo
        }

        #$rsGetPtInfo | FT
        Write-Output $PtInfo
    }

}

if (-not $FSDeploymentIsLoading){
    $ptInfo = Get-FSSqlPartitionInfo -InstanceName "PBG1SQL01V105.fs.local" -Database "ModuleAssembly"
    $a = 3
    #$ptInfo = Get-FSSqlPartitionInfo -InstanceName "EDR1SQL01V700.fs.local\ODSPROD" -Database "ODS"
    #$ptInfo | ft
}