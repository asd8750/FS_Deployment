# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
function Set-FsTCTrigger  {
    <#
.SYNOPSIS

FS Copy Table - (SubCommand) - Create a trigger on either the source or destination table

.DESCRIPTION

Create a custom insert, update, delete trigger on either the source or destination table for this copy table operation.

.EXAMPLE

PS> New-FSTableTransform

#>
    [CmdletBinding()]
    Param (
        # Source Table Information
        #
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [Alias("ControlObject")]
        [PSTypeName('FSCopyTableControl')] 
        $CtlObj,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        #[ValidationSet("Ins", "Insert", "Upd", "Update", "Del", "Delete", "All")]
        [string]
        $Action,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        #[ValidationSet("Source", "Src", "Destination", "Dest")]
        [string]
        $Target
    )

    Begin {

        $Version_FSCTTrigger = "1.0.1"

        $ErrorLoggingTable = "[DBA].[ErrorLogging]";

        #   Ensure the SqlServer module is loaded
        if ((Get-Module | Where-Object { $_.Name -ilike "sqlserver" }).Count -eq 0) {
            Write-Verbose "Importing module: 'SqlServer'"
            Import-Module -Name SqlServer
        }

        #   Fetch table column schema information from a specified table
        #
        $sqlTabInfo = @"
        WITH IDXC
        AS
        (
            SELECT *,
                   ROW_NUMBER() OVER ( PARTITION BY IDXC2.[object_id],
                                                    IDXC2.column_id
                                       ORDER BY IDXC2.IndexPriority
                                     ) AS ColPrio
               FROM
                   (
                       SELECT DISTINCT
                              TBL.[object_id],
                              TBL.[type] AS ObjType,
                              COALESCE(IDX.index_id, 0) AS IndexID,
                              COALESCE(IDX.is_unique, 0) AS is_unique,
                              COL.column_id,
                              COALESCE(IDC.key_ordinal, 0) AS key_ordinal,
                              COALESCE(IDC.is_descending_key, 0) AS is_descending_key,
                              COALESCE(IDC.partition_ordinal, 0) AS partition_ordinal,
                              DENSE_RANK() OVER ( PARTITION BY TBL.[object_id]
                                                  ORDER BY
                                                      COALESCE(IDX.is_unique, 0) DESC,
                                                      COALESCE(IDX.index_id, 0)
                                                ) AS IndexPriority
                          FROM(sys.objects TBL
                              INNER JOIN sys.columns COL
                                 ON ( TBL.[object_id] = COL.[object_id] ))
                              LEFT OUTER JOIN(sys.indexes IDX
                              INNER JOIN sys.index_columns IDC
                                 ON ( IDX.[object_id] = IDC.[object_id] )
                                    AND ( IDX.[index_id] = IDC.[index_id] ))
                                ON ( TBL.[object_id] = IDX.[object_id] )
                                   AND ( COL.column_id = IDC.column_id )
                          WHERE
                           ( TBL.[type] IN ( 'U', 'V' ))
                          --ORDER BY TBL.object_id, COL.column_id
                   ) IDXC2
              --ORDER BY IDXC2.object_id, IDXC2.column_id, IDXC2.IndexPriority
        )
         SELECT TAB.[object_id],
                TAB.[schema_id],
                SCH.[name] AS SchemaName,
                TAB.[name] AS TableName,
                TAB.[type] AS ObjType,
                COALESCE(IDX.[type], 0) AS IndexType,
                IC.[name] AS ColName,
                IC.column_id,
                TYP.[name] AS DataType,
                IC.max_length,
                IC.[precision],
                IC.[scale],
                IC.is_xml_document,
                IC.is_nullable,
                IC.is_identity,
                IC.is_computed + (ISNULL(cc.is_persisted,0)*2) AS is_computed, -- 0-Not computed, 1-computed, 3-computed+persisted
                CASE WHEN TYP.[name] IN ('timestamp', 'rowversion') THEN 0
                  ELSE 1
                END AS is_updateable,
                COALESCE(IDXC.key_ordinal, 0) AS key_ordinal,
                COALESCE(IDXC.is_descending_key, 0) AS is_descending_key,
                COALESCE(IDXC.partition_ordinal, 0) AS partition_ordinal,
                CONCAT(TYP.[name],
                      CASE
                          WHEN (TYP.[name] LIKE '%char') OR (TYP.[name] LIKE '%binary') THEN CONCAT('(', CASE WHEN IC.max_length = -1 THEN 'MAX' ELSE CONVERT(VARCHAR, IC.max_length) END,')')
                          WHEN (TYP.[name] LIKE 'datetime2') THEN CONCAT('(', CONVERT(VARCHAR, IC.scale) ,')')
                          WHEN (TYP.[name] LIKE 'decimal') THEN CONCAT('(', CONVERT(VARCHAR, IC.[precision]), ',', CONVERT(VARCHAR, IC.scale) ,')')
                          WHEN (TYP.[name] LIKE 'float') THEN CASE WHEN IC.[precision] <> 53 THEN CONCAT('(', CONVERT(VARCHAR, IC.[precision]) ,')') END
                          WHEN (TYP.[name] LIKE 'time') THEN 
                                                          CASE (IC.[precision] * 10 + IC.scale)
                                                              WHEN 80 THEN '(0)'
                                                              WHEN 101 THEN '(1)'
                                                              WHEN 112 THEN '(2)'
                                                              WHEN 123 THEN '(3)'
                                                              WHEN 134 THEN '(4)'
                                                              WHEN 145 THEN '(5)'
                                                              WHEN 156 THEN '(6)'
                                                              WHEN 167 THEN '(7)'
                                                              ELSE CONCAT('(?? ', CONVERT(VARCHAR, IC.[precision]), ',', CONVERT(VARCHAR, IC.scale), ')')
                                                          END
                      END
                ) AS FullDatatype,
                CC.[definition] AS cc_definition,
                DF.[name] AS df_name,
                DF.[definition] AS df_definition
            FROM sys.objects TAB
                INNER JOIN sys.schemas SCH
                   ON ( TAB.[schema_id] = SCH.[schema_id] )
                INNER JOIN sys.columns IC
                   ON ( TAB.[object_id] = IC.[object_id] )
                INNER JOIN sys.types TYP
                   ON ( IC.user_type_id = TYP.user_type_id )
                LEFT OUTER JOIN sys.indexes IDX
                  ON ( TAB.[object_id] = IDX.[object_id] )
                LEFT OUTER JOIN /* sys.index_columns */ IDXC
                  ON ( TAB.[object_id] = IDXC.[object_id] )
                     -- AND ( IDX.index_id = IDXC.index_id )
                     AND ( IC.column_id = IDXC.column_id )
                LEFT OUTER JOIN sys.computed_columns CC
                  ON (IC.[object_id] = CC.[object_id]) AND (IC.column_id = CC.column_id) AND (IC.is_computed = 1)
                LEFT OUTER JOIN sys.default_constraints DF
                  ON (TAB.[object_id] = DF.[parent_object_id]) AND (IC.column_id = DF.[parent_column_id])
            WHERE
             ( TAB.is_ms_shipped = 0 )
             AND ( TAB.[type] IN ( 'U', 'V' ))
             AND ( ISNULL(IDX.[type], 0) IN ( 0, 1, 5 ))
             AND ( IDXC.ColPrio = 1 )
             AND ( SCH.[name] = `$(TblSchema) ) AND ( TAB.[name] = `$(TblName))
  
            ORDER BY
             SchemaName ,
             TableName ,
             IC.column_id;
"@;

        #   Get the column lists for the source and destination tables
        #
        $infSrcTable = Invoke-Sqlcmd -ServerInstance $CtlObj.SrcInstance -Database $CtlObj.SrcDatabase -query $sqlTabInfo `
                        -Variable "TblSchema='$($CtlObj.SrcTableSchema)'","TblName='$($CtlObj.SrcTableName)'"
        $infDestTable = Invoke-Sqlcmd -ServerInstance $CtlObj.DestInstance -Database $CtlObj.DestDatabase -query $sqlTabInfo  `
                        -Variable "TblSchema='$($CtlObj.DestTableSchema)'","TblName='$($CtlObj.DestTableName)'"
       
        #   Detect use of a sequence in a DEFAULT value clause of the destination table
        #
        $defSeqCol = ( $infDestTable | Where-object { "DBNull" -ine $_.df_name.GetType().name} ) | Where-object { ($_.df_definition -ilike "*NEXT VALUE FOR*") } | Select-ObJect -First 1
        if ($defSeqCol) {
            if ($defSeqCol.df_definition -imatch 'NEXT VALUE FOR +?(.+?)\)') {
                $SeqName = $Matches[1]
            }
        }

        $defIdentCol = ( $infDestTable | Where-object is_identity -ne 0 | Select-ObJect -First 1 )
        # if ($defIdentCol) {
        #     if ($defSeqCol.df_definition -imatch 'NEXT VALUE FOR +?(.+?)\)') {
        #         $SeqName = $Matches[1]
        #     }
        # }
        #   Generate names used by different object we create
        #
        $nameSrcTable = "[$($CtlObj.SrcTableSchema)].[$($CtlObj.SrcTableName)]"
        $nameDestTable = "[$($CtlObj.DestTableSchema)].[$($CtlObj.DestTableName)]"
        $nameTrigger =  "[$($CtlObj.SrcTableSchema)].[trg_$($CtlObj.SrcTableName)_FSCT]"


        #   Start assembling the TSQL source code of the trigger
        #
        $sqlTrigger = @"
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
    -- =============================================
    -- Author:      (PowerShell) Set-FSCTTrigger
    -- Create date: $(Get-Date -format 'yyyy-MM-dd HH:mm')
    -- Version:     $($Version_FSCTTrigger)
    -- Description:	Backfill trigger for use with FSCopyTable
    -- =============================================
    CREATE TRIGGER $($nameTrigger)
    ON  $($nameSrcTable)
    WITH EXECUTE AS OWNER
    AFTER INSERT,DELETE,UPDATE
    AS 
    BEGIN
        SET NOCOUNT ON;
$( 
    #   Output code to update the NEXT VALUE f the sequence if one of the new inserted rows IDs exceeds the current sequence high
    #
            if ($SeqName){
"       
        --  Update the NEXT VALUE for the sequence to be above the MAX id of the new rows from INSERTED, if needed.
        DECLARE @seqCurrent BIGINT;
        DECLARE @maxID  BIGINT;
        SELECT  @MaxID = MAX([$($defSeqCol.ColName)])
            FROM inserted INS;
        SELECT  @seqCurrent = CAST(current_value AS BIGINT)
            FROM  sys.sequences
            WHERE [object_ID] = OBJECT_ID('$($SeqName)')
        IF (@MaxID >= @seqCurrent)
            BEGIN
                DECLARE @sqlAlter VARCHAR(1000);
                SET @sqlAlter = CONCAT('ALTER SEQUENCE $($SeqName) RESTART WITH ', CONVERT(VARCHAR,(@MaxID+1)))
                EXEC(@sqlAlter); 
            END;
"
            }
)
        BEGIN TRY
$(
    #   Enable IDENTITY_INSERT mode if we are preserving the supplied ID column value
    #
    if ($defIdentCol) {
"           SET IDENTITY_INSERT $($nameDestTable) ON;"        
    }

    #   Main insert section of the trigger
    #
)           
            --  Insert all rows from the INSERTED trigger table
            INSERT  INTO $($nameDestTable) (
                $(
                    $insCols = ($infSrcTable | Sort-Object column_id ) | Where-Object is_updateable -eq 1
                    foreach ($Col in $insCols[0..($insCols.Count-2)]) {
                        "   [$($Col.ColName)], `n              "
                    }
                    "   [$($insCols[$insCols.Count-1].ColName)] `n                "
            )  )
                SELECT $(            
                    $insColNames = ($infSrcTable | Sort-Object column_id ) | Where-Object is_updateable -eq 1
                    foreach ($Col in $insColNames[0..($insColNames.Count-2)]) {
                        "   INS.[$($Col.ColName)], `n                  "
                    }
                    "   INS.[$($insCols[$insCols.Count-1].ColName)] `n                  "
                )    
                    FROM INSERTED INS;
$(
    #   Disable IDENTITY_INSERT if enabled in this trigger
    #
    if ($defIdentCol) {
"           SET IDENTITY_INSERT $($nameDestTable) OFF;"        
    }

    #   Handle any captured errors
    #
) 
        END TRY
        BEGIN CATCH
            DECLARE @Err_Msg    NVARCHAR(4000),
                    @Err_Sev    SMALLINT,
                    @Err_Sta    SMALLINT;
            SELECT  @Err_Msg = ERROR_MESSAGE(),
                    @Err_Sev = ERROR_SEVERITY(),
                    @Err_Sta = ERROR_STATE();
            DECLARE @sqlLogInsert  NVARCHAR(4000);
            DECLARE @CRLF NCHAR(2) = CHAR(13)+CHAR(10);
            SET @sqlLogInsert = CONCAT('INSERT INTO $($ErrorLoggingTable) (ErrMsg, ErrSev, ErrSta, InfoStr1, InfoStr2, InfoStr3) ', @CRLF,
                    'VALUES (', @CRLF,
                    '''', @Err_Msg, ''',', @CRLF,
                    CONVERT(NVARCHAR, ISNULL(@Err_Sev,0)), ',',  @CRLF,
                    CONVERT(NVARCHAR, ISNULL(@Err_Sta,0)), ',', @CRLF,
                    '''$($nameSrcTable)'',', @CRLF,
                    '''$($nameDestTable)'',', @CRLF,
                    '''$($nameTrigger)'',', @CRLF,
                    ')'                             
                )
            IF (OBJECT_ID('$($ErrorLoggingTable)') IS NOT NULL)
                BEGIN
                    EXEC (@sqlLogInsert);
                END;
        END CATCH;
            
    END

"@;

        $a = 1
    }

}

if (-not $FSDeploymentIsLoading){
    $obj = New-FSTableCopyCtl -SrcInstance "PBG1SQL01V105.fs.local" -SrcDatabase "ClipProcessData" -SrcTableSchema "Clip" -SrcTableName "CIIVRaw" `
                            -DestTableSchema "DBA-Post" -Verbose
    $obj | Format-List -Property *

    Set-FsTCTrigger -ControlObject $obj -Action 'INS' -Target 'Src'
}