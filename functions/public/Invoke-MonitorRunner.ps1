
function Invoke-MonitorRunner {
    <#
    .SYNOPSIS

    Run a monitoring session that will query other servers for information

    .DESCRIPTION


    .PARAMETER FullInstanceName
    Specifies the Fully qualified SQL Server instance name of the global master job server 

    .PARAMETER SkipSystemDB

    .INPUTS

    None. You cannot pipe objects to Checkpoint-FSDeployDirectories

    .OUTPUTS

    PS Object

    .EXAMPLE

    PS> Get-FSSqlPartitionInfo -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

    #>
    [CmdletBinding()]
    Param (
        [parameter(Mandatory=$true)]
        [string[]] 
        $TargetInstanceList,
    
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $RepoInstance,

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $RepoDatabase,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [string] 
        $RepoSchema = "MonitorData",

        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $ListName,

        [parameter(ValueFromPipeline=$false, Mandatory=$false)]
        [switch] 
        $ResumeSession

    )

    begin {

        #$ListName = "DBInfo"
        #$ResumeSession = 0


        #
        #  ############################################################################################
        #   Get initial information from the Data Repository
        #  ############################################################################################
        #   Data Repository holds the control tables and stored procedures.
        #
        #$RepoInstance = 'EDR1SQL01S004\DBA'
        #$RepoDatabase = 'RepoInstanceInfo'
        #$RepoSchema = 'MonitorData'

        #   Get the list of valid tables in the Repo instance

        $RepoTableLists = @{};
        $RepoTableLists[$RepoDatabase] = @{}  # Create an empty hash list of tables for the current repo-database

        #
        #  ############################################################################################
        #   Obtain a list of Sql Server instances we will target for information queryies
        #  ############################################################################################
#         $TargetInstanceList = @();
#         $TargetInstanceList += $TargetInstance

#         $MSXTSXServers = Invoke-Sqlcmd -ServerInstance $RepoInstance -Database "msdb" -Query @"
#                 SELECT	* 
#                     FROM msdb.dbo.systargetservers
#                     WHERE (last_poll_date > DATEADD(DAY, -1, GETDATE()))
#                     ORDER BY server_name
# "@
#         $TargetInstanceList = ($MSXTSXServers | Select-Object -ExpandProperty server_name)

        if ($ResumeSession) {
            $sqlMostRecent = @"
                WITH FICLast AS  (
                        SELECT	ImportCycle,
                                FullInstanceName,
                                DateCreated,
                                FICLast2.StepSeq,
                                ROW_NUMBER() OVER (ORDER BY FICLast2.DateCreated DESC) AS MRFICNUM
                            FROM (
                                SELECT	FIC.ImportCycle,
                                        FIC.FullInstanceName,
                                        FIC.DateCreated,
                                        FIC.StepSeq,
                                        ROW_NUMBER() OVER (PARTITION BY FIC.FullInstanceName ORDER BY FIC.DateCreated DESC) AS FICNUM
                                    FROM MONITOR.FetchInfoControl FIC
                                        INNER JOIN Monitor.FetchInfoList FIL
                                            ON (FIC.ListID = FIL.ListID)
                                    WHERE (FIC.StepSeq >= 999999999 )
                                        AND (FIL.ListName = '$($ListName)')
                                    ) FICLast2
                                WHERE (FICLast2.FICNUM = 1)
                            )
                    SELECT	FICLast.FullInstanceName,
                            FICLast.ImportCycle,
                            FICLast.DateCreated
                        FROM FICLast
                        WHERE (FICLast.MRFICNUM = 1)
"@
            $FIMostRecent = Invoke-SqlCmd -ServerInstance $RepoInstance -Database $RepoDatabase -Query $sqlMostRecent
            $LastCollectedInstance = $null
            foreach ($MR in $FIMostRecent) {
                $LastCollectedInstance = $MR.FullInstanceName
                $TargetInstanceList = ($TargetInstanceList | Where-Object  {$_ -gt $MR.FullInstanceName} | Sort-Object)
                break
            }
        }


        #
        #  ############################################################################################
        #   Now Loop per target instances
        #  ############################################################################################
        :TargetInstanceLoop
        foreach ( $TargetInstance in $TargetInstanceList) {

            "--> ListName: $($ListName)  TargetInstance: $($TargetInstance)"    

            #   Now open a FetchInfo session
            $FIControl = Invoke-SqlCmd -ServerInstance $RepoInstance -Database $RepoDatabase -MaxCharLength 1000000 `
                        -Query "EXEC [Monitor].[sp_FetchInfo_OpenSession] @FullInstanceName='$($TargetInstance)', @ListName='$($ListName)', @Resume=0"


            #  Order by StepSeq and eliminate the "End of List" row
            #
            $FISteps = $FIControl | Where-Object StepSeq -lt 999999999 | Sort-Object StepSeq | Select-Object

            #   Now process each step in the $FISteps collection
            #
            :StepLoop
            foreach ($FIStep in $FISteps) {
                $ImportCycle = $FIStep.ImportCycle
                $ListID = $FIStep.ListID
                $StepID = $FIStep.StepID
                $StepSeq = $FIStep.StepSeq

                if ($FIStep.ImportDatabase -eq 'DEFAULT') { $ImportDatabase = $RepoDatabase } else { $ImportDatabase = $FIStep.ImportDatabase };
                if ($FIStep.ImportSchema -eq 'DEFAULT')   { $ImportSchema = $RepoSchema } else { $ImportSchema = $FIStep.ImportSchema };

                $StepID = $FIStep.StepID        
                $StepSeq = $FIStep.StepSeq
                $TargetDatabaseList = @()

                #   $TargetDatabase = '[ALL]'     - Query each database with this query
                #                   = '[USER]'    - Similar to '[ALL]' - Exclude the system databases (master, msdb, tempdb, model) 
                #                   =  anything else is treated as a single database to scan
                try {
                    if ($FIStep.TargetDatabase -ieq '[ALL]' -or $FIStep.TargetDatabase -ieq '[USER]') {
                        $sqlTrgtDBs = @'
                            SELECT DB.[name]
                                FROM sys.databases DB 
                                WHERE (DB.state_desc = 'ONLINE') AND (DB.user_access_desc = 'MULTI_USER') AND (DB.is_in_standby = 0) AND (is_read_only = 0)
                                    AND (ISNULL(DATABASEPROPERTYEX(DB_NAME(database_id), 'Updateability'), 'READ_WRITE') IN ('READ_WRITE'))
                                    AND ([name] NOT IN ('model'))
                                ORDER BY DB.database_id
'@
                        $TargetDatabaseList = (Invoke-Sqlcmd -ServerInstance $TargetInstance -Database 'master' -Query $sqlTrgtDBs | 
                                        Select-Object -ExpandProperty name);
                        if ($FIStep.TargetDatabase -ieq '[USER]') {
                            $TargetDatabaseList = $TargetDatabaseList | Where-Object {-not ('master','msdb','model','tempdb' -contains $_) } 
                        } 
                    }
                    else {
                        $TargetDatabaseList += $FIStep.TargetDatabase
                    }
                }

                catch {
                    #   If we catch any exception then close the session for this instance
                    $EscapedExMsg = $_.Exception.Message -replace "'","''";
                    $EscapedExStackTrace = $_.ScriptStackTrace -replace "'","''"

                    $sqlCloseSession = "EXEC [Monitor].[sp_FetchInfo_CloseSession] @ImportCycle=$($ImportCycle), @CompletionStatus=-1, @Message='$($EscapedExMsg)', @StackTrace='$($EscapedExStackTrace)'"
                    $ResultClose = Invoke-SqlCmd -ServerInstance $RepoInstance -Database $RepoDatabase -MaxCharLength 1000000 -Query $sqlCloseSession
                    continue TargetInstanceLoop         # Skip this $TargetInstance is we get an error getting the database list
                }
                
                #   Submit the current query step to each database in the $TargetDatabaseList collection
                #
                :TargetDatabaseLoop
                for ($dIdx=0; $dIdx -lt $TargetDatabaseList.Count; ++$dIdx) {
                    $TargetDatabase = $TargetDatabaseList[$dIdx];
                    $LastDB = ($dIdx -ge ($TargetDatabaseList.Count -1))

                    $EscapedExMsg = ""              # Clear out the completion messages
                    $EscapedExStackTrace = ""

                    # 
                    #   We have a valid step and target.  Send the query to the target instance.
                    #   Invoke-FSSqlCmd also returns datatype information per returned column.
                    #
                    $StepQueryText = $FIStep.QueryText 
                    $FIResults = Invoke-FSSqlCmd -Instance $TargetInstance -Database $TargetDatabase -GetSchema -Query $StepQueryText 
                    if ($FIResults.ExMsg -eq '') {
                        $RowsReturned = $FIResults.Data.Rows.Count

                        #   After we have the query results, check the existence of the receiving import table.
                        #   If it exists, import its column list information
                        #
                        $RepoFullTableName = "[$($ImportSchema)].[$($FIStep.ImportTable)]"
                        $RepoTableSchema = $null

                        if (-not (($RepoTableLists[$RepoDatabase]).ContainsKey($RepoFullTableName))) {
                            # The repo receiving table is not in our table schema cache, so test for its existence in the repo database
                            $sqlGetTableInfo = @"
                                IF (OBJECT_ID('$($RepoFullTableName)') IS NOT NULL)
                                    SELECT TOP (1) *    
                                        FROM $($RepoFullTableName)
                                        WHERE (1=0);
"@
                            $tblInfo = Invoke-FSSqlCmd -Instance $RepoInstance -Database $RepoDatabase -GetSchema -Query $sqlGetTableInfo
                            if (($tblInfo.Schema) -and ($tblInfo.Schema[0].SqlDef -ne $null)) {
                                ($RepoTableLists[$RepoDatabase])[$RepoFullTableName] = $tblInfo.Schema   # Receiving table exists, cache its column information
                            }
                        }
                        $RepoTableSchema = ($RepoTableLists[$RepoDatabase])[$RepoFullTableName] # Point to the repor table schema, $null is no table exists

                        if (-not $RepoTableSchema) {

                            #   Create the repo receiving table with the appropriate partitioning.
                            #   The table will have some standard columns added to Identify each import batch of data.
                            $sqlCreateTable = @"
                                CREATE TABLE $($RepoFullTableName) (
                                    [__ID__] BIGINT IDENTITY(1,1) NOT NULL,
                                    [__ImportCycle__] BIGINT NOT NULL,
                                    [__DateTimeUtcCaptured__] DATETIME2(7) NOT NULL,
                                    [__TargetDatabase__] VARCHAR(128) 
"@

                            foreach ($col in $FIResults.Schema) { 
                                $sqlCreateTable = $sqlCreateTable + ",`r            [$($col.ColumnName)] $($col.SqlDef) NULL"
                            }
                            $sqlCreateTable = $sqlCreateTable + "`r        ) ON [PtSch_MonitorData_FetchInfoMonthly]([__DateTimeUtcCaptured__]);;";
                            Invoke-Sqlcmd -ServerInstance $RepoInstance -Database $RepoDatabase -Query $sqlCreateTable

                            #   Now create the clustered columnstore index 
                            $sqlCreateCCSI = @"
                                CREATE CLUSTERED COLUMNSTORE INDEX [CCSI_$($ImportSchema)_$($FIStep.ImportTable)] ON $($RepoFullTableName) 
                                    WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0, DATA_COMPRESSION = COLUMNSTORE) 
                                        ON [PtSch_MonitorData_FetchInfoMonthly]([__DateTimeUtcCaptured__]);
"@
                            Invoke-Sqlcmd -ServerInstance $RepoInstance -Database $RepoDatabase -Query $sqlCreateCCSI
                        }

                        else {
                            #   If the repo table exists, then make sure every column in the returned dataset exists in the repo table.
                            #   Create any new columns found in the returned dataset into the repo table
                            $NewColCount = 0
                            $sqlAddColumns = "ALTER TABLE $($RepoFullTableName) ADD "
                            foreach ($col in $FIResults.Schema) { 
                                if ($null -eq ($RepoTableSchema | where-object ColumnName -eq $Col.ColumnName)) {
                                    $NewColCount += 1  # Increment new column counter
                                    $sqlAddColumns = $sqlAddColumns + " [$($Col.ColumnName)] $($col.SqlDef) NULL,"

                                }
                            } 

                            #   If any new columns are detected, submit the constructed ALTER TABLE command to add them to the repo table
                            #
                            if ($NewColCount -gt 0) {
                                $sqlAddColumns = $sqlAddColumns.Substring(0, $sqlAddColumns.Length-1)
                                $result = Invoke-Sqlcmd -ServerInstance $RepoInstance -Database $RepoDatabase -Query $sqlAddColumns
                                ($RepoTableLists[$RepoDatabase]).Remove($RepoFullTableName)                # Remove the old table column list from the cache
                            }
                        }

                        #   Add our overhead columns/values to the dataset
                        #   -- __ImportCycle__ - Supplied by the FetchInfo session management stored procedures
                        #   -- __DateTimeUtcCaptured__ -- Time UTC when the captured rows were received
                        #
                        $newCol = $FIResults.Data.Columns.Add("__ImportCycle__", "System.Int64");
                        $newCol = $FIResults.Data.Columns.Add("__DateTimeUtcCaptured__", "System.Datetime");
                        $newCol = $FIResults.Data.Columns.Add("__TargetDatabase__", "System.String");

                        $nowDateTime = [DateTime]::UtcNow
                        For ($idx = 0; $idx -lt $FIResults.Data.Rows.Count; ++$idx) {  
                            $FIResults.Data.Rows[$idx]."__ImportCycle__"         = $ImportCycle        # Fill the overhead column with the ImportCycle value and the current time
                            $FIResults.Data.Rows[$idx]."__DateTimeUtcCaptured__" = $nowDateTime
                            $FIResults.Data.Rows[$idx]."__TargetDatabase__"      = $TargetDatabase
                        }

                        #   Build a list of data column to import by the SqlBulkCopy process in Write-FSSqlDataTable.  The SqlBulkCopy function
                        #   needs to be told the name pairs of the source and destination columns to "map" each piece of data moved.
                        #
                        $colNames = @()
                        $ColNames += '__ImportCycle__'
                        $ColNames += '__DateTimeUtcCaptured__'
                        $ColNames += '__TargetDatabase__'
                        $colNames += $FIResults.schema | Select-Object -expandproperty ColumnName

                        #   Now Import the query data into the metrics repository
                        try {
                            $bc = Write-FSSqlDataTable -SqlInstanceName $RepoInstance -Database $ImportDatabase -TableSchema $ImportSchema -TableName $FIStep.ImportTable -Columns $colNames -DataTable $FIResults.Data
                        }
                        catch {
                            $EscapedExMsg = $_.Exception.Message -replace "'","''";
                            $EscapedExStackTrace = $_.ScriptStackTrace -replace "'","''"
                        }
                    }

                    else {
                        $RowsReturned = 0
                        $EscapedExMsg = $FIResults.ExMsg
                        $EscapedExStackTrace = $FIResults.ExStackTrace
                    }

                    #   Finally, invoke the FetchInfo_StepComplete funtion to mark the end of this fetch step
                    #
                    $CompletionStatus = 0
                    if ($LastDB) { $CompletionStatus = 1 }

                    $sqlStepComplete = @"    
                        EXEC [Monitor].[sp_FetchInfo_StepComplete] @ImportCycle=$($ImportCycle), @StepSeq=$($StepSeq), @CompletionStatus=$($CompletionStatus), 
                                                @RowsReturned=$($RowsReturned), @TargetDatabase='$($TargetDatabase)', 
                                                @Message='$($EscapedExMsg)', @StackTrace='$($EscapedExStackTrace)'
"@

                    $FIStepCmpl = Invoke-SqlCmd -ServerInstance $RepoInstance -Database $RepoDatabase -Query $sqlStepComplete;

                    $a = 3  # DEBUG Breakpoint
                }
            
            } 

            $EscapedExMsg = ""              # Clear out the completion messages
            $EscapedExStackTrace = ""

            $sqlCloseSession = "EXEC [Monitor].[sp_FetchInfo_CloseSession] @ImportCycle=$($ImportCycle), @CompletionStatus=0, @Message='CloseSession', @StackTrace='$($EscapedExStackTrace)'"
            $ResultClose = Invoke-SqlCmd -ServerInstance $RepoInstance -Database $RepoDatabase -MaxCharLength 1000000 -Query $sqlCloseSession
        }

    }
    
}

#
# Test
#

if (-not $FSDeploymentIsLoading) {

    $TList = 'EDR1SQL01S004.fs.local\DBA'
    Invoke-MonitorRunner -RepoInstance "EDR1SQL01S004.fs.local\DBA" -RepoDatabase RepoInstanceInfo -RepoSchema MonitorData -ListName DBInfo `
            -TargetInstanceList $TList
}
