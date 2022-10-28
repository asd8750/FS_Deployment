function Write-FSSqlDataTable {

    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $SqlInstanceName,

        [Parameter(Mandatory=$true)]
        [string]
        $Database,

        [Parameter(Mandatory=$true)]
        [string]
        $TableSchema,

        [Parameter(Mandatory=$true)]
        [string]
        $TableName,

        [Parameter(Mandatory=$true)]
        [string[]]
        $Columns,
        
        [Parameter(Mandatory=$true)]
        [System.Data.DataTable]
        $DataTable
    )
    
    Begin {
        try {
            $connString = "Server=$($SqlInstanceName);Database=$($Database);Integrated Security=True;"
            [System.Data.SqlClient.SqlConnection] $sqlConn = New-Object System.Data.SqlClient.SqlConnection
            $sqlConn.ConnectionString = $connString
            $sqlConn.Open();
            $tran = $sqlConn.BeginTransaction()
            #$bcpyOpts = New-Object System.Data.SqlClient.SqlBulkCopyOptions
            #$bcpyOpts = [System.Data.SqlClient.SqlBulkCopyOptions]::KeepNulls.Value__ 
            $bulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy ($sqlConn, [System.Data.SqlClient.SqlBulkCopyOptions]::KeepNulls.Value__ , $tran)
            $bulkCopy.BulkCopyTimeout = 30;
            $bulkCopy.DestinationTableName = "[$($TableSchema)].[$($TableName)]"

            foreach ($colName in $Columns) {
                $newCMap = New-Object System.Data.SqlClient.SqlBulkCopyColumnMapping($colName, $colName);
                $bulkCopy.ColumnMappings.Add($newCMap);
            }

            $bulkCopy.WriteToServer($DataTable);
            Write-Output $Database.Rows.Count
            $tran.Commit();
        }

        catch {
            $e = $_
            if ($tran) {$tran.Rollback();}
            throw $_.Exception.Message
        }

        finally {
            if ($sqlConn) {
                $sqlConn.Close();
                $sqlConn.Dispose();
            }
        }

    }

}


if (-not $FSDeploymentIsLoading) {
    # $QryResults = Invoke-FSSqlCmd -SqlInstanceName 'PBG1SQL01V105' -Database 'master' -Query 'SELECT * FROM sys.databases'

    # $colNames = $QryResults.schema | Select-Object -expandproperty ColumnName
    # $bc = Write-FSSqlDataTable -SqlInstanceName 'EDR1SQL01S004\DBA' -Database 'DBAInventoryManagement' -TableSchema 'Monitor' -TableName 'SysDatabases' -Columns $colNames -DataTable $QryResults.Data
}
