function Checkpoint-FSDeployDirectories  {
    <#
.SYNOPSIS

Scans the defined FS deployment directories for new, modified and/or removed files.  
Changes are recorded in the FSDeployDB database.

.DESCRIPTION

Scans the defined FS deployment directories for new, modified and/or removed files.  This involves reading
the file attributes and file binary signature to detect changes.
Changes are recorded in the FSDeployDB database. 
** This is only executed on the global master job server **

.PARAMETER FullInstanceName
Specifies the Fully qualified SQL Server instance name of the global master job server 

.PARAMETER Database
Specifies the database name of the deployment control database.  
Default is 'FSDeployDB'

.INPUTS

None. You cannot pipe objects to Checkpoint-FSDeployDirectories

.OUTPUTS

System.String.  Synopsis of the work accomplished

.EXAMPLE

PS> Checkpoint-FSDeployDirectories -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $FullInstanceName,

        [Parameter(Mandatory=$false)]
        [string]
        $Database = "FSDeployDB",

        [Parameter(Mandatory=$true)]
        [string]
        $Directory,

        [Parameter(Mandatory=$true)]
        [int]
        $DeployDirID = 0
    )

    #   1) Connect to the FSDeployDB database and get the list of registered deployment directories
    #
    Begin {
        
            #   The needed SQL statements are below

        $TestFileSql = @"
        USE [$($Database)];
        SELECT  COUNT(*) AS FileCnt,
                COUNT(CASE WHEN (FL.LastWriteTimeUtc = @LastWriteTimeUtc) THEN 1 ELSE NULL END) AS DupCnt
            FROM  [dbo].[FileLibrary] FL
            WHERE (FL.FileName = @FileName) 
                AND (FL.FilePath = @FilePath) 
                AND (FL.DeployDirID = @DeployDirID)
                AND (FL.LastWriteTimeUtc <= @LastWriteTimeUtc)
          --  GROUP BY FL.FileName, FL.FilePath
"@

        $InsertNewFileSql = @"
        USE [$($Database)];
        INSERT  INTO [dbo].[FileLibrary] (DeployDirID, FileName, FilePath, DestFlgs, ByteLen, ByteLenCompress, ByteContent, CreationTimeUtc, LastWriteTimeUtc)
        VALUES (@DeployDirId, @FileName, @FilePath, @DestFlgs, @ByteLen, @ByteLenCompress, @ByteContent, @CreationTimeUtc, @LastWriteTimeUtc)
"@

        #   Create the SqlCommand objects and create the query SqlParameter list
        #   Test Query
        $sqlCmdTest = New-OBJECT SYSTEM.DATA.SqlClient.SqlCommand
        $sqlCmdTest.CommandText = $TestFileSql

        $sqlCmdTest.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@DeployDirID",[Data.SQLDBType]::Int))) | OUT-NULL
        $sqlCmdTest.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@FileName",[Data.SQLDBType]::VarChar, 256))) | OUT-NULL
        $sqlCmdTest.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@FilePath",[Data.SQLDBType]::VarChar, 512))) | OUT-NULL
        $sqlCmdTest.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@LastWriteTimeUtc",[Data.SQLDBType]::DateTime2))) | OUT-NULL

        #   Insert Query
        $sqlCmdIns = New-OBJECT SYSTEM.DATA.SqlClient.SqlCommand
        $sqlCmdIns.CommandText = $InsertNewFileSql

        $sqlCmdIns.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@DeployDirID",[Data.SQLDBType]::Int))) | OUT-NULL
        $sqlCmdIns.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@FileName",[Data.SQLDBType]::VarChar, 256))) | OUT-NULL
        $sqlCmdIns.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@FilePath",[Data.SQLDBType]::VarChar, 512))) | OUT-NULL

        $sqlCmdIns.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@CreationTimeUtc",[Data.SQLDBType]::DateTime2))) | OUT-NULL
        $sqlCmdIns.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@LastWriteTimeUtc",[Data.SQLDBType]::DateTime2))) | OUT-NULL
                                                                                                
        $sqlCmdIns.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@DestFlgs",[Data.SQLDBType]::Int))) | OUT-NULL
        $sqlCmdIns.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@ByteLen",[Data.SQLDBType]::BigInt))) | OUT-NULL
        $sqlCmdIns.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@ByteLenCompress",[Data.SQLDBType]::BigInt))) | OUT-NULL
        $sqlCmdIns.Parameters.ADD((New-OBJECT DATA.SQLClient.SQLParameter("@ByteContent",[Data.SQLDBType]::VarBinary))) | OUT-NULL                                                                                                
    }

    Process{

        #   Open the connection to the SQL Server repository

        try {
            $sqlConn = New-OBJECT SYSTEM.DATA.SqlClient.SQLConnection
            $sqlConn.ConnectionString = "Server=$($FullInstanceName);Database=$($Database);Integrated Security=True;"
            $sqlConn.OPEN()     

            $sqlCmdTest.Connection = $SqlConn
            $sqlCmdIns.Connection = $SqlConn

            Get-ChildItem -Path $Directory -Recurse | ForEach-Object {
                #
                #   Read each file into a binary byte array and compress it.  Get other file information as well.
                $FileName = $_.Name
                $FilePath = $_.Directory.FullName
                $CreationTimeUtc = $_.CreationTimeUtc
                $LastWriteTimeUtc = $_.LastWriteTimeUtc
                $ByteLen = $_.Length

                #   Prepare the test query to see if the file already exists in the repository

                $sqlCmdTest.Parameters["@DeployDirID"].Value = $DeployDirID
                $sqlCmdTest.Parameters["@FileName"].Value = $FileName
                $sqlCmdTest.Parameters["@FilePath"].Value = $FilePath
                $sqlCmdTest.Parameters["@LastWriteTimeUtc"].Value = $LastWriteTimeUtc

                $dtTest = New-Object System.Data.DataTable
                [System.Data.SqlClient.SqlDataReader] $drTest = $sqlCmdTest.ExecuteReader()
                $Private:status = $dtTest.Load($drTest)
                $drTest.Close();
                $drTest.Dispose();

                $FileCnt = $dtTest.Rows[0]["FileCnt"]
                $DupCnt = $dtTest.Rows[0]["DupCnt"]
                $dtTest.Clear();
                $dtTest.Dispose();

                #   If the file does not already exist, then insert it into the repository

                if ($DupCnt -eq 0) {
                        
                    [byte[]] $Private:content = [IO.File]::ReadAllBytes($_.FullName)  #  To get the entire contents of a file:
                    [byte[]] $Private:cmprsdContent = Get-CompressedByteArray $content  # Compress via GZip method
                    $ByteLenCompress = $cmprsdContent.Length    

                    $sqlCmdIns.Parameters["@DeployDirID"].Value = $DeployDirID
                    $sqlCmdIns.Parameters["@FileName"].Value = $FileName
                    $sqlCmdIns.Parameters["@FilePath"].Value = $FilePath
                    $sqlCmdIns.Parameters["@ByteLen"].Value = $ByteLen
                    $sqlCmdIns.Parameters["@DestFlgs"].Value = $DestFlgs
                    $sqlCmdIns.Parameters["@ByteLenCompress"].Value = $ByteLenCompress
                    $sqlCmdIns.Parameters["@ByteContent"].Size = $ByteLenCompress
                    $sqlCmdIns.Parameters["@ByteContent"].Value = $cmprsdContent
                    $sqlCmdIns.Parameters["@CreationTimeUtc"].Value = $CreationTimeUtc
                    $sqlCmdIns.Parameters["@LastWriteTimeUtc"].Value = $LastWriteTimeUtc

                    $sqlCmdIns.ExecuteNonQuery();

                    $sqlCmdIns.Parameters["@ByteContent"].Value = $null
                }
                
            }
        }
        catch {
            $_.Exception.Message
            $_.ScriptStackTrace
            $false
        }
        finally {
            $sqlConn.Close();
            $sqlConn.Dispose();
        }
    }

    End{}
}
