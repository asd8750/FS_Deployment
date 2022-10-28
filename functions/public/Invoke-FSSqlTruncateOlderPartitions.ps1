function Invoke-FSSqlTruncateOlderPartitions  {
    <#
.SYNOPSIS

Create empty partitions in the future.

.DESCRIPTION

Returns a data structure containing a list of partition functions.  Schemes using the functions,
And tables ties to the schemes.

.PARAMETER FullInstanceName
Specifies the Fully qualified SQL Server instance name of the global master job server 

.PARAMETER Database
Specifies the database name of the deployment control database.  
Default is 'FSDeployDB'

.PARAMETER Schema
Specifies the schema of the table with older partitions

.PARAMETER Table
Specifies the name of the table with older partitions

.PARAMETER BeforeDate
Date cutoff between data retained and data truncated

.INPUTS

None. You cannot pipe objects to Invoke-FSSqlTruncateOlderPartitions

.OUTPUTS

PS Objectlimbaugh


.EXAMPLE

PS> Get-FSSqlPartitionInfo -FullInstanceName 'EDR1SQL01S003.fs.local\DBA'

#>
    [CmdletBinding()]
    Param (
        [parameter(ValueFromPipeline=$false, Mandatory=$true)]
        [string] 
        $FullInstanceName,

        [Parameter(Mandatory=$true)]
        [string]
        $Database
    )

    #   1) Connect to the FSDeployDB database and get the list of registered deployment directories
    #
    Begin {

        $PtFunc = @{};

        $sqlGetPt2 = @"
"@;
    }


    Process {
        $rsGetPtInfo = Invoke-DbaQuery -query $sqlGetPt2 -SqlInstance $FullInstanceName -Database $Database

        foreach ($rowPtInfo in $rsGetPtInfo) {
            if ($PtFunc[$rowPtInfo.PtFunc] -eq $null) {
                $ptb = ConvertFrom-Json $rowPtInfo.PTB
                $ptfCfg = ConvertFrom-Json $rowPtInfo.PFConfig
                $PtFunc[$rowPtInfo.PtFunc] = New-Object PSObject -Property @{
                    FuncName = $rowPtInfo.PtFunc
                    Config   = $ptfCfg.Cfg
                    DataType = $rowPtInfo.Datatype
                    RR       = $rowPtInfo.RR
                    Fanout   = $rowPtInfo.fanout
                    BndList  = $ptb.PTB
                    Schemes  = @{}
                }
            }
            $curPtFunc = $PtFunc[$rowPtInfo.PtFunc];

            if ($curPtFunc.Schemes[$rowPtInfo.PtScheme] -eq $null) {
                $fgList = ConvertFrom-Json $rowPtInfo.FGList
                $ptsCfg = ConvertFrom-Json $rowPtInfo.PSConfig
                $curPtFunc.Schemes[$rowPtInfo.PtScheme] = New-Object PSObject -Property @{
                    SchemeName = $rowPtInfo.PtScheme
                    Config     = $ptsCfg.Cfg.Cfg
                    FGList     = $fgList.FGList
                    Indexes    = @{}
                }
            }
            $curPtScheme = $curPtFunc.Schemes[$rowPtInfo.PtScheme];
            
            if ($rowPtInfo.ObjectId -ne [System.DBNull]::Value) {
                if ($curPtScheme.Indexes[$rowPtInfo.IndexName] -eq $null) {
                    $rows = ConvertFrom-JSON $rowPtInfo.Rows
                    $curPtScheme.Indexes[$rowPtInfo.IndexName] = New-Object PSObject -Property @{
                        TableSchema = $rowPtInfo.TableSchema
                        TableName   = $rowPtInfo.TableName
                        IndexName   = $rowPtInfo.IndexName
                        IndexID     = $rowPtInfo.IndexID
                        IndexType   = $rowPtInfo.IndexType
                        Config      = $rowPtInfo.TabConfig
                        TableObjID  = $rowPtInfo.TableObjID
                        PtColumn    = $rowPtInfo.PtColumn
                        Rows        = $rows.Rows
                    }

                }
            }
        }
    }


    End {
        #$rsGetPtInfo | FT
        Write-Output $PtFunc
    }
}

if (-not $FSDeploymentIsLoading){
    #$ptInfo = Get-FSSSqlPartitionInfo -FullInstanceName "PBG1SQL01V105.fs.local" -Database "ProcessData"

    #$ptInfo = Get-FSSSqlPartitionInfo -FullInstanceName "EDR1SQL01S341.fs.local\SLT" -Database "StagingEDW"

    #$ptInfo | ft

    #$ptInfo.PtFunc[0]
}