
# =========================================================================
function New-FSDatabase {
    <#
.SYNOPSIS

Create a blank database with filegraoups and files pattered after the source DB

.DESCRIPTION

Version History
- 2022-09-12 - 1.0 - F.LaForest - Initial version


.PARAMETER PlantCode

.PARAMETER SourceInstance

.PARAMETER SourceDatabase


.INPUTS

.OUTPUTS


.EXAMPLE


#>
    [CmdletBinding()]
    Param (
        [parameter(Mandatory=$true)]
        [string] 
        $PlantCode,

        [parameter(Mandatory=$true)]
        [string] 
        $SourceInstance,

        [parameter(Mandatory=$true)]
        [string] 
        $SourceDatabase,

        [parameter(Mandatory=$true)]
        [string] 
        $DstInstance,

        [Parameter(Mandatory=$false)]
        [int32]
        $DebugLevel = 3
    )

    #   Loop through each supplied SQL instance name.. Querying each one to find the current state
    #
    begin {

        $ErrorCnt = 0

        # Make sure the plantcode fits the template
        if ($PlantCode -match '^[A-Z]{3}[1-9]$') {
            $a=3
        }
        $newDbName = "$($SourceDatabase)_$($PlantCode)"

        #   Get the file makeup of the selected database
        #
        $sqlDbFiles = "
            SELECT  ISNULL(FG.[name], 'LOG') AS FGName,
                    DF.[name] AS LogicalFile,
                    DF.physical_name,
                    DF.file_id,
                    DF.type_desc,
                    FG.is_default AS IsDefaultFG                
                FROM sys.database_files DF
                    LEFT OUTER JOIN sys.filegroups FG
                        ON (DF.data_space_id = FG.data_space_id)
        "
        $DbFiles = Invoke-Sqlcmd -ServerInstance $SourceInstance -Database $SourceDatabase -Query $sqlDbFiles

        $FL_Primary = $null
        $FL_FG = @{}
        $FL_LOG = $null
        foreach ($fl in ($DbFiles | Sort-Object FGID,file_id)) {

            $newFilePath = $fl.physical_name.SUBSTRING(0,$fl.physical_name.length-4) + "_$($PlantCode)" + $fl.physical_name.SUBSTRING($fl.physical_name.length-4,4)
            $FileSpec = `
            "    (NAME = N'$($fl.LogicalFile)',
                FILENAME = N'$($newFilePath)',
                SIZE = 1024KB,
                FILEGROWTH = 1024KB )
            "
            if ($fl.type_desc -eq 'ROWS') {
                if ($fl.FGName -eq 'PRIMARY') {
                    if ($FL_Primary) { $FL_Primary = $FL_Primary + ', '}
                    $FL_Primary = $FL_Primary + $FileSpec
                } else {
                    if (-not $FL_FG[$fl.FGName]) {
                        $FL_FG[$fl.FGName] = $FileSpec
                    } else {
                        $FL_FG[$fl.FGName] = $FL_FG[$fl.FGName] + ", " + $FileSpec
                    }
                }
            } else {
                if ($FL_LOG) {
                    $FL_LOG = $FL_LOG + ", "
                }
                $FL_LOG = $FL_LOG + $FileSpec
            }
        }


        #   Now build the SQL command needed to create the database.

        $sqlCreateDB = "  CREATE DATABASE [$($newDBName)] CONTAINMENT = NONE `r";

        if ($FL_Primary) {
            $sqlCreateDB = $sqlCreateDB + "ON PRIMARY `r  " + $FL_Primary
        }

        foreach ($fgname in $FL_FG.Keys) {
            $sqlCreateDB = $sqlCreateDB + ", FILEGROUP [$($fgname)] $($FL_FG[$fgname])"
        }

        if ($FL_LOG) {
            $sqlCreateDB = $sqlCreateDB + "LOG ON `r  " + $FL_LOG
        }

        $sqlDBOptions1 = "
            ALTER DATABASE [$($newDBName)] SET COMPATIBILITY_LEVEL = 150;
            GO
            ALTER DATABASE [$($newDBName)] SET ANSI_NULL_DEFAULT OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET ANSI_NULLS OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET ANSI_PADDING OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET ANSI_WARNINGS OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET ARITHABORT OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET AUTO_CLOSE OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET AUTO_SHRINK OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET AUTO_CREATE_STATISTICS ON ( INCREMENTAL = OFF );
            GO
            ALTER DATABASE [$($newDBName)] SET AUTO_UPDATE_STATISTICS ON;
            GO
            ALTER DATABASE [$($newDBName)] SET CURSOR_CLOSE_ON_COMMIT OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET CURSOR_DEFAULT GLOBAL;
            GO
            ALTER DATABASE [$($newDBName)] SET CONCAT_NULL_YIELDS_NULL OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET NUMERIC_ROUNDABORT OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET QUOTED_IDENTIFIER OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET RECURSIVE_TRIGGERS OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET DISABLE_BROKER;
            GO
            ALTER DATABASE [$($newDBName)] SET AUTO_UPDATE_STATISTICS_ASYNC OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET DATE_CORRELATION_OPTIMIZATION OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET PARAMETERIZATION SIMPLE;
            GO
            ALTER DATABASE [$($newDBName)] SET READ_COMMITTED_SNAPSHOT OFF;
            GO
            ALTER DATABASE [$($newDBName)] SET READ_WRITE;
            GO
            ALTER DATABASE [$($newDBName)] SET RECOVERY FULL;
            GO
            ALTER DATABASE [$($newDBName)] SET MULTI_USER;
            GO
            ALTER DATABASE [$($newDBName)] SET PAGE_VERIFY CHECKSUM;
            GO
            ALTER DATABASE [$($newDBName)] SET TARGET_RECOVERY_TIME = 60 SECONDS;
            GO
            ALTER DATABASE [$($newDBName)] SET DELAYED_DURABILITY = DISABLED;
            GO";

        $sqlList1 = $sqlDBOptions1.Split([Environment]::NewLINE) | ? { ($_ -ne "") -and (-not ( $_ -match '^\s+?GO')) }


        $sqlDBOptions2 = "
            ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
            GO
            ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET LEGACY_CARDINALITY_ESTIMATION = PRIMARY;
            GO
            ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
            GO
            ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET MAXDOP = PRIMARY;
            GO
            ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
            GO
            ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET PARAMETER_SNIFFING = PRIMARY;
            GO
            ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
            GO
            ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET QUERY_OPTIMIZER_HOTFIXES = PRIMARY;
            GO";

        $sqlList2 = $sqlDBOptions1.Split([Environment]::NewLINE) | ? { ($_ -ne "") -and (-not ( $_ -match '^\s+?GO')) }

        # Now create the database
        #
        $results = Invoke-Sqlcmd -ServerInstance $DstInstance -Database 'master' -Query $sqlCreateDB;

        foreach ($dbOpt1 in $sqlList1) {
            $results = Invoke-Sqlcmd -ServerInstance $DstInstance -Database 'master' $dbOpt1
        }

        foreach ($dbOpt2 in $sqlList2) {
            $results = Invoke-Sqlcmd -ServerInstance $DstInstance -Database $newDbName -query $dbOpt2
        }

        # $sqlFGDefault = "
        #     IF NOT EXISTS ( SELECT name FROM sys.filegroups WHERE is_default = 1 AND name = N'PRIMARY' ) ALTER DATABASE [$($newDBName)] MODIFY FILEGROUP [PRIMARY] DEFAULT;"
        # $results = Invoke-Sqlcmd -ServerInstance $DstInstance -Database $newDbName -query $sqlFGDefault 

        $results = Invoke-Sqlcmd -ServerInstance $DstInstance -Database $newDbName -query  "EXEC sys.sp_changedbowner @loginame = 'sa' --"
    }
}


if (-not $FSDeploymentIsLoading){
    New-FSDatabase -PlantCode "PMT1" -SourceInstance 'PBG1SQL01T214.fs.local' -SourceDatabase 'SPC' -DstInstance 'PBG2SQL01T214.fs.local'
    #New-FSDatabase -PlantCode "PMT1" -SourceInstance 'PBG1SQL01V001.fs.local' -SourceDatabase 'Reliability'
}