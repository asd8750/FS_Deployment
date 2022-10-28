
function Set-FSSqlPtManager {

    #
    #
    #   Revision History:
    #   2020-12-17 - F. LaForest - Initial version
    #
    #   Retain, Width and PreAlloc "Units" values
    #       - day  - one or more multiples of one 24-hour day, 00:00 - 23:59 (not month aligned)
    #       - mday - one or more multiple of one 24-hour day, 00:00 - 23:59 (a multiple day period will end at the month boundary)
    #       - month - multiple of a calendar month, aligned to calendar month
    #       - week  - roughly 1 calendar week or 1/4 of a calendar month (7-8 days per week), aligned to calendar month
    #

    [CmdletBinding(DefaultParameterSetName = "Global")]
    param(
        [Parameter(Mandatory=$True)][string] $InstanceName,
        [Parameter(Mandatory=$True)][string] $Database,

        [Parameter(Mandatory=$True, ParameterSetName="Global")]
        [Parameter(Mandatory=$false, ParameterSetName="Function")]
        [Parameter(Mandatory=$false, ParameterSetName="Table")]
        [string] $Mode,     

        [Parameter(Mandatory=$false, ParameterSetName="Global")]
        [Parameter(Mandatory=$false, ParameterSetName="Scheme")]
        [string] $FGList,     

        [Parameter(Mandatory=$True, ParameterSetName="Function")][string] $Function,

        [Parameter(Mandatory=$True, ParameterSetName="Scheme")][string] $Scheme,

        [Parameter(Mandatory=$True, ParameterSetName="Table")][string] $TableName,
        [Parameter(Mandatory=$False, ParameterSetName="Table")][string] $IndexName,

        [Parameter(Mandatory=$false, ParameterSetName="Function")]
        [string] $PreAlloc = $null,

        [Parameter(Mandatory=$false, ParameterSetName="Function")]
        [Parameter(Mandatory=$false, ParameterSetName="Table")]
        [string] $Retain = $null,

        [Parameter(Mandatory=$false, ParameterSetName="Function")]
        [string] $Width = $null,

        # [Parameter(Mandatory=$false, ParameterSetName="Tags")]
        # [Parameter(Mandatory=$false, ParameterSetName="Function")]
        # [Parameter(Mandatory=$false, ParameterSetName="Table")]
        [Parameter(Mandatory=$false)]
        [string] $Tags = $null,

        [Parameter(Mandatory=$False)][switch] $Reset,

        [Parameter(Mandatory=$False)][switch] $WhatIf,
        [Parameter(Mandatory=$False)][switch] $Force
    )

    
    #
    #   Local function -- ParseTimePeriod   Parse the user supplied time period specification into a
    #       PowerShell object
    #
    function ParseTimePeriod {
        param (
            [Parameter(Mandatory=$true)] [string] $RawCfg, 
            [Parameter(Mandatory=$true)] [string] $Label
        )

        $local:optValue = New-Object -TypeName PSCustomObject -Property @{
            Value = "1"
            Units = "month"
            Raw   = $rawCfg
        }

        if (-not ($rawCfg -imatch "^\s*(?<num>\d+)\s*(?<units>(day|mday|week|month))?s?\s*$")) {
            Write-error -Message "Unknown $($Label) duration spec: '$($rawCfg)'"
            $errCnt += 1
            return $null
        }
        else {
            $num = $Matches["num"]
            if (([string]::IsNullOrEmpty($num)) -or ($num -le 0)) {
                Write-error -Message "Unknown $($Label) units specification: '$($rawCfg)'"
                $errCnt += 1    
                return $null
            }
            else {
                if (-not [string]::IsNullOrEmpty($Matches["units"])) { $optValue.Units = $Matches["units"].ToLower() }
                $optValue.Value = $num
            }
        }
        return $optValue
    }

    #
    #   The start of the functional code
    #

        #   Include needed modules
        #
        if ((Get-Module | Where-Object { $_.Name -ilike "*FS_Deployment*" }).Count -eq 0) {
            Write-Verbose "Importing module: 'FSDeployment'"
            #Import-Module -Name C:\Projects\DBA-Deployments\FS_Deployment
        }

        #   Now collect the partitioning information for this database
        #
        $CacheName = "Cache_$($MyInvocation.InvocationName)_Inst_$($Database)"
        $CacheExpire = "$($CacheName)_Expire)"

        #   Check for local cache expiration to save processing time
        #
        if (Get-Variable -Name $CacheExpire -Scope Global -ErrorAction SilentlyContinue){ 
            $cacheExpireTime = [Datetime]::Parse((Get-Variable -Name $CacheExpire -ValueOnly -Scope Global))
            if ($cacheExpireTime -lt [DateTime]::Now) {
                Remove-Variable -Name $CacheName -Scope Global -ErrorAction SilentlyContinue
                Write-Verbose "Expire Cache: $($CacheName)"
                Set-Variable -Name $CacheExpire -Value ([Datetime]::Now.AddMinutes(1)) -Visibility Public -Scope Global
            }
        }
        else {           
            Set-Variable -Name $CacheExpire -Value ([Datetime]::Now.AddMinutes(1)) -Visibility Public -Scope Global
        }

        if (Get-Variable -Name $CacheName -ErrorAction SilentlyContinue){
            $ptInfo = Get-Variable -Name $CacheName -ValueOnly -Scope Global
            Write-Verbose "Using Cache: $($CacheName)"
        }
        else {
            try {
            $ptInfo = Get-FSSqlPartitionInfo -InstanceName $InstanceName -Database $Database
            Write-Verbose "Create Cache: $($CacheName)"
            Set-Variable -Name $CacheName -Value $ptInfo -Visibility Public -Scope Global  
            }
            catch {
                Write-Verbose "Cannot create Cache: $($CacheName)"
            }
        }

        #   Perform further initalization based on the command format 
        #
        $optsCur = @()    # Initialize Opts collections to empty
        switch ($PSCmdlet.ParameterSetName) {
            "Global" {
                $validOpts = @("Mode", "FGList", "Tags")
                $optsDef = ConvertFrom-Json '[{"Mode":"Enabled"},{"FGList": [{"FGName":  "PRIMARY", "StartDate": "\/Date(315550800000)\/"}] }]' # Default opts
                if ($PtInfo.DbConfig -and -not $Reset) {
                    $optsCur = ConvertFrom-Json $PtInfo.DBConfig
                }
            }

            "Function" {
                $validOpts = @("Mode", "Width", "Retain", "PreAlloc", "Tags")
                $optsDef = ConvertFrom-Json '[{"Mode":"Enabled"}]' # Default opts
                $PfInfo = $PtInfo.GetFunction( $Function )
                if ($PfInfo -and $PfInfo.Config -and -not $Reset) {
                    $optsCur = ConvertFrom-Json $PfInfo.Config
                }
            }

            "Scheme" {
                $validOpts = @("FGList")
                $optsDef = $null  # Default opts         
                $PsInfo = $PtInfo.GetScheme( $Scheme )
                if ($PsInfo -and $PsInfo.Config -and -not $Reset) {
                    $optsCur = ConvertFrom-Json $PsInfo.Config
                }
            }

            "Table" {
                $validOpts = @("Retain", "Tags", "Truncate")
                $optsDef = $null  # Default opts

                $TblSchema = $TableName.Split('.')[0] 
                $TblName   = $TableName.Split('.')[1] 

                $TblInfo = $PtInfo.GetTable( $TblSchema, $TblName ) 
                if ($TblInfo = $null) {
                    throw "Unknown table: '$($TblSchema).$($TblName)'"
                }
                if ($TblInfo.Config -and -not $Reset) {
                    $optsCur = ConvertFrom-Json $TblOpts
                }
            }
        }

        #   Now place any option settings into the $optsNew collection
        #
        $global:optsNew = @();
        $validOpts | Select-Object {
            switch ($_) {
                "PreAlloc" {
                    if ($PreAlloc) {
                        $opt = [PSCustomObject]@{"PreAlloc" = (ParseTimePeriod -RawCfg $PreAlloc -Label $_ )}
                        #$global:optsNew += $opt    
                    }            
                }

                "Retain" { 
                    if ($Retain) {
                        $opt = [PSCustomObject]@{"Retain" = (ParseTimePeriod -RawCfg $Retain -Label $_ )}
                        #$global:optsNew += $opt
                    }                    
                }

                "Width" {
                    if ($Width) {
                        $opt = [PSCustomObject]@{"Width" = (ParseTimePeriod -RawCfg $Width -Label $_ )}
                        #$global:optsNew += $opt       
                    }                    
                }

                "Tags" {
                    if ($Tags) {
                        $opt = [PSCustomObject]@{"Tags" = $Tags.Split(',')}
                        #$global:optsNew += $opt       
                    }                    
                }

                "Mode" {
                    if ($Mode) {
                        $opt = [PSCustomObject]@{"Mode" = $Mode}
                        #$global:optsNew += $opt     
                    }                    
                }

                "FGList" {
                    if ($FGList) {
                        $opt = [PSCustomObject]@{"FGList" = $FGList}
                    }                    
                }

                default {
                    Write-Error "Unknown parameter: '$($_)'"
                    continue;
                }
            }
            if ($opt) { $global:optsNew += $opt }               # Add non-null option to the $optsNew collection
        }


        #   Now validate the various options set in the command.  There are 3 option sets being collected; Default, Current and New. 
        #
        #   Look for unknown opts in $optsNew
        foreach ($nopt in $optsNew) {
            $nName = $nopt.psobject.properties.Name  # Get the new opt name
            $vopt = $validOpts | Where-Object { $_ -ieq $nName }
            if ( -not $vopt) {
                Write-Verbose "Unknown New Option: '$($nOpt)'"
            }
        }

        #   Look for unknown opts in $optsCur
        foreach ($nopt in $optsCur) {
            $nName = $nopt.psobject.properties.Name  # Get the new opt name
            $vopt = $validOpts | Where-Object { $_ -ieq $nName }
            if ( -not $vopt) {
                Write-Verbose "Unknown Current Option: '$($nOpt)'"
            }
        }

        #   Now Determine which options to apply or override    
        $opts = @();
        foreach ($optName in $validOpts) {

            $optValueDef = (($optsDef | Where-Object { $_.psobject.properties.name -ieq $optName }).$optName | Select -first 1 )
            $optValueCur = (($optsCur | Where-Object { $_.psobject.properties.name -ieq $optName }).$optName | Select -first 1 )
            $optValueNew = (($optsNew | Where-Object { $_.psobject.properties.name -ieq $optName }).$optName | Select -first 1 )
          
            if (-not $optValueNew) {
                if ($optValueCur) {
                    $optValueNew = $optValueCur
                } 
                else {
                    $optValueNew = $optValueDef
                }
            }
            if ($optValueNew) { 
                $opts += ( Select-Object @{n=$optName;e={$optValueNew}} -InputObject '' )
            }
        }

        #   Validate the selected options
        $errCnt = 0
        $opts2 = @();
        foreach ($opt in $opts) {
            $optName  = $opt.psobject.properties.name
            $optValue = $opt.psobject.properties.value

            switch ($optName) {
                "Mode" {
                    if (-not ($optValue -match "^(Disabled|Enabled|Monitor)$")) {
                        Write-error -Message "Unknown Mode option: '$($optValue)'"
                        $errCnt += 1
                    }
                }

                "Retain" {
                    if ($optValue.GetType().Name -ieq "string") {
                        $duration = ParseTimePeriod -RawCfg $optValue -Label "Retain"
                        $optValue = $duration                        
                    }
                }

                "PreAlloc" {
                    if ($optValue.GetType().Name -ieq "string") {
                        $duration = ParseTimePeriod -RawCfg $optValue -Label "PreAlloc"
                        $optValue = $duration                        
                    }
                }

                "Width" {
                    if ($optValue.GetType().Name -ieq "string") {
                        $duration = ParseTimePeriod -RawCfg $optValue -Label "Retain"
                        $optValue = $duration                        
                    }
                }

                "FGList" {                  
                }
            }
            $opts2 += ( Select-Object @{n=$optName;e={$optValue}} -InputObject '' )
        }

        #   Build the TSQL to change the Extended Properties
        if ($opts2.count -gt 0) {
            $newJson = ("{`"Cfg`":" + (ConvertTo-Json $opts2) + "}") -replace "\s+", " " 
        }
        else {
            $newJson = ''
        }
        
        #   Construct the extended property updating TSQL
        #    
        switch ($PSCmdlet.ParameterSetName) {
            "Global" {
                $sqlDropProperty = "EXEC sp_dropextendedproperty @name = 'FSPtManager';"
                $sqlAddProperty  = "EXEC sp_addextendedproperty  @name = 'FSPtManager', @level0type = NULL, @value = '$($newJson)';"
            }

            "Function" {
                $sqlDropProperty = "EXEC sp_dropextendedproperty @name = 'FSPtManager', @level0type = 'PARTITION FUNCTION', @level0name = '$($Function)';"
                $sqlAddProperty  = "EXEC sp_addextendedproperty  @name = 'FSPtManager', @level0type = 'PARTITION FUNCTION', @level0name = '$($Function)', @value = '$($newJson)';"
            }

            "Scheme" {
                $sqlDropProperty = "EXEC sp_dropextendedproperty @name = 'FSPtManager', @level0type = 'PARTITION SCHEME', @level0name = '$($Scheme)';"
                $sqlAddProperty  = "EXEC sp_addextendedproperty  @name = 'FSPtManager', @level0type = 'PARTITION SCHEME', @level0name = '$($Scheme)', @value = '$($newJson)';"
            }

            "Table" {
                $sqlDropProperty = "EXEC sp_dropextendedproperty @name = 'FSPtManager', @level0type = 'SCHEMA', @level0name = '$($TblSchema)', @level1type = 'TABLE', @level1name = '$($TblName)';"
                $sqlAddProperty  = "EXEC sp_addextendedproperty  @name = 'FSPtManager', @level0type = 'SCHEMA', @level0name = '$($TblSchema)', @level1type = 'TABLE', @level1name = '$($TblName)', @value = '$($newJson)';"
            }
        }

        # switch ($setCommand) {
        #     "Database" {
        #     }

        #     "Function" {
        #     }

        #     "Scheme" {
        #     }

        #     "Table" {
        #     }
        # }

        #   Submit the update commands
        #
        try {
            #   Drop existing extended property
            if (-not [string]::IsNullOrEmpty($sqlDropProperty)) { Write-Verbose "XP Drop: $($sqlDropProperty)" }
            if (-not $WhatIf) { Invoke-Sqlcmd -ServerInstance $InstanceName -Database $Database -Query $sqlDropProperty -ErrorAction SilentlyContinue }

            #   Add the new extended property
            if (-not [string]::IsNullOrEmpty($sqlAddProperty)) { Write-Verbose "XP Add:  $($sqlAddProperty)" }
            if (-not $WhatIf) { Invoke-Sqlcmd -ServerInstance $InstanceName -Database $Database -Query $sqlAddProperty }       
        }
        catch {
            throw $_
        }
    #}

    #End {
        #Write-Verbose "Valid Options: $($validOpts)"
        if (-not [string]::IsNullOrEmpty($optsCur)) { Write-Verbose "Current Options: $($optsCur)" }
        if (-not [string]::IsNullOrEmpty($optsNew)) { Write-Verbose "New Options: $($optsNew)" }
    #}
}



# Test Commands
if (-not $FSDeploymentIsLoading){
    Set-FSSqlPtManager -InstanceName "PBG1SQL01V105.fs.local" -Database "ProcessData" -Mode "Enabled" -Reset -Whatif -Verbose

    Set-FSSqlPtManager -InstanceName "PBG1SQL01V105.fs.local" -Database "ProcessData" -Function "PtFunc_Vision_V1" `
        -Mode "Enabled"  -Width "1 weeK  " -Retain "3 mOnths" -PreAlloc "30" -Verbose -WhatIf -Tags "a,-b"

    Set-FSSqlPtManager -InstanceName "PBG1SQL01V105.fs.local" -Database "ProcessData" -Function "PtFunc_Vision_V1" `
                    -PfOpts '[{"Mode": "Enabled"}]' -Verbose -WhatIf

    Set-FSSqlPtManager -InstanceName "PBG1SQL01V105.fs.local" -Database "ProcessData" -Scheme "PtSch_Vision_V1" `
                    -PsOpts '[{"FGList":"PRIMARY,2020/01/01,PRIMARY"}]' -Verbose -WhatIf

    
    Set-FSSqlPtManager -InstanceName "PBG1SQL01V105.fs.local" -Database "ProcessData" -TableName "Vision.Defect" `
                    -TblOpts '[{"Retain":"4 months"}, {"Tag":"MESExtract"}]' -Verbose -WhatIf
}