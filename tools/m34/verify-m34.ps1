param([string]$MavenCommand='mvn.cmd',[switch]$Offline,[switch]$ContinueAfterKnownDeadlineFailure,
      [ValidateSet('All','Build','VM')][string]$Phase='All')
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$saved=@{}
Get-ChildItem Env: | Where-Object {$_.Name -match '^(SPRING_|APEX_|CELL_|ORDER_|INVENTORY_|PAYMENT_|KAFKA_|EUREKA_|KUBERNETES_|SERVER_|JAVA_TOOL_OPTIONS$|JDK_JAVA_OPTIONS$|_JAVA_OPTIONS$|MAVEN_OPTS$)'} |
    ForEach-Object {$saved[$_.Name]=$_.Value;Remove-Item -LiteralPath "Env:$($_.Name)"}
Push-Location $root
try {
    if(-not $env:JAVA_HOME){throw 'Set JAVA_HOME to Java 21.'}
    $version=& (Join-Path $env:JAVA_HOME 'bin/java') --version | Out-String
    if($LASTEXITCODE -ne 0 -or $version -notmatch '(?m)^(java|openjdk) 21[\. ]'){throw 'Java 21 required.'}
    $options=@('-B','-ntp');if($Offline){$options+='-o'}
    function Maven([string[]]$goals){ & $MavenCommand @options @goals;if($LASTEXITCODE -ne 0){throw "M34 Maven failure exit $LASTEXITCODE"} }
    $modules=@('apex-platform-core','apex-platform-context-autoconfigure','apex-platform-events',
        'apex-platform-messaging','apex-platform-webmvc','apex-platform-grpc-api','apex-platform-grpc',
        'apex-starter-webmvc','apex-starter-cell-messaging','apex-starter-grpc-client','apex-starter-grpc-server',
        'apex-order-contracts','apex-platform-discovery','apex-discovery-eureka','apex-discovery-kubernetes',
        'apex-discovery-grpc','order-cell','inventory-cell','payment-cell','sample-grpc-client',
        'apex-api-gateway','apex-discovery-server')
    function Inputs {
        $paths=@('pom.xml','tools/m34/verify-m34.ps1')
        foreach($module in $modules){
            $paths+="$module/pom.xml"
            $paths+=@(Get-ChildItem "$module/src/main" -Recurse -File -ErrorAction SilentlyContinue | ForEach-Object {$_.FullName.Substring($root.Length+1).Replace('\','/')})
        }
        @(foreach($path in ($paths | Sort-Object -Unique)){[pscustomobject]@{path=$path;sha256=(Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()}})
    }
    function LegacyFingerprint {
        $legacyModules=@('apex-platform-core','apex-platform-events','apex-platform-context-autoconfigure',
            'apex-platform-webmvc','apex-platform-messaging','apex-starter-webmvc','apex-starter-cell-messaging','order-cell')
        $paths=[Collections.Generic.List[string]]::new();$paths.Add('pom.xml');$paths.Add('inventory-cell/tools/verify-saga.ps1')
        foreach($module in $legacyModules){$paths.Add("$module/pom.xml");Get-ChildItem "$module/src/main" -Recurse -File -ErrorAction SilentlyContinue | ForEach-Object {$paths.Add($_.FullName.Substring($root.Length+1).Replace('\','/'))}}
        $ordered=$paths.ToArray();[Array]::Sort($ordered,[StringComparer]::Ordinal);$text=[Text.StringBuilder]::new()
        foreach($path in $ordered){[void]$text.Append($path).Append("`n").Append((Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()).Append("`n")}
        $sha=[Security.Cryptography.SHA256]::Create()
        try{([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($text.ToString())))).Replace('-','').ToLowerInvariant()}finally{$sha.Dispose()}
    }
    if($Phase -ne 'VM') {
        $started=[DateTimeOffset]::UtcNow.ToString('o')
        $before=Inputs;$legacyBefore=LegacyFingerprint
        try {Maven @('-pl','apex-discovery-eureka,apex-discovery-kubernetes,apex-discovery-grpc','-am','clean','install')}
        catch {
            if(-not $ContinueAfterKnownDeadlineFailure){throw}
            $failed=@(Get-ChildItem apex-platform-grpc/target/surefire-reports/TEST-*.xml | ForEach-Object {
                [xml]$xml=Get-Content -LiteralPath $_.FullName
                @($xml.testsuite.testcase | Where-Object {$_.error -or $_.failure})
            })
            if($failed.Count -ne 1 -or $failed[0].classname -ne 'com.apex.platform.grpc.internal.CollectorDeadlineIntegrationTest' -or
                $failed[0].name -ne 'repeatedNativeDeadlinesNeverBlockTheSharedDeadlineScheduler'){throw}
            $productionDiff=git diff --name-only HEAD -- apex-platform-grpc/src/main apex-platform-grpc/pom.xml apex-platform-grpc-api apex-starter-grpc-client
            if($productionDiff){throw 'Cannot qualify independent build with changed legacy gRPC production inputs.'}
            $archive='out/m34-known-deadline-'+[DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss')
            New-Item -ItemType Directory $archive -Force | Out-Null
            Copy-Item apex-platform-grpc/target/surefire-reports/TEST-*.xml $archive
            Write-Warning 'Legacy deadline regression FAILED. Continue only independent M34; this is NOT a global regression PASS. Previously installed unchanged M31 artifacts are required.'
            Maven @('-pl','apex-platform-discovery,apex-discovery-eureka,apex-discovery-kubernetes,apex-discovery-grpc','clean','install')
        }
        Maven @('-f','order-cell/pom.xml','-Pdiscovery-eureka','clean','verify')
        if((LegacyFingerprint) -ne $legacyBefore){throw 'Verified Order/Platform inputs changed during build.'}
        $receipt=@('schemaVersion=1',"sourceSha256=$legacyBefore",'buildProfile=discovery-eureka')
        foreach($module in @('apex-platform-core','apex-platform-events','apex-platform-context-autoconfigure','apex-platform-webmvc','apex-platform-messaging','apex-starter-webmvc','apex-starter-cell-messaging','order-cell')) {
            $jar=if($module -eq 'order-cell'){'order-cell/target/order-cell.jar'}else{"$module/target/$module-1.0-SNAPSHOT.jar"}
            $receipt+="$module.sha256=$((Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash.ToLowerInvariant())"
        }
        New-Item -ItemType Directory -Path out -Force | Out-Null
        [IO.File]::WriteAllLines((Join-Path $root 'out/m26-order-build.properties'),$receipt,[Text.UTF8Encoding]::new($false))
        Maven @('-f','apex-replay-tool/pom.xml','clean','verify')
        foreach($cell in @('inventory-cell','payment-cell','sample-grpc-client')) {
            Maven @('-f',"$cell/pom.xml",'-Pdiscovery-eureka','clean','verify')
        }
        Maven @('-f','apex-discovery-server/pom.xml','clean','verify')
        Maven @('-f','apex-api-gateway/pom.xml','-Peureka','clean','verify')
        New-Item -ItemType Directory -Path out/m34-artifacts -Force | Out-Null
        $files=Inputs
        if(($files|ConvertTo-Json -Compress) -ne ($before|ConvertTo-Json -Compress)){throw 'M34 inputs changed during verified builds.'}
        foreach($app in @('order-cell','inventory-cell','payment-cell','sample-grpc-client','apex-api-gateway','apex-discovery-server')) {
            $path="out/m34-artifacts/$app.jar"
            Copy-Item -LiteralPath "$app/target/$app.jar" -Destination $path -Force
            $files+=[pscustomobject]@{path=$path;sha256=(Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()}
        }
        [IO.File]::WriteAllText((Join-Path $root 'out/m34-build-receipt.json'),
            ([pscustomobject]@{startedUtc=$started;files=$files}|ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))
    }
    if($Phase -ne 'Build') {
        try {Maven @('-f','order-cell/pom.xml','-Pm34-acceptance,discovery-eureka','test-compile','surefire:test@dual-discovery-replicas')}
        finally {
            New-Item -ItemType Directory -Path out/m34-acceptance -Force | Out-Null
            if(Test-Path order-cell/target/m34-reports){Copy-Item order-cell/target/m34-reports/TEST-*.xml out/m34-acceptance -Force}
            if(Test-Path order-cell/target/m34-evidence/acceptance.json){Copy-Item order-cell/target/m34-evidence/acceptance.json out/m34-acceptance -Force}
        }
    }
    if($Phase -eq 'All') {
        $legacy=@('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/m32/verify-m32.ps1','-MavenCommand',$MavenCommand)
        if($Offline){$legacy+='-Offline'}
        $gitIndex=[int]$env:GIT_CONFIG_COUNT
        $oldCount=$env:GIT_CONFIG_COUNT
        try {
            # Suppress CRLF conversion warnings only; retain actual whitespace validation.
            $env:GIT_CONFIG_COUNT=[string]($gitIndex+1)
            Set-Item "Env:GIT_CONFIG_KEY_$gitIndex" 'core.safecrlf';Set-Item "Env:GIT_CONFIG_VALUE_$gitIndex" 'false'
            & powershell.exe @legacy
            if($LASTEXITCODE -ne 0){throw 'M34 independent evidence retained; complete prior M32 regression failed.'}
        } finally {
            $env:GIT_CONFIG_COUNT=$oldCount
            Remove-Item "Env:GIT_CONFIG_KEY_$gitIndex","Env:GIT_CONFIG_VALUE_$gitIndex"
        }
    }
    git -c core.safecrlf=false diff --check;if($LASTEXITCODE -ne 0){throw 'Whitespace check failed.'}
} finally {Pop-Location;foreach($name in $saved.Keys){Set-Item -LiteralPath "Env:$name" -Value $saved[$name]}}
exit 0
