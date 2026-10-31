param([string]$MavenCommand='mvn.cmd',[switch]$Offline,[switch]$Minimal)
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$fixture=Join-Path ([IO.Path]::GetTempPath()) ('apex-sdk-consumer-'+[Guid]::NewGuid().ToString('N'))
if([IO.Path]::GetFullPath($fixture).StartsWith($root,[StringComparison]::OrdinalIgnoreCase)){throw 'Consumer must be outside the repository.'}
$evidence=Join-Path $root ('out/m35-6/external/'+[DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))
New-Item -ItemType Directory -Path $fixture,$evidence -Force|Out-Null
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'external-consumer/pom.xml') -Destination $fixture
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'external-consumer/src') -Destination $fixture -Recurse
$saved=@{}
Get-ChildItem Env:|Where-Object {$_.Name -match '^(SPRING_|APEX_|CELL_|IDENTITY_|ORDER_|INVENTORY_|PAYMENT_|KAFKA_|EUREKA_|KUBERNETES_|SERVER_|JAVA_TOOL_OPTIONS$|JDK_JAVA_OPTIONS$|_JAVA_OPTIONS$|MAVEN_OPTS$|MAVEN_ARGS$)'}|ForEach-Object {$saved[$_.Name]=$_.Value;Remove-Item -LiteralPath "Env:$($_.Name)"}
Push-Location $fixture
try {
    if(-not $env:JAVA_HOME){throw 'Set JAVA_HOME to Java 21.'}
    $options=@('-B','-ntp');if($Offline){$options+='-o'}
    $receipts=@()
    $modes=@('default','http');if(-not $Minimal){$modes+='capabilities'}
    foreach($mode in $modes) {
        $profileArgs=@();if($mode -ne 'default'){$profileArgs+= "-P$mode"}
        if($mode -eq 'capabilities'){$profileArgs+="-Dexternal.registry.jar=$root/out/m34-artifacts/apex-discovery-server.jar"}
        & $MavenCommand @options @profileArgs clean verify *> (Join-Path $evidence "$mode-build.log")
        if($LASTEXITCODE -ne 0){throw "External $mode build failed; see $evidence"}
        New-Item -ItemType Directory -Path (Join-Path $evidence $mode) -Force|Out-Null
        Copy-Item target/surefire-reports/TEST-*.xml (Join-Path $evidence $mode)
        & $MavenCommand @options @profileArgs dependency:tree '-Dscope=runtime' "-DoutputFile=$evidence/$mode-tree.txt" *> (Join-Path $evidence "$mode-dependencies.log")
        if($LASTEXITCODE -ne 0){throw 'Dependency audit failed.'}
        $tree=Get-Content (Join-Path $evidence "$mode-tree.txt") -Raw
        if($tree -match '(apex-platform-context-autoconfigure|apex-platform-webmvc|apex-(api-gateway|discovery-server|identity-server|bootstrap|cell-template|order-contracts|replay-tool)|com.apex:(order|inventory|payment|customer|reporting)-cell|oauth2-authorization-server)'){throw 'Executable or removed artifact leaked into consumer.'}
        if($mode -ne 'capabilities' -and $tree -match '(spring-kafka|kafka-clients|spring-jdbc|grpc-netty|eureka|fabric8)'){throw 'Unselected capability leaked into consumer.'}
        if($mode -eq 'capabilities'){
            $receipts+=[ordered]@{mode=$mode;jarSha256=(Get-FileHash target/sdk-consumer.jar -Algorithm SHA256).Hash;bootExit='In JUnit: owned PostgreSQL/Kafka and real gRPC';testExit=0}
            continue
        }
        $args=@('-jar','target/sdk-consumer.jar','--spring.config.location=optional:classpath:/absent.properties','--spring.main.banner-mode=off','--spring.application.name=external-consumer','--apex.cell.id=external-consumer')
        if($mode -eq 'default'){$args+='--spring.main.web-application-type=none'}else{$args+=@('--server.address=127.0.0.1','--server.port=0')}
        & (Join-Path $env:JAVA_HOME 'bin/java') @args *> (Join-Path $evidence "$mode-boot.log")
        if($LASTEXITCODE -ne 0 -or -not (Select-String -LiteralPath (Join-Path $evidence "$mode-boot.log") -Pattern 'SDK_CONSUMER_ACCEPTED')){throw "External $mode packaged boot failed."}
        $receipts+=[ordered]@{mode=$mode;jarSha256=(Get-FileHash target/sdk-consumer.jar -Algorithm SHA256).Hash;bootExit=0;testExit=0}
    }
    [ordered]@{outsideRepository=$fixture;artifactReceipts=$receipts;completedUtc=[DateTimeOffset]::UtcNow.ToString('o')}|ConvertTo-Json -Depth 5|Set-Content (Join-Path $evidence 'receipt.json')
    Write-Host "External consumer acceptance PASS: $evidence"
} finally {Pop-Location;foreach($name in $saved.Keys){Set-Item -LiteralPath "Env:$name" -Value $saved[$name]}}
exit 0
