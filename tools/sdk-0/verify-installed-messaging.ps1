param([string]$MavenCommand='mvn.cmd', [switch]$Sdk01)
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$fixture=Join-Path ([IO.Path]::GetTempPath()) ('apex-sdk0-messaging-'+[Guid]::NewGuid().ToString('N'))
if([IO.Path]::GetFullPath($fixture).StartsWith($root,[StringComparison]::OrdinalIgnoreCase)){throw 'Consumer must be outside repository.'}
$evidenceRoot=if($Sdk01){'out/sdk-01'}else{'out/sdk-0'}
$evidence=Join-Path $root ($evidenceRoot+'/installed-'+[DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))
New-Item -ItemType Directory -Path $fixture,$evidence -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $root 'tools/m35-6/external-consumer/pom.xml') -Destination $fixture
Copy-Item -LiteralPath (Join-Path $root 'tools/m35-6/external-consumer/src') -Destination $fixture -Recurse
$saved=@{}
Get-ChildItem Env: | Where-Object {$_.Name -match '^(SPRING_|APEX_|CELL_|IDENTITY_|ORDER_|INVENTORY_|PAYMENT_|KAFKA_|EUREKA_|KUBERNETES_|SERVER_|JAVA_TOOL_OPTIONS$|JDK_JAVA_OPTIONS$|_JAVA_OPTIONS$|MAVEN_OPTS$|MAVEN_ARGS$)'} | ForEach-Object {$saved[$_.Name]=$_.Value;Remove-Item -LiteralPath "Env:$($_.Name)"}
Push-Location $fixture
try {
    $options=@('-o','-B','-ntp','-Pcapabilities')
    $selected='-Dtest=InstalledCapabilitiesTest#installedMessagingPublishesTransactionalOutboxToOwnedKafka+installedProvisioningReadiesIncomingAndCustomDltBeforeListenersStart'
    if($Sdk01){$selected+='+installedCollectorExpiresRepeatedNativeCalls+installedOperationsReadsAndEnforcesSqlDeadline'}
    $ErrorActionPreference='Continue'
    & $MavenCommand @options $selected clean verify *> (Join-Path $evidence 'build.log')
    $code=$LASTEXITCODE
    $ErrorActionPreference='Stop'
    if(Test-Path target/surefire-reports){Copy-Item target/surefire-reports (Join-Path $evidence 'surefire-reports') -Recurse}
    [ordered]@{outsideRepository=$fixture;buildExit=$code;selectedTests=$selected;completedUtc=[DateTimeOffset]::UtcNow.ToString('o')} | ConvertTo-Json | Set-Content (Join-Path $evidence 'receipt.json')
    if($code -ne 0){throw "Installed Messaging build failed: $evidence"}
    $ErrorActionPreference='Continue'
    & $MavenCommand @options dependency:tree '-Dscope=test' "-DoutputFile=$evidence/dependencies.txt" *> (Join-Path $evidence 'dependencies.log')
    $dependencyExit=$LASTEXITCODE
    $ErrorActionPreference='Stop'
    if($dependencyExit -ne 0){throw 'Installed dependency audit failed.'}
    $tree=Get-Content (Join-Path $evidence 'dependencies.txt') -Raw
    if($tree -notmatch 'com.apex:apex-sdk-messaging:jar:1.0-SNAPSHOT:compile'){throw 'Installed Messaging dependency missing.'}
    Get-FileHash target/sdk-consumer.jar -Algorithm SHA256 | ConvertTo-Json | Set-Content (Join-Path $evidence 'consumer-artifact.json')
    Get-ChildItem (Join-Path $root 'apex-sdk-*/target/*.jar') -ErrorAction SilentlyContinue | Get-FileHash -Algorithm SHA256 | ConvertTo-Json | Set-Content (Join-Path $evidence 'reactor-artifacts.json')
    $localRepo=Join-Path $env:USERPROFILE '.m2/repository/com/apex'
    Get-ChildItem (Join-Path $localRepo 'apex-sdk-*/1.0-SNAPSHOT/*.jar') | Get-FileHash -Algorithm SHA256 | ConvertTo-Json | Set-Content (Join-Path $evidence 'installed-artifacts.json')
    $reactorHash=(Get-FileHash -LiteralPath (Join-Path $root 'apex-sdk-messaging/target/apex-sdk-messaging-1.0-SNAPSHOT.jar')).Hash
    $installedHash=(Get-FileHash -LiteralPath (Join-Path $localRepo 'apex-sdk-messaging/1.0-SNAPSHOT/apex-sdk-messaging-1.0-SNAPSHOT.jar')).Hash
    if($reactorHash -ne $installedHash){throw 'Installed Messaging artifact differs from the verified reactor artifact.'}
    foreach($capability in @('grpc','operations')) {
        $jar="apex-sdk-$capability-1.0-SNAPSHOT.jar"
        $reactor=(Get-FileHash -LiteralPath (Join-Path $root "apex-sdk-$capability/target/$jar")).Hash
        $installed=(Get-FileHash -LiteralPath (Join-Path $localRepo "apex-sdk-$capability/1.0-SNAPSHOT/$jar")).Hash
        if($reactor -ne $installed){throw "Installed $capability differs from the verified reactor."}
    }
    [ordered]@{outsideRepository=$fixture;buildExit=$code;dependencyExit=$dependencyExit;selectedTests=$selected;messagingSha256=$installedHash;matchesReactor=$true;completedUtc=[DateTimeOffset]::UtcNow.ToString('o')} | ConvertTo-Json | Set-Content (Join-Path $evidence 'receipt.json')
    Write-Host "Installed Messaging PASS: $evidence"
} finally {Pop-Location;foreach($name in $saved.Keys){Set-Item -LiteralPath "Env:$name" -Value $saved[$name]}}
