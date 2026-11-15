param([string]$MavenCommand='mvn.cmd',[switch]$Offline,[switch]$Minimal,[switch]$Sdk1,[string]$RegistryJar)
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$fixture=Join-Path ([IO.Path]::GetTempPath()) ('apex-sdk-consumer-'+[Guid]::NewGuid().ToString('N'))
if([IO.Path]::GetFullPath($fixture).StartsWith($root,[StringComparison]::OrdinalIgnoreCase)){throw 'Consumer must be outside the repository.'}
$evidenceRoot=if($Sdk1){'out/sdk-1/external/'}else{'out/m35-6/external/'}
$evidence=Join-Path $root ($evidenceRoot+[DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))
if(-not $RegistryJar){$RegistryJar=Join-Path $root 'out/m34-artifacts/apex-discovery-server.jar'}
function Run([string]$Executable,[string[]]$Arguments,[string]$Log) {
    $ErrorActionPreference='Continue'
    & $Executable @Arguments *> $Log
    return $LASTEXITCODE
}
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
    $modes=@('default','http');if(-not $Minimal){$modes+=@('capabilities','observability','rpc-client','rpc-server','operations')}
    if($Sdk1){$modes+=@('messaging','discovery-eureka','discovery-kubernetes')}
    foreach($mode in $modes) {
        $profileArgs=@();if($mode -ne 'default'){$profileArgs+= "-P$mode"}
        if($mode -eq 'capabilities'){$profileArgs+="-Dexternal.registry.jar=$RegistryJar"}
        $code=Run $MavenCommand ($options+$profileArgs+@('clean','verify')) (Join-Path $evidence "$mode-build.log")
        New-Item -ItemType Directory -Path (Join-Path $evidence $mode) -Force|Out-Null
        if(Test-Path target/surefire-reports){Copy-Item target/surefire-reports/TEST-*.xml (Join-Path $evidence $mode)}
        if($code -ne 0){throw "External $mode build failed exit=$code; see $evidence"}
        $code=Run $MavenCommand ($options+$profileArgs+@('dependency:tree','-Dscope=runtime',"-DoutputFile=$evidence/$mode-tree.txt")) (Join-Path $evidence "$mode-dependencies.log")
        if($code -ne 0){throw 'Dependency audit failed.'}
        $tree=Get-Content (Join-Path $evidence "$mode-tree.txt") -Raw
        if($tree -match '(apex-platform-context-autoconfigure|apex-platform-webmvc|apex-(api-gateway|discovery-server|identity-server|bootstrap|cell-template|order-contracts|replay-tool)|com.apex:(order|inventory|payment|customer|reporting)-cell|oauth2-authorization-server)'){throw 'Executable or removed artifact leaked into consumer.'}
        if($mode -notin @('capabilities','messaging','discovery-eureka','discovery-kubernetes') -and $tree -match '(spring-kafka|kafka-clients|spring-jdbc|eureka|fabric8)'){throw 'Unselected capability leaked into consumer.'}
        if($mode -eq 'messaging' -and $tree -match '(eureka|fabric8|grpc-netty|spring-webmvc)'){throw 'Unselected transport leaked into Messaging.'}
        if($mode -like 'discovery-*' -and $tree -match '(spring-kafka|kafka-clients|spring-jdbc|grpc-netty|spring-webmvc)'){throw 'Transport leaked into discovery-only consumer.'}
        if($mode -eq 'discovery-eureka' -and $tree -match 'fabric8'){throw 'Kubernetes leaked into Eureka.'}
        if($mode -eq 'discovery-kubernetes' -and $tree -match 'eureka'){throw 'Eureka leaked into Kubernetes.'}
        if($mode -notin @('capabilities','rpc-client','rpc-server') -and $tree -match 'grpc-netty'){throw 'RPC transport leaked into consumer.'}
        if($mode -eq 'rpc-client' -and $tree -match 'spring-boot-(starter-)?grpc-server'){throw 'Server leaked into client.'}
        if($mode -eq 'rpc-server' -and $tree -match 'spring-boot-(starter-)?grpc-client'){throw 'Client leaked into server.'}
        if($mode -in @('capabilities','messaging')){
            $receipts+=[ordered]@{mode=$mode;jarSha256=(Get-FileHash target/sdk-consumer.jar -Algorithm SHA256).Hash;bootExit='In JUnit: owned PostgreSQL/Kafka and real gRPC';testExit=0}
            continue
        }
        $args=@('-jar','target/sdk-consumer.jar','--spring.config.location=optional:classpath:/absent.properties','--spring.main.banner-mode=off','--spring.application.name=external-consumer','--apex.cell.id=external-consumer')
        if($mode -ne 'http'){$args+=@('--spring.main.web-application-type=none','--spring.grpc.server.enabled=false','--eureka.client.enabled=false')}else{$args+=@('--server.address=127.0.0.1','--server.port=0')}
        if($mode -eq 'observability'){$args+='--apex.observability.enabled=true'}
        $code=Run (Join-Path $env:JAVA_HOME 'bin/java') $args (Join-Path $evidence "$mode-boot.log")
        if($code -ne 0 -or -not (Select-String -LiteralPath (Join-Path $evidence "$mode-boot.log") -Pattern 'SDK_CONSUMER_ACCEPTED')){throw "External $mode packaged boot failed."}
        $receipts+=[ordered]@{mode=$mode;jarSha256=(Get-FileHash target/sdk-consumer.jar -Algorithm SHA256).Hash;bootExit=0;testExit=0}
    }
    if($Sdk1){
        $binary=Join-Path $fixture 'frozen/compatibility';New-Item -ItemType Directory -Force $binary|Out-Null
        [IO.File]::WriteAllBytes((Join-Path $binary 'FrozenConsumerV1.class'),[Convert]::FromBase64String((Get-Content (Join-Path $root 'tools/sdk-1/baseline/frozen-consumer-v1.class.b64') -Raw)))
        $jar=Join-Path $env:USERPROFILE '.m2/repository/com/apex/apex-sdk-api/1.0-SNAPSHOT/apex-sdk-api-1.0-SNAPSHOT.jar'
        $code=Run (Join-Path $env:JAVA_HOME 'bin/java') @('-cp',"$fixture/frozen;$jar",'compatibility.FrozenConsumerV1') (Join-Path $evidence 'frozen-consumer.log')
        if($code -ne 0 -or -not (Select-String (Join-Path $evidence 'frozen-consumer.log') -Pattern 'FROZEN_CONSUMER_V1_ACCEPTED')){throw 'Frozen binary consumer failed.'}
        $hashes=@();foreach($module in Get-ChildItem $root -Directory -Filter 'apex-sdk-*') {
            $name=$module.Name;$local=Join-Path $env:USERPROFILE ".m2/repository/com/apex/$name/1.0-SNAPSHOT/$name-1.0-SNAPSHOT.jar"
            $built=Join-Path $module.FullName "target/$name-1.0-SNAPSHOT.jar"
            if((Get-FileHash $local).Hash -ne (Get-FileHash $built).Hash){throw "Installed artifact mismatch: $name"}
            $hashes+=Get-FileHash $local
        };$hashes|ConvertTo-Json|Set-Content (Join-Path $evidence 'installed-artifacts.json')
        Get-FileHash $RegistryJar|ConvertTo-Json|Set-Content (Join-Path $evidence 'registry-artifact.json')
    }
    [ordered]@{outsideRepository=$fixture;artifactReceipts=$receipts;frozenBinaryChecked=[bool]$Sdk1;completedUtc=[DateTimeOffset]::UtcNow.ToString('o')}|ConvertTo-Json -Depth 5|Set-Content (Join-Path $evidence 'receipt.json')
    Write-Host "External consumer acceptance PASS: $evidence"
} finally {Pop-Location;foreach($name in $saved.Keys){Set-Item -LiteralPath "Env:$name" -Value $saved[$name]}}
exit 0
