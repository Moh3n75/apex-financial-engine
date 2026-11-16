param([string]$MavenCommand='mvn.cmd')
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$fixture=Join-Path ([IO.Path]::GetTempPath()) ('apex-aor01-neutral-'+[Guid]::NewGuid().ToString('N'))
$evidence=Join-Path $root ('out/aor-01/neutral-'+[DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))
New-Item -ItemType Directory -Path $fixture,$evidence -Force | Out-Null
Copy-Item (Join-Path $PSScriptRoot 'neutral-consumer/*') $fixture -Recurse
$saved=@{}
Get-ChildItem Env: | Where-Object {$_.Name -match '^(SPRING_|APEX_|KAFKA_|JAVA_TOOL_OPTIONS$|JDK_JAVA_OPTIONS$|_JAVA_OPTIONS$|MAVEN_OPTS$|MAVEN_ARGS$)'} | ForEach-Object {$saved[$_.Name]=$_.Value;Remove-Item -LiteralPath "Env:$($_.Name)"}
function Run([string]$exe,[string[]]$arguments,[string]$log,[bool]$pass=$true) {
    $ErrorActionPreference='Continue'
    & $exe @arguments *> (Join-Path $evidence $log)
    $code=$LASTEXITCODE
    $ErrorActionPreference='Stop'
    if(($code -eq 0) -ne $pass){throw "Unexpected exit $code for $log"}
}
Push-Location $fixture
try {
    Run $MavenCommand @('-o','-B','-ntp','clean','verify') 'build.log'
    Copy-Item target/surefire-reports (Join-Path $evidence 'surefire-reports') -Recurse
    Run $MavenCommand @('-o','-B','-ntp','dependency:tree','-Dscope=test',"-DoutputFile=$evidence/dependencies.txt") 'dependencies.log'
    $tree=Get-Content (Join-Path $evidence 'dependencies.txt') -Raw
    if($tree -match '(org.postgresql:postgresql|com.oracle|oracle-r2dbc|flyway-database|spring-boot-starter-jdbc|spring-boot-jdbc:)'){throw 'Database-specific dependencies leaked into neutral graph'}
    if($tree -notmatch 'com.apex:apex-sdk-messaging-core:jar:1.0-SNAPSHOT'){throw 'Neutral artifact missing'}
    $java=Join-Path $env:JAVA_HOME 'bin/java'
    Run $java @('-jar','target/neutral-consumer.jar','--spring.main.banner-mode=off') 'boot.log'
    if(-not (Select-String (Join-Path $evidence 'boot.log') -Pattern 'NEUTRAL_MESSAGING_CONSUMER_OK' -Quiet)){throw 'Neutral boot receipt missing'}
    Run $java @('-jar','target/neutral-consumer.jar','--spring.main.banner-mode=off','--apex.messaging.cell.enabled=true','--apex.messaging.storage.provider=oracle') 'unsupported.log' $false
    if(-not (Select-String (Join-Path $evidence 'unsupported.log') -Pattern 'Unsupported apex.messaging.storage.provider' -Quiet)){throw 'Unsupported selection did not fail clearly'}
    Run $java @('-jar','target/neutral-consumer.jar','--spring.main.banner-mode=off','--apex.messaging.cell.enabled=true','--apex.messaging.outbox.publisher.enabled=false') 'missing-owner.log' $false
    if(-not (Select-String (Join-Path $evidence 'missing-owner.log') -Pattern 'requires owner bean OutboxAppender' -Quiet)){throw 'Missing adapter did not fail clearly'}
    Get-FileHash target/neutral-consumer.jar | ConvertTo-Json | Set-Content (Join-Path $evidence 'artifact.json')
    [ordered]@{outsideRepository=$fixture;buildExit=0;bootExit=0;unsupportedRejected=$true;missingOwnerRejected=$true;completedUtc=[DateTimeOffset]::UtcNow.ToString('o')} | ConvertTo-Json | Set-Content (Join-Path $evidence 'receipt.json')
    Write-Host "Neutral installed consumer PASS: $evidence"
} finally {Pop-Location;foreach($name in $saved.Keys){Set-Item -LiteralPath "Env:$name" -Value $saved[$name]}}
