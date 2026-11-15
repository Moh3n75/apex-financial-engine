$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$fixture=Join-Path $root ('out/sdk-1/negative-controls-'+[Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force $fixture|Out-Null
foreach($module in Get-ChildItem $root -Directory -Filter 'apex-sdk-*') {
    $dest=Join-Path $fixture $module.Name
    New-Item -ItemType Directory -Force (Join-Path $dest 'src'),(Join-Path $dest 'target/classes/META-INF')|Out-Null
    if(Test-Path (Join-Path $module.FullName 'src/main')){Copy-Item (Join-Path $module.FullName 'src/main') (Join-Path $dest 'src') -Recurse}
    $metadata=Join-Path $module.FullName 'target/classes/META-INF/spring-configuration-metadata.json'
    if(Test-Path $metadata){Copy-Item $metadata (Join-Path $dest 'target/classes/META-INF')}
}
# Frozen protocol/SQL reference files live in test resources in the copied tree.
$frozen=Join-Path $fixture 'apex-sdk-operations/src/test/resources/sdk-1'
New-Item -ItemType Directory -Force $frozen | Out-Null
Copy-Item (Join-Path $root 'apex-sdk-operations/src/test/resources/sdk-1/v1') $frozen -Recurse
function Check([string]$Script,[bool]$MustPass,[string]$Label) {
    $ErrorActionPreference='Continue'
    & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot $Script) -Root $fixture *> (Join-Path $fixture "$Label.log")
    $code=$LASTEXITCODE
    if(($code -eq 0) -ne $MustPass){throw "Negative control failed: $Label exit=$code"}
    Write-Host "$Label exit=$code expectedPass=$MustPass"
}
Check 'verify-contract-files.ps1' $true 'protocol-sql-unchanged'
Check 'verify-configuration.ps1' $true 'configuration-unchanged'
$proto=Join-Path $fixture 'apex-sdk-grpc/src/main/proto/cell_gateway.proto'
$original=[IO.File]::ReadAllText($proto)
[IO.File]::WriteAllText($proto,$original.Replace('int32 contract_version = 2;','int64 contract_version = 2;'))
Check 'verify-contract-files.ps1' $false 'protobuf-field-type-break'
[IO.File]::WriteAllText($proto,$original)
$sql=Join-Path $fixture 'apex-sdk-messaging/src/main/resources/db/platform/V1001__platform_outbox_event.sql'
[IO.File]::WriteAllText($sql,[IO.File]::ReadAllText($sql).Replace('event_version > 0','event_version >= 0'))
Check 'verify-contract-files.ps1' $false 'applied-sql-constraint-break'
$metadata=Join-Path $fixture 'apex-sdk-grpc/target/classes/META-INF/spring-configuration-metadata.json'
[IO.File]::WriteAllText($metadata,[IO.File]::ReadAllText($metadata).Replace('65536','65535'))
Check 'verify-configuration.ps1' $false 'configuration-default-break'
Write-Host "Negative controls PASS; retained at $fixture"
