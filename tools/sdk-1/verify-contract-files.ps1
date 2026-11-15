param([string]$Root=(Join-Path $PSScriptRoot '../..'))
$ErrorActionPreference='Stop'
$root=(Resolve-Path $Root).Path
$frozen=Join-Path $root 'apex-sdk-operations/src/test/resources/sdk-1/v1'
$files=New-Object 'System.Collections.Generic.List[object]'
$files.Add(@('apex-sdk-grpc/src/main/proto/cell_gateway.proto','cell_gateway.proto'))
foreach($sql in Get-ChildItem (Join-Path $root 'apex-sdk-messaging/src/main/resources/db/platform') -File){$files.Add(@("apex-sdk-messaging/src/main/resources/db/platform/$($sql.Name)","sql/$($sql.Name)"))}
if($files.Count -ne 3){throw 'Protocol/migration file set changed; explicit versioned compatibility review required.'}
foreach($pair in $files) {
    $actual=[IO.File]::ReadAllText((Join-Path $root $pair[0])).Replace("`r`n","`n")
    $expected=[IO.File]::ReadAllText((Join-Path $frozen $pair[1])).Replace("`r`n","`n")
    if($actual -ne $expected){throw "Frozen protocol/applied migration changed: $($pair[0])"}
}
Write-Host 'Protocol and applied SQL v1 fixtures PASS'
