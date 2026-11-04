. "$PSScriptRoot\common.ps1"
$m = Read-DemoManifest
if (-not (Test-DemoIdentity $m.supervisor)) {
    Write-Host 'Recorded supervisor is absent. No foreign process will be stopped.'
    return
}
[IO.File]::WriteAllText((Join-Path $m.runDirectory 'stop.flag'), 'OWNER_REQUESTED_STOP')
Write-Host 'Requested graceful shutdown of owned children and disposable PG/KRaft/certificates.'
$deadline = (Get-Date).AddSeconds(150)
while (Test-DemoIdentity $m.supervisor) {
    if ((Get-Date) -gt $deadline) { throw "Supervisor has not stopped; no blind kill performed. Inspect $($m.runDirectory)." }
    Start-Sleep -Seconds 1
}
$cleanup = Join-Path $m.runDirectory 'cleanup.json'
if (Test-Path $cleanup) { Get-Content $cleanup -Raw } else { Write-Warning 'No cleanup receipt; inspect owned directory.' }
foreach ($child in $m.children) {
    if (Test-DemoIdentity $child) { throw "Recorded owned child still running: $($child.service). No unverified kill performed." }
}
if ($m.kafkaDataDirectory) {
    $expected = [IO.Path]::GetFullPath((Join-Path $m.runDirectory 'kafka-data'))
    $target = [IO.Path]::GetFullPath($m.kafkaDataDirectory)
    if ($target -ne $expected -or -not $target.StartsWith($DemoOut + '\', [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Refusing cleanup outside the exact owned Kafka data directory.'
    }
    if (Test-Path -LiteralPath $target) { Remove-Item -LiteralPath $target -Recurse -Force }
    @{ kafkaDataRemoved = -not (Test-Path -LiteralPath $target); at = [DateTimeOffset]::UtcNow.ToString('o') } |
        ConvertTo-Json | Set-Content (Join-Path $m.runDirectory 'cleanup-details.json') -Encoding UTF8
}
@{ DEMO_READY = 'NO'; state = 'STOPPED'; runDirectory = $m.runDirectory; at = [DateTimeOffset]::UtcNow.ToString('o') } |
    ConvertTo-Json | Set-Content "$DemoOut\demo-readiness.json" -Encoding UTF8
