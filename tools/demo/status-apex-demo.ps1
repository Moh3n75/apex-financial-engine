param([switch]$Logs)
. "$PSScriptRoot\common.ps1"
$m = Read-DemoManifest
Write-Host "Mode=$($m.mode) security=$($m.security) phase=$($m.phase)"
Write-Host "URL=$($m.url) projection=$($m.projection)"
foreach ($identity in @($m.supervisor) + @($m.children)) {
    [pscustomobject]@{ Service = $identity.service; PID = $identity.pid; OwnedProcessAlive = (Test-DemoIdentity $identity) }
}
Write-Host "HTTP: Next=$($m.nextPort) Console=$($m.consolePort) Order=$($m.orderPort) Inventory=$($m.inventoryPort) (all loopback)"
Write-Host "Owner gRPC: $($m.ownerGrpcPorts | ConvertTo-Json -Compress)"
Write-Host "Owned PostgreSQL ports=$($m.databasePorts -join ',') Kafka=$($m.brokers)"
Write-Host "Known Inventory Inbox scope: $($m.groups.'inventory-cell')"
Write-Host "Redacted diagnostic summary/log locations: $($m.runDirectory)"
Write-Host 'READABLE means owner DB probe, not full readiness. SHED/BACKOFF: wait a few seconds and Search again.'
if (Test-Path "$DemoOut\demo-readiness.json") {
    $r = Get-Content "$DemoOut\demo-readiness.json" -Raw | ConvertFrom-Json
    $alive = @(@($m.supervisor) + @($m.children) | Where-Object { -not (Test-DemoIdentity $_) }).Count -eq 0
    Write-Host "DEMO_READY=$(if($alive){$r.DEMO_READY}else{'NO'}) lastSmoke=$($r.verdict) at=$($r.at)"
}
if ($Logs) {
    foreach ($service in @('supervisor','order-cell','inventory-cell','payment-cell','console','next','browser-smoke')) {
        $log = Join-Path $m.runDirectory "$service.log"
        if (Test-Path -LiteralPath $log) {
            Write-Host "[$service]"
            Get-Content -LiteralPath $log -Tail 5 | ForEach-Object {
                if ($_ -match '(?i)password|token|secret|authorization:|cookie|private.key|payload|jdbc:|SELECT |INSERT |UPDATE ') { '[redacted sensitive diagnostic line]' } else { $_ }
            }
        }
    }
}
