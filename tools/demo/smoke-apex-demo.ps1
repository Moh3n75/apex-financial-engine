param([switch]$LatestOrder)
. "$PSScriptRoot\common.ps1"
$tools = Get-DemoTools
$m = Read-DemoManifest
if (-not (Test-DemoIdentity $m.supervisor) -or $m.phase -ne 'RUNNING') { throw 'Demo supervisor not ready.' }
Write-Host 'Checking actual Browser -> BFF -> Console -> owner evidence. Stack will remain running.'
$ErrorActionPreference = 'Continue'
$sample = Join-Path $m.runDirectory 'seed.json'
if ($LatestOrder) {
    $sample = Join-Path $m.runDirectory 'smoke-latest-order.json'
    [IO.File]::WriteAllText($sample, ($m.orders[-1] | ConvertTo-Json -Depth 8))
}
& $tools.Node "$PSScriptRoot\live-smoke.cjs" $m.runDirectory $m.sourceDirectory $sample *> (Join-Path $m.runDirectory 'browser-smoke.log')
$code = $LASTEXITCODE
$ErrorActionPreference = 'Stop'
$ready = if ($code -eq 0) { 'YES' } else { 'NO' }
@{ DEMO_READY = $ready; verdict = $(if ($code -eq 0) { 'INTERACTIVE_DEMO_PASS' } else { 'INTERACTIVE_SMOKE_FAIL' });
    mode = 'Preview'; security = 'NO_M36_AUTH_LOOPBACK_TEST_DATA'; url = $m.url; fixtureMode = $false;
    runDirectory = $m.runDirectory; supervisor = $m.supervisor; children = $m.children; orders = $m.orders;
    smokeExit = $code; fullRegression = 'NOT_RUN_M36B_COMBINED_FULL_REGRESSION_REMAINS_FAIL';
    productionReady = $false; projection = 'NONE'; at = [DateTimeOffset]::UtcNow.ToString('o') } |
    ConvertTo-Json -Depth 10 | Set-Content "$DemoOut\demo-readiness.json" -Encoding UTF8
if ($code -ne 0) { throw "DEMO_READY=NO. Smoke failed. Stack remains running; inspect $($m.runDirectory)\browser-smoke.log. Stop with stop-apex-demo.ps1." }
Write-Host 'DEMO_READY=YES INTERACTIVE_DEMO_PASS (not FULL_REGRESSION / PRODUCTION_READY)'
