# Design preview only. No Java/PG/Kafka claims; entirely synthetic fixture JSON.
. "$PSScriptRoot\common.ps1"
$tools = Get-DemoTools
$env:APEX_OPS_UI_FIXTURE_MODE = 'true'; $env:OPS04B_REQUIRE_LIVE = 'false'
$env:APEX_OPS_M36A_ENABLED = 'false'; $env:APEX_OPS_M36B_ENABLED = 'false'
Write-Host 'SYNTHETIC DEMO: http://127.0.0.1:3000. Foreground server; Ctrl+C stops this preview.'
Push-Location "$DemoRoot\apps\operations-web"
try { & $tools.Node 'node_modules/next/dist/bin/next' dev -H 127.0.0.1 -p 3000 } finally { Pop-Location }
