param([switch]$NoAuth)
$ErrorActionPreference = 'Stop'
$taskRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
# Process-only settings. Do not overwrite an existing .env.local or touch auth implementation.
$env:APEX_OPERATIONS_API_ORIGIN = 'http://127.0.0.1:9190'
$env:APEX_OPS_UI_FIXTURE_MODE = 'false'
$env:OPS04B_REQUIRE_LIVE = 'true'
if ($NoAuth) { $env:APEX_OPS_M36A_ENABLED = 'false' }
Push-Location (Join-Path $taskRoot 'apps/operations-web')
try {
  Write-Host 'Using real loopback Console http://127.0.0.1:9190; no fixtures. Start Java Console with the local profile.'
  & pnpm.cmd dev
  if ($LASTEXITCODE -ne 0) { throw 'Next.js failed; check whether port 3000 is already in use.' }
} finally { Pop-Location }
