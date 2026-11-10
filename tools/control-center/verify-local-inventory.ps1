param([string]$ReceiptPath, [int]$Samples = 1, [int]$IntervalSeconds = 5)
$ErrorActionPreference = 'Stop'
if ($Samples -lt 1 -or $Samples -gt 60 -or $IntervalSeconds -lt 1 -or $IntervalSeconds -gt 30) { throw 'Invalid sampling bounds' }
$taskRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
if (-not $ReceiptPath) { $ReceiptPath = Join-Path $taskRoot 'out/local-admin-live.json' }
$taskReceipts = @()
for ($taskIndex = 0; $taskIndex -lt $Samples; $taskIndex++) {
  $taskJava = Invoke-RestMethod 'http://127.0.0.1:9190/ops/v1/topology?schemaVersion=2' -TimeoutSec 8
  $taskBff = Invoke-RestMethod 'http://127.0.0.1:3000/api/ops/topology' -TimeoutSec 8
  $taskOverview = Invoke-WebRequest 'http://127.0.0.1:3000/api/ops/overview' -UseBasicParsing -TimeoutSec 8
  $taskPage = Invoke-WebRequest 'http://127.0.0.1:3000/topology' -UseBasicParsing -TimeoutSec 8
  # Registry access is test-side PowerShell only, never a browser fetch.
  $taskRegistry = Invoke-RestMethod 'http://127.0.0.1:8761/eureka/apps' -Headers @{Accept='application/json'} -TimeoutSec 8
  $taskExpected = @($taskRegistry.applications.application | Where-Object {
    @($_.instance | Where-Object {$_.status -eq 'UP' -and (-not $_.metadata.'apex.environment' -or $_.metadata.'apex.environment' -eq 'local')}).Count -gt 0
  } | ForEach-Object {$_.name.ToLowerInvariant()} | Sort-Object -Unique)
  $taskJavaNames = @($taskJava.nodes | Where-Object {$_.evidence.source -contains 'EUREKA'} | ForEach-Object {$_.name} | Sort-Object)
  $taskBffNames = @($taskBff.nodes | Where-Object {$_.evidence.source -contains 'EUREKA'} | ForEach-Object {$_.name} | Sort-Object)
  if ($taskJava.schemaVersion -ne 2 -or $taskBff.schemaVersion -ne 2) { throw 'Expected schemaVersion=2' }
  $taskProvider = @($taskBff.providers | Where-Object {$_.name -eq 'eureka-local-inventory'})
  if ($taskProvider.Count -ne 1 -or $taskProvider[0].status -ne 'AVAILABLE') { throw 'Registry coverage is not AVAILABLE; inspect provider status, never use fixtures' }
  if (($taskJavaNames -join ',') -cne ($taskBffNames -join ',')) { throw 'Java and BFF registry service sets differ; allow cache convergence and retry' }
  $taskConverged = ($taskExpected -join ',') -ceq ($taskBffNames -join ',')
  foreach ($taskService in $taskBffNames) {
    if (-not $taskPage.Content.Contains($taskService)) { throw 'A discovered service is missing from the server-rendered page' }
  }
  $taskReceipts += [ordered]@{
    source='LIVE_EUREKA_JAVA_BFF_SERVER_RENDERED_PAGE'; collectedAt=[DateTime]::UtcNow.ToString('o'); asOf=$taskBff.asOf
    javaStatus=200; bffStatus=200; overviewStatus=$taskOverview.StatusCode; pageStatus=$taskPage.StatusCode
    registryConverged=$taskConverged; registryServices=$taskExpected; renderedServices=$taskBffNames
    nodeCount=@($taskBff.nodes).Count; connectionCount=@($taskBff.connections).Count; membershipCount=@($taskBff.memberships).Count
    provider=$taskProvider[0]; topology=$taskBff
  }
  Write-Host "Sample $($taskIndex+1): $($taskBffNames.Count) services; registry converged=$taskConverged"
  if ($taskIndex + 1 -lt $Samples) { Start-Sleep -Seconds $IntervalSeconds }
}
$taskReceiptDirectory = Split-Path -Parent ([IO.Path]::GetFullPath($ReceiptPath))
New-Item -ItemType Directory -Force -Path $taskReceiptDirectory | Out-Null
$taskReceipts | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $ReceiptPath -Encoding UTF8
if (-not $taskReceipts[-1].registryConverged) { throw 'Registry and Console cache have not converged; receipt records the difference' }
Write-Host "Redacted API receipt written to $ReceiptPath (no Eureka endpoints or metadata)."
