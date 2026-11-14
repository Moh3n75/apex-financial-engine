<#
.SYNOPSIS
  M36A.1 authenticated live Browser → OIDC Identity → Next BFF → JWT Console → owners.

.DESCRIPTION
  Packages owned Identity/cells/Console in an isolated git worktree (avoids locked workspace
  Boot jars), builds Next with fixture mode OFF, then runs OpsM36a1AuthenticatedLiveIT.
  Does not commit/push or kill foreign PIDs.
#>
param(
  [string]$MavenCommand = $(
    Join-Path $env:USERPROFILE '.m2\wrapper\dists\apache-maven-3.9.11-bin\6mqf5t809d9geo83kj4ttckcbc\apache-maven-3.9.11\bin\mvn.cmd'
  ),
  [string]$RepositoryRoot = ''
)

$ErrorActionPreference = 'Stop'
$root = if ($RepositoryRoot) { (Resolve-Path $RepositoryRoot).Path } else { (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path }
Set-Location $root

function Assert-LoopbackPortFree([int]$Port) {
  $busy = @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
    Where-Object { $_.LocalPort -eq $Port })
  if ($busy.Count -gt 0) {
    $owner = ($busy | Select-Object -First 1).OwningProcess
    throw "Loopback port $Port is busy (PID $owner). M36A.1 will not stop unrelated processes."
  }
}

function Invoke-Maven([string[]]$MavenArgs, [string]$LogName, [string]$WorkDir = $root) {
  $logDir = Join-Path $root 'out\m36a1'
  New-Item -ItemType Directory -Force -Path $logDir | Out-Null
  $log = Join-Path $logDir $LogName
  Write-Host ">> ($WorkDir) mvn $($MavenArgs -join ' ')"
  Push-Location $WorkDir
  try {
    & $MavenCommand @MavenArgs 2>&1 | Tee-Object -FilePath $log
    if ($LASTEXITCODE -ne 0) { throw "Maven failed ($LASTEXITCODE). See $log" }
  }
  finally { Pop-Location }
}

Write-Host "M36A.1 authenticated live root=$root"
Assert-LoopbackPortFree 9190
Assert-LoopbackPortFree 3000

if (-not (Test-Path $MavenCommand)) { throw "Maven not found: $MavenCommand" }
$env:JAVA_HOME = if ($env:JAVA_HOME) { $env:JAVA_HOME } else { 'C:\Program Files\Java\jdk-21' }
$env:Path = "$env:JAVA_HOME\bin;$env:USERPROFILE\.local-node;$env:USERPROFILE\.local-node\bin;$env:Path"

Push-Location (Join-Path $root 'apps\operations-web')
try {
  if (-not (Get-Command pnpm -ErrorAction SilentlyContinue)) { throw 'pnpm not on PATH' }
  Remove-Item Env:APEX_OPS_UI_FIXTURE_MODE -ErrorAction SilentlyContinue
  $env:APEX_OPERATIONS_API_ORIGIN = 'http://127.0.0.1:9190'
  $env:OPS04B_REQUIRE_LIVE = 'true'
  $env:APEX_OPS_M36A_ENABLED = 'true'
  # Placeholder OIDC values so next build can evaluate server modules; runtime IT overrides process env.
  $env:APEX_OIDC_ISSUER = 'http://127.0.0.1:9'
  $env:APEX_OIDC_CLIENT_ID = 'apex-ops-web'
  $env:APEX_OIDC_CLIENT_SECRET = 'build-time-placeholder-secret-not-used'
  $env:APEX_OPS_SESSION_SECRET = 'build-time-session-secret-32chars-min!!'
  pnpm install --frozen-lockfile
  if ($LASTEXITCODE -ne 0) { throw "pnpm install failed: $LASTEXITCODE" }
  pnpm build
  if ($LASTEXITCODE -ne 0) { throw "pnpm build failed: $LASTEXITCODE" }
}
finally { Pop-Location }

Invoke-Maven @('-pl','apex-sdk-webmvc,apex-operations-console,apex-sdk-operations,apex-sdk-observability,apex-sdk-grpc-server,apex-sdk-messaging,apex-order-contracts','-am','install','-DskipTests') 'install-prereq.log'
# Identity is outside the aggregator reactor — install module + test-jar directly.
Invoke-Maven @('-f','apex-identity-server/pom.xml','install','-DskipTests') 'identity-install.log'

$wt = Join-Path $env:TEMP ("m36a1-wt-" + [guid]::NewGuid().ToString('N').Substring(0, 8))
Write-Host "Creating owned packaging worktree at $wt"
git worktree add --detach $wt HEAD
if ($LASTEXITCODE -ne 0) { throw "git worktree add failed" }

try {
  $syncPaths = @(
    'inventory-cell/src/test/java/com/apex/inventory/OpsM36a1AuthenticatedLiveIT.java',
    'inventory-cell/src/test/java/com/apex/inventory/m36a',
    'inventory-cell/src/test/java/com/apex/inventory/Ops04bLiveUiBridgeIT.java',
    'inventory-cell/pom.xml',
    'apps/operations-web',
    'tools/m36a'
  )
  foreach ($rel in $syncPaths) {
    $src = Join-Path $root $rel
    $dst = Join-Path $wt $rel
    if (-not (Test-Path $src)) { continue }
    if (Test-Path $src -PathType Container) {
      New-Item -ItemType Directory -Force -Path $dst | Out-Null
      robocopy $src $dst /E /XD node_modules .next test-results /NFL /NDL /NJH /NJS /nc /ns /np | Out-Null
      if ($LASTEXITCODE -ge 8) { throw "robocopy failed for $rel (code $LASTEXITCODE)" }
      $global:LASTEXITCODE = 0
    }
    else {
      New-Item -ItemType Directory -Force -Path (Split-Path $dst) | Out-Null
      Copy-Item -Force $src $dst
    }
  }

  Invoke-Maven @('-f','order-cell/pom.xml','package','-DskipTests') 'order-package.log' $wt
  Invoke-Maven @('-f','inventory-cell/pom.xml','package','-DskipTests') 'inventory-package.log' $wt
  Invoke-Maven @('-f','payment-cell/pom.xml','package','-DskipTests') 'payment-package.log' $wt
  Invoke-Maven @('-f','apex-operations-console/pom.xml','package','-DskipTests') 'console-package.log' $wt
  Invoke-Maven @('-f','apex-identity-server/pom.xml','package','-DskipTests') 'identity-package.log' $wt

  $jarDir = Join-Path $root 'out\m36a1\jars'
  New-Item -ItemType Directory -Force -Path $jarDir | Out-Null
  $pairs = @{
    'order-cell.jar' = (Join-Path $wt 'order-cell\target\order-cell.jar')
    'inventory-cell.jar' = (Join-Path $wt 'inventory-cell\target\inventory-cell.jar')
    'payment-cell.jar' = (Join-Path $wt 'payment-cell\target\payment-cell.jar')
    'apex-operations-console-1.0-SNAPSHOT.jar' = (Join-Path $wt 'apex-operations-console\target\apex-operations-console-1.0-SNAPSHOT.jar')
    'apex-identity-server.jar' = (Join-Path $wt 'apex-identity-server\target\apex-identity-server.jar')
  }
  foreach ($name in $pairs.Keys) {
    $j = $pairs[$name]
    if (-not (Test-Path $j)) { throw "Missing jar $j" }
    if ((Get-Item $j).Length -lt 1MB) { throw "Jar too small: $j" }
    Copy-Item -Force $j (Join-Path $jarDir $name)
  }
  $sha = @{}
  Get-ChildItem $jarDir -Filter *.jar | ForEach-Object {
    $sha[$_.Name] = (Get-FileHash $_.FullName -Algorithm SHA256).Hash.ToLower()
  }
  $sha['worktree'] = $wt
  $sha | ConvertTo-Json | Set-Content (Join-Path $root 'out\m36a1\build-receipt.json')

  Remove-Item Env:APEX_OPS_UI_FIXTURE_MODE -ErrorAction SilentlyContinue
  $env:APEX_OPS_UI_FIXTURE_MODE = 'false'
  Invoke-Maven @(
    '-f','inventory-cell/pom.xml','test',
    '-Dtest=OpsM36a1AuthenticatedLiveIT',
    "-Dsaga.repositoryRoot=$root",
    "-Dsaga.inventoryJar=$jarDir\inventory-cell.jar",
    "-Dsaga.orderJar=$jarDir\order-cell.jar",
    "-Dsaga.paymentJar=$jarDir\payment-cell.jar",
    "-Dsaga.consoleJar=$jarDir\apex-operations-console-1.0-SNAPSHOT.jar",
    "-Dsaga.identityJar=$jarDir\apex-identity-server.jar"
  ) 'authenticated-live-bridge.log' $wt

  Write-Host 'M36A.1 authenticated live bridge Maven exit 0'
  Get-Content (Join-Path $root 'out\m36a1\live-bridge-receipt.json') -ErrorAction SilentlyContinue
}
finally {
  Set-Location $root
  git worktree remove --force $wt 2>$null
  if (Test-Path $wt) {
    Remove-Item -Recurse -Force $wt -ErrorAction SilentlyContinue
  }
}
