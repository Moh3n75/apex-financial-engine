<#
.SYNOPSIS
  OPS-04B live Browser → Next BFF → Java Operations Console acceptance (loopback only).

.DESCRIPTION
  Packages owned cells + Console in an isolated git worktree (never renames locked workspace
  target jars), builds the Next app (fixture mode OFF), then runs Ops04bLiveUiBridgeIT.
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
    throw "Loopback port $Port is busy (PID $owner). OPS-04B will not stop unrelated processes."
  }
}

function Invoke-Maven([string[]]$MavenArgs, [string]$LogName, [string]$WorkDir = $root) {
  $logDir = Join-Path $root 'out\ops04b'
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

Write-Host "OPS-04B live acceptance root=$root"
Assert-LoopbackPortFree 9190
Assert-LoopbackPortFree 3000

if (-not (Test-Path $MavenCommand)) { throw "Maven not found: $MavenCommand" }
$env:JAVA_HOME = if ($env:JAVA_HOME) { $env:JAVA_HOME } else { 'C:\Program Files\Java\jdk-21' }
$env:Path = "$env:JAVA_HOME\bin;$env:USERPROFILE\.local-node;$env:USERPROFILE\.local-node\bin;$env:Path"

# Frontend frozen install + production build (live server started by Java bridge).
Push-Location (Join-Path $root 'apps\operations-web')
try {
  if (-not (Get-Command pnpm -ErrorAction SilentlyContinue)) { throw 'pnpm not on PATH' }
  Remove-Item Env:APEX_OPS_UI_FIXTURE_MODE -ErrorAction SilentlyContinue
  $env:APEX_OPERATIONS_API_ORIGIN = 'http://127.0.0.1:9190'
  $env:OPS04B_REQUIRE_LIVE = 'true'
  pnpm install --frozen-lockfile
  if ($LASTEXITCODE -ne 0) { throw "pnpm install failed: $LASTEXITCODE" }
  pnpm build
  if ($LASTEXITCODE -ne 0) { throw "pnpm build failed: $LASTEXITCODE" }
}
finally { Pop-Location }

# Install reactor deps into local .m2 from the working tree (does not repackage locked Boot jars).
Invoke-Maven @('-pl','apex-sdk-webmvc,apex-operations-console,apex-sdk-operations,apex-sdk-observability,apex-sdk-grpc-server,apex-sdk-messaging,apex-order-contracts','-am','install','-DskipTests') 'install-prereq.log'

# Isolated worktree avoids renaming workspace order-cell/target/*.jar held by foreign JVMs.
$wt = Join-Path $env:TEMP ("ops04b-wt-" + [guid]::NewGuid().ToString('N').Substring(0, 8))
Write-Host "Creating owned packaging worktree at $wt"
git worktree add --detach $wt HEAD
if ($LASTEXITCODE -ne 0) { throw "git worktree add failed" }

try {
  # Carry uncommitted OPS-04B bridge + frontend sources needed for the live IT.
  $syncPaths = @(
    'inventory-cell/src/test/java/com/apex/inventory/Ops04bLiveUiBridgeIT.java',
    'apps/operations-web',
    'tools/ops04b'
  )
  foreach ($rel in $syncPaths) {
    $src = Join-Path $root $rel
    $dst = Join-Path $wt $rel
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

  $orderJar = Join-Path $wt 'order-cell\target\order-cell.jar'
  $invJar = Join-Path $wt 'inventory-cell\target\inventory-cell.jar'
  $payJar = Join-Path $wt 'payment-cell\target\payment-cell.jar'
  $consoleJar = Join-Path $wt 'apex-operations-console\target\apex-operations-console-1.0-SNAPSHOT.jar'
  foreach ($j in @($orderJar, $invJar, $payJar, $consoleJar)) {
    if (-not (Test-Path $j)) { throw "Missing jar $j" }
    if ((Get-Item $j).Length -lt 1MB) { throw "Jar too small (not Boot fat jar?): $j" }
  }

  $jarDir = Join-Path $root 'out\ops04b\jars'
  New-Item -ItemType Directory -Force -Path $jarDir | Out-Null
  Copy-Item -Force $orderJar (Join-Path $jarDir 'order-cell.jar')
  Copy-Item -Force $invJar (Join-Path $jarDir 'inventory-cell.jar')
  Copy-Item -Force $payJar (Join-Path $jarDir 'payment-cell.jar')
  Copy-Item -Force $consoleJar (Join-Path $jarDir 'apex-operations-console-1.0-SNAPSHOT.jar')
  $orderJar = Join-Path $jarDir 'order-cell.jar'
  $invJar = Join-Path $jarDir 'inventory-cell.jar'
  $payJar = Join-Path $jarDir 'payment-cell.jar'
  $consoleJar = Join-Path $jarDir 'apex-operations-console-1.0-SNAPSHOT.jar'

  $sha = @{
    'order-cell' = (Get-FileHash $orderJar -Algorithm SHA256).Hash.ToLower()
    'inventory-cell' = (Get-FileHash $invJar -Algorithm SHA256).Hash.ToLower()
    'payment-cell' = (Get-FileHash $payJar -Algorithm SHA256).Hash.ToLower()
    'apex-operations-console' = (Get-FileHash $consoleJar -Algorithm SHA256).Hash.ToLower()
    'worktree' = $wt
  }
  $sha | ConvertTo-Json | Set-Content (Join-Path $root 'out\ops04b\build-receipt.json')

  # Compile/run bridge IT against worktree inventory-cell (has synced Ops04bLiveUiBridgeIT).
  # saga.repositoryRoot stays the real workspace so Next.js uses the built apps/operations-web.
  Remove-Item Env:APEX_OPS_UI_FIXTURE_MODE -ErrorAction SilentlyContinue
  $env:APEX_OPS_UI_FIXTURE_MODE = 'false'
  Invoke-Maven @(
    '-f','inventory-cell/pom.xml','test',
    '-Dtest=Ops04bLiveUiBridgeIT',
    "-Dsaga.repositoryRoot=$root",
    "-Dsaga.inventoryJar=$invJar",
    "-Dsaga.orderJar=$orderJar",
    "-Dsaga.paymentJar=$payJar",
    "-Dsaga.consoleJar=$consoleJar"
  ) 'live-bridge.log' $wt

  Write-Host 'OPS-04B live bridge Maven exit 0'
  Get-Content (Join-Path $root 'out\ops04b\live-bridge-receipt.json') -ErrorAction SilentlyContinue
}
finally {
  Set-Location $root
  git worktree remove --force $wt 2>$null
  if (Test-Path $wt) {
    Remove-Item -Recurse -Force $wt -ErrorAction SilentlyContinue
  }
}

exit 0
