<#
.SYNOPSIS
  M36B live matrix: resource auth, 2×Next continuity, JWKS rotation, OIDC negatives, owner ACL.

.DESCRIPTION
  Packages owned jars in an isolated git worktree, builds Next (fixture OFF), runs OpsM36bLiveIT.
  Does not commit/push or kill foreign PIDs. LOCAL_RESTRICTED_ONLY.
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
    throw "Loopback port $Port is busy (PID $owner). M36B will not stop unrelated processes."
  }
}

function Invoke-Maven([string[]]$MavenArgs, [string]$LogName, [string]$WorkDir = $root) {
  $logDir = Join-Path $root 'out\m36b'
  New-Item -ItemType Directory -Force -Path $logDir | Out-Null
  $log = Join-Path $logDir $LogName
  Write-Host ">> ($WorkDir) mvn $($MavenArgs -join ' ')"
  Push-Location $WorkDir
  try {
    # Maven/Surefire write diagnostics to stderr; do not let PS NativeCommandError abort mid-run.
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    & $MavenCommand @MavenArgs 2>&1 | Tee-Object -FilePath $log
    $code = $LASTEXITCODE
    $ErrorActionPreference = $prev
    if ($code -ne 0) { throw "Maven failed ($code). See $log" }
  }
  finally { Pop-Location }
}

Write-Host "M36B live root=$root"
Assert-LoopbackPortFree 9190
Assert-LoopbackPortFree 3000
Assert-LoopbackPortFree 3001

if (-not (Test-Path $MavenCommand)) { throw "Maven not found: $MavenCommand" }
$env:JAVA_HOME = if ($env:JAVA_HOME) { $env:JAVA_HOME } else { 'C:\Program Files\Java\jdk-21' }
$env:Path = "$env:JAVA_HOME\bin;$env:USERPROFILE\.local-node;$env:USERPROFILE\.local-node\bin;$env:Path"

# Source fingerprint of paths used for this run
$manifest = Join-Path $root 'out\m36b\source-entry-manifest.json'
$paths = @(
  'apex-operations-console/src/main/java/com/apex/operations/console/security/m36b',
  'apex-operations-console/src/main/java/com/apex/operations/console/security/OpsM36aSecurityConfiguration.java',
  'apps/operations-web/src/server/auth/resource-grants.ts',
  'inventory-cell/src/test/java/com/apex/inventory/OpsM36bLiveIT.java',
  'inventory-cell/src/test/java/com/apex/inventory/m36a/DisposableM36aIdentity.java',
  'tools/m36b'
)
$entries = @{}
foreach ($rel in $paths) {
  $p = Join-Path $root $rel
  if (Test-Path $p -PathType Leaf) {
    $entries[$rel] = (Get-FileHash $p -Algorithm SHA256).Hash.ToLower()
  } elseif (Test-Path $p) {
    Get-ChildItem $p -Recurse -File | ForEach-Object {
      $relFile = $_.FullName.Substring($root.Length).TrimStart('\','/')
      $entries[$relFile.Replace('\','/')] = (Get-FileHash $_.FullName -Algorithm SHA256).Hash.ToLower()
    }
  }
}
$entries['HEAD'] = (git rev-parse HEAD)
New-Item -ItemType Directory -Force -Path (Split-Path $manifest) | Out-Null
$entries | ConvertTo-Json | Set-Content $manifest

Push-Location (Join-Path $root 'apps\operations-web')
try {
  if (-not (Get-Command pnpm -ErrorAction SilentlyContinue)) { throw 'pnpm not on PATH' }
  Remove-Item Env:APEX_OPS_UI_FIXTURE_MODE -ErrorAction SilentlyContinue
  $env:APEX_OPERATIONS_API_ORIGIN = 'http://127.0.0.1:9190'
  $env:OPS04B_REQUIRE_LIVE = 'true'
  $env:APEX_OPS_M36A_ENABLED = 'true'
  $env:APEX_OIDC_ISSUER = 'http://127.0.0.1:9'
  $env:APEX_OIDC_CLIENT_ID = 'apex-ops-web'
  $env:APEX_OIDC_CLIENT_SECRET = 'build-time-placeholder-secret-not-used'
  $env:APEX_OPS_SESSION_SECRET = 'build-time-session-secret-32chars-min!!'
  $env:APEX_OPS_M36B_RESOURCE_AUTH = 'true'
  $env:APEX_OPS_M36B_RESOURCE_GRANTS = '[{"subject":"*","environment":"test","owner":"order-cell","kinds":"OUTBOX_EVENT,OVERVIEW","scopes":"owner,order"}]'
  pnpm install --frozen-lockfile
  if ($LASTEXITCODE -ne 0) { throw "pnpm install failed: $LASTEXITCODE" }
  pnpm lint
  if ($LASTEXITCODE -ne 0) { throw "pnpm lint failed: $LASTEXITCODE" }
  pnpm typecheck
  if ($LASTEXITCODE -ne 0) { throw "pnpm typecheck failed: $LASTEXITCODE" }
  pnpm test
  if ($LASTEXITCODE -ne 0) { throw "pnpm test failed: $LASTEXITCODE" }
  pnpm build
  if ($LASTEXITCODE -ne 0) { throw "pnpm build failed: $LASTEXITCODE" }
}
finally { Pop-Location }

Invoke-Maven @('-pl','apex-sdk-webmvc,apex-operations-console,apex-sdk-operations,apex-sdk-observability,apex-sdk-grpc-server,apex-sdk-messaging,apex-order-contracts','-am','install','-DskipTests') 'install-prereq.log'
Invoke-Maven @('-f','apex-identity-server/pom.xml','install','-DskipTests') 'identity-install.log'
Invoke-Maven @('-f','apex-operations-console/pom.xml','test','-Dtest=OpsM36bResourceAuthorizerTest,OpsM36aJwtValidatorTest,OpsM36aHttpSecurityIT,OwnerRecentSearchClientTest') 'console-m36b-unit.log'

$wt = Join-Path $env:TEMP ("m36b-wt-" + [guid]::NewGuid().ToString('N').Substring(0, 8))
Write-Host "Creating owned packaging worktree at $wt"
git worktree add --detach $wt HEAD
if ($LASTEXITCODE -ne 0) { throw "git worktree add failed" }

try {
  $syncPaths = @(
    'inventory-cell/src/test/java/com/apex/inventory/OpsM36bLiveIT.java',
    'inventory-cell/src/test/java/com/apex/inventory/OpsM36a1AuthenticatedLiveIT.java',
    'inventory-cell/src/test/java/com/apex/inventory/m36a',
    'inventory-cell/src/test/java/com/apex/inventory/Ops04bLiveUiBridgeIT.java',
    'inventory-cell/pom.xml',
    'apex-operations-console',
    'apps/operations-web',
    'tools/m36b'
  )
  foreach ($rel in $syncPaths) {
    $src = Join-Path $root $rel
    $dst = Join-Path $wt $rel
    if (-not (Test-Path $src)) { continue }
    if (Test-Path $src -PathType Container) {
      New-Item -ItemType Directory -Force -Path $dst | Out-Null
      robocopy $src $dst /E /XD node_modules .next test-results target /NFL /NDL /NJH /NJS /nc /ns /np | Out-Null
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

  $jarDir = Join-Path $root 'out\m36b\jars'
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
  $sha | ConvertTo-Json | Set-Content (Join-Path $root 'out\m36b\build-receipt.json')

  # Sync built .next from workspace into worktree is unnecessary — Next runs from workspace apps/operations-web
  Remove-Item Env:APEX_OPS_UI_FIXTURE_MODE -ErrorAction SilentlyContinue
  $env:APEX_OPS_UI_FIXTURE_MODE = 'false'
  Invoke-Maven @(
    '-f','inventory-cell/pom.xml','test',
    '-Dtest=OpsM36bLiveIT',
    "-Dsaga.repositoryRoot=$root",
    "-Dsaga.inventoryJar=$jarDir\inventory-cell.jar",
    "-Dsaga.orderJar=$jarDir\order-cell.jar",
    "-Dsaga.paymentJar=$jarDir\payment-cell.jar",
    "-Dsaga.consoleJar=$jarDir\apex-operations-console-1.0-SNAPSHOT.jar",
    "-Dsaga.identityJar=$jarDir\apex-identity-server.jar"
  ) 'm36b-live-matrix.log' $wt

  Write-Host 'M36B live matrix Maven exit 0'
  Get-Content (Join-Path $root 'out\m36b\live-matrix-receipt.json') -ErrorAction SilentlyContinue
}
finally {
  Set-Location $root
  git worktree remove --force $wt 2>$null
  if (Test-Path $wt) {
    Remove-Item -Recurse -Force $wt -ErrorAction SilentlyContinue
  }
}
