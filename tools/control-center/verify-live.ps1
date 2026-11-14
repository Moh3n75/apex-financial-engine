param(
  [string]$MavenCommand = (Join-Path $env:USERPROFILE '.m2/wrapper/dists/apache-maven-3.9.11-bin/6mqf5t809d9geo83kj4ttckcbc/apache-maven-3.9.11/bin/mvn.cmd'),
  [switch]$PrepareOnly,
  [switch]$RunPrepared,
  [switch]$Membership
)
$ErrorActionPreference = 'Stop'
$taskRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$taskOut = Join-Path $taskRoot 'out/control-center-live'
New-Item -ItemType Directory -Force -Path (Join-Path $taskOut 'jars') | Out-Null
$taskReceipt = @{ verdict='BLOCKED'; phase='PREPARING'; cellsContacted=@(); fullCI='NOT_QUALIFIED'; startedAt=[DateTime]::UtcNow.ToString('o') }
function Invoke-LiveMaven([string[]]$MavenArgs, [string]$LogName) {
  $taskPreference=$ErrorActionPreference
  try { $ErrorActionPreference='Continue'; & $MavenCommand @MavenArgs *> (Join-Path $taskOut $LogName); $taskCode=$LASTEXITCODE }
  finally { $ErrorActionPreference=$taskPreference }
  if ($taskCode -ne 0) { throw "Maven failed in $LogName; inspect private local diagnostics." }
}
Push-Location $taskRoot
try {
  if (-not $env:JAVA_HOME) { $env:JAVA_HOME='C:\Program Files\Java\jdk-21' }
  $env:Path="$env:JAVA_HOME\bin;$env:USERPROFILE\.local-node;$env:Path"
  foreach ($taskPort in @(3000,9190)) {
    if (Get-NetTCPConnection -State Listen -LocalPort $taskPort -ErrorAction SilentlyContinue) { throw "Required loopback port $taskPort is busy; foreign process left untouched." }
  }
  if (-not $RunPrepared) {
  $taskReceipt.phase='PREREQUISITES'
  Invoke-LiveMaven -MavenArgs @('-o','-q','-f','inventory-cell/pom.xml','test','-Dtest=ControlCenterLivePrerequisitesIT',"-Dsaga.repositoryRoot=$taskRoot") -LogName 'prerequisites.log'
  $taskReceipt.phase='BUILD'
  Invoke-LiveMaven -MavenArgs @('-o','-q','-pl','apex-sdk-discovery-eureka,apex-sdk-discovery-grpc,apex-sdk-operations,apex-sdk-grpc-server,apex-sdk-messaging,apex-sdk-webmvc,apex-sdk-operations,apex-sdk-observability,apex-order-contracts','-am','install','-DskipTests') -LogName 'install-platform.log'
  # Record the known full test-compilation problem separately, without deleting or editing it.
  & $MavenCommand -o -q -f order-cell/pom.xml test-compile *> (Join-Path $taskOut 'order-full-test-compile.log')
  $taskReceipt.orderFullTestCompileExit=$LASTEXITCODE
  foreach ($taskModule in @('order-cell','inventory-cell','payment-cell','customer-cell','reporting-cell','apex-operations-console','apex-identity-server','apex-discovery-server')) {
    $taskPom=Join-Path $taskRoot "$taskModule/pom.xml"
    $taskTemporary=Join-Path $taskRoot "$taskModule/.control-center-live-pom.xml"
    if (Test-Path -LiteralPath $taskTemporary) { throw "Temporary POM already exists for $taskModule" }
    $taskBuild=Join-Path $taskOut "build/$taskModule"
    $taskXml=(Get-Content -LiteralPath $taskPom -Raw).Replace('<build>', "<build><directory>$taskBuild</directory>")
    [System.IO.File]::WriteAllText($taskTemporary,$taskXml)
    try { Invoke-LiveMaven -MavenArgs @('-o','-q','-f',$taskTemporary,'package','-Dmaven.test.skip=true') -LogName "$taskModule-package.log" }
    finally { Remove-Item -LiteralPath $taskTemporary }
    $taskJar=Get-ChildItem -LiteralPath $taskBuild -Filter '*.jar' | Where-Object { $_.Length -gt 1MB -and $_.Name -notmatch 'tests' } | Select-Object -First 1
    if (-not $taskJar) { throw "No packaged JAR for $taskModule" }
    Copy-Item -LiteralPath $taskJar.FullName -Destination (Join-Path $taskOut "jars/$taskModule.jar")
    $taskReceipt["sha256:$taskModule"]=(Get-FileHash -LiteralPath $taskJar.FullName -Algorithm SHA256).Hash.ToLower()
  }
  }
  if ($PrepareOnly) { $taskReceipt.phase='PREPARED'; $taskReceipt.verdict='PREPARED_NOT_LIVE_ACCEPTANCE'; return }
  if ($RunPrepared) {
    foreach ($taskModule in @('order-cell','inventory-cell','payment-cell','customer-cell','reporting-cell','apex-operations-console','apex-identity-server')) {
      $taskJarPath=Join-Path $taskOut "jars/$taskModule.jar"
      if (-not (Test-Path -LiteralPath $taskJarPath)) { throw "Missing prepared artifact $taskModule" }
      $taskReceipt["sha256:$taskModule"]=(Get-FileHash -LiteralPath $taskJarPath -Algorithm SHA256).Hash.ToLower()
    }
    $taskReceipt.packaging='REUSED_EXPLICIT_PREPARED_ARTIFACTS'
  }
  $taskReceipt.phase='FRONTEND_BUILD'
  Push-Location (Join-Path $taskRoot 'apps/operations-web')
  try {
    $taskPreference=$ErrorActionPreference
    try { $ErrorActionPreference='Continue'; & pnpm.cmd build *> (Join-Path $taskOut 'web-build.log'); $taskWebExit=$LASTEXITCODE }
    finally { $ErrorActionPreference=$taskPreference }
    if ($taskWebExit -ne 0) { throw 'Frontend build failed.' }
  }
  finally { Pop-Location }
  $taskReceipt.phase='LIVE_TEST'
  Invoke-LiveMaven -MavenArgs @('-o','-q','-f','inventory-cell/pom.xml','test','-Dtest=ControlCenterFiveCellLiveIT',"-Dsaga.repositoryRoot=$taskRoot",("-Dcontrolcenter.membership="+$Membership.IsPresent.ToString().ToLower())) -LogName 'five-cell-live.log'
  $taskLive=Get-Content -LiteralPath (Join-Path $taskOut 'live-acceptance.json') -Raw | ConvertFrom-Json
  $taskReceipt.verdict=$taskLive.verdict
  $taskReceipt.cellsContacted=$taskLive.cellsContacted
  # Archive only reviewed API/manifest receipts, never private runtime logs, tokens or key material.
  $taskPublic=Join-Path $taskRoot 'docs/evidence/control-center-live'
  if($Membership){$taskPublic=Join-Path $taskRoot 'docs/evidence/control-center-membership'}
  New-Item -ItemType Directory -Force -Path $taskPublic | Out-Null
  $taskPublicFiles=@('live-acceptance.json','prerequisites.json','live-topology.json','denied-owner-topology.json','expired-unavailable-topology.json','missing-consumer-topology.json','wrong-cluster-topology.json')
  $taskPublicFiles+=@('order-cell','inventory-cell','payment-cell','customer-cell','reporting-cell') | ForEach-Object { "$_-live-manifest.json" }
  if($Membership){$taskPublicFiles=@('live-acceptance.json','prerequisites.json','live-topology.json','denied-owner-topology.json');$taskPublicFiles+=Get-ChildItem -LiteralPath $taskOut -Filter 'membership-*.json' | Select-Object -ExpandProperty Name}
  foreach ($taskFile in $taskPublicFiles) {
    $taskSource=Join-Path $taskOut $taskFile
    $taskJson=Get-Content -LiteralPath $taskSource -Raw
    if ($taskJson -match '"(password|client_secret|access_token|refresh_token|keystore|truststore)"\s*:|eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.') { throw 'Public receipt failed credential scan.' }
    Copy-Item -LiteralPath $taskSource -Destination (Join-Path $taskPublic $taskFile)
  }
  $taskReceipt.phase='COMPLETE'
} finally {
  $taskReceipt.completedAt=[DateTime]::UtcNow.ToString('o')
  $taskReceipt | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $taskOut 'launcher.json')
  Pop-Location
}
