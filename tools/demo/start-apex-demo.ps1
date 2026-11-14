param([ValidateSet('Preview')][string]$Mode = 'Preview', [switch]$OpenBrowser)
. "$PSScriptRoot\common.ps1"
$tools = Get-DemoTools
if (Test-Path (Join-Path $DemoOut 'current.txt')) {
    try { $old = Read-DemoManifest } catch { $old = $null }
    if ($old -and (Test-DemoIdentity $old.supervisor)) { throw 'An owned demo is already running. Use status or stop first.' }
}
foreach ($port in @(3000, 9190)) {
    $socket = New-Object Net.Sockets.TcpListener([Net.IPAddress]::Loopback, $port)
    try { $socket.Start() } catch { throw "Port $port is busy. No process was stopped." } finally { $socket.Stop() }
}
$run = Join-Path $DemoOut ('r-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Path $run -Force | Out-Null
# Every invocation snapshots CURRENT tracked/nonignored source, including owner patches.
# No original target, .env, credentials, reports, index or user configuration is overwritten.
$source = Join-Path $run 's'
$paths = @(git -C $DemoRoot ls-files --cached --others --exclude-standard) | Sort-Object -Unique
$hashes = @()
foreach ($relative in $paths) {
    if ($relative -match '(^|/)(\.env[^/]*|\.git|node_modules|target|out|\.next|\.idea|\.agents|\.codex|\.aws)(/|$)') { continue }
    $from = Join-Path $DemoRoot $relative
    if (-not (Test-Path -LiteralPath $from -PathType Leaf)) { continue }
    $to = Join-Path $source $relative
    New-Item -ItemType Directory -Force -Path (Split-Path $to) | Out-Null
    Copy-Item -LiteralPath $from -Destination $to
    $hashes += @{ path = $relative; sha256 = (Get-FileHash -LiteralPath $to -Algorithm SHA256).Hash }
}
@{ head = (git -C $DemoRoot rev-parse HEAD); status = @(git -C $DemoRoot status --porcelain=v1); files = $hashes } |
    ConvertTo-Json -Depth 6 | Set-Content (Join-Path $run 'source-manifest.json') -Encoding UTF8
$repo = Join-Path $DemoOut 'm2'
if (-not (Test-Path $repo)) {
    New-Item -ItemType Directory -Force -Path $repo | Out-Null
    # Cached third-party jars only; rebuild all APEX libraries from this snapshot.
    $cache = Join-Path $env:USERPROFILE '.m2\repository'
    robocopy $cache $repo /E /XD (Join-Path $cache 'com\apex') /NFL /NDL /NJH /NJS /NP | Out-Null
    if ($LASTEXITCODE -ge 8) { throw 'Third-party cache copy failed.' }
}
function Build([string[]]$Arguments, [string]$Name) {
    Write-Host "Building $Name (log: $run\$Name.log)"
    Push-Location $source
    try {
        $ErrorActionPreference = 'Continue' # Native stderr is a log stream; the exit code decides failure.
        & $tools.Maven "-Dmaven.repo.local=$repo" @Arguments *> (Join-Path $run "$Name.log")
        $ErrorActionPreference = 'Stop'
        if ($LASTEXITCODE -ne 0) { throw "Build $Name failed, exit $LASTEXITCODE. See $run\$Name.log" }
    } finally { Pop-Location }
}
Build @('-pl','apex-sdk-webmvc,apex-operations-console,apex-sdk-operations,apex-sdk-observability,apex-sdk-grpc-server,apex-sdk-messaging,apex-order-contracts','-am','install','-DskipTests') 'sdk'
foreach ($cell in @('order-cell','inventory-cell','payment-cell')) { Build @('-f',"$cell/pom.xml",'package','-DskipTests') $cell }
Build @('-f','inventory-cell/pom.xml','dependency:build-classpath','-DincludeScope=test',"-Dmdep.outputFile=$run\classpath.txt") 'classpath'
$web = Join-Path $source 'apps\operations-web'
$dependencies = Join-Path $DemoRoot 'apps\operations-web\node_modules'
if (-not (Test-Path "$dependencies\next\dist\bin\next")) { throw 'Install web dependencies first: pnpm install --frozen-lockfile in apps/operations-web.' }
New-Item -ItemType Junction -Path (Join-Path $web 'node_modules') -Target $dependencies | Out-Null
$env:APEX_OPERATIONS_API_ORIGIN = 'http://127.0.0.1:9190'
$env:APEX_OPS_UI_FIXTURE_MODE = 'false'; $env:OPS04B_REQUIRE_LIVE = 'true'
$env:APEX_OPS_M36A_ENABLED = 'false'; $env:APEX_OPS_M36B_ENABLED = 'false'
Write-Host "Building Next.js (log: $run\web-build.log)"
Push-Location $web
try {
    $ErrorActionPreference = 'Continue'
    & $tools.Node "$web\node_modules\next\dist\bin\next" build --webpack *> (Join-Path $run 'web-build.log')
    $ErrorActionPreference = 'Stop'
    if ($LASTEXITCODE -ne 0) { throw "Next build failed. See $run\web-build.log" }
} finally { Pop-Location }
$cp = "$source\inventory-cell\target\test-classes;$source\inventory-cell\target\classes;" + (Get-Content (Join-Path $run 'classpath.txt') -Raw).Trim()
# An argument file avoids Windows command-line limits; it contains no secrets.
$javaArgs = @('-Xmx512m','-cp', ('"' + $cp.Replace('\','/') + '"'), 'com.apex.inventory.ApexDemoSupervisor', ('"' + $source.Replace('\','/') + '"'), ('"' + $run.Replace('\','/') + '"'))
[IO.File]::WriteAllLines((Join-Path $run 'java.args'), $javaArgs)
$supervisor = Start-Process -FilePath $tools.Java -ArgumentList ('@"' + (Join-Path $run 'java.args') + '"') -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $run 'supervisor.log') -RedirectStandardError (Join-Path $run 'supervisor-error.log')
[IO.File]::WriteAllText((Join-Path $DemoOut 'current.txt'), $run)
Write-Host 'Starting owned PostgreSQL/KRaft, three Cells, Console and Next. Preview: NO M36 AUTH, LOCAL ONLY.'
$deadline = (Get-Date).AddMinutes(5)
do {
    Start-Sleep -Seconds 2
    $supervisor.Refresh()
    if ($supervisor.HasExited) { throw "Supervisor exited ($($supervisor.ExitCode)). Logs: $run" }
    $manifest = $null
    if (Test-Path (Join-Path $run 'manifest.json')) { $manifest = Get-Content (Join-Path $run 'manifest.json') -Raw | ConvertFrom-Json }
    if ($manifest -and $manifest.phase -eq 'RUNNING') { break }
} while ((Get-Date) -lt $deadline)
if (-not $manifest -or $manifest.phase -ne 'RUNNING') { throw "Startup not ready. Inspect $run; status/stop scripts manage only owned processes." }
& "$PSScriptRoot\smoke-apex-demo.ps1"
if ($OpenBrowser) { Start-Process 'http://127.0.0.1:3000' }
Write-Host 'LIVE DEMO: http://127.0.0.1:3000 (NO M36 AUTH; synthetic inputs, real Cells/Kafka/PostgreSQL)'
Write-Host 'Use Events / Workflows / Cells. Create another order: .\tools\demo\create-demo-order.ps1'
Write-Host 'Partial recent current state only, no durable history. Stack stays running until .\tools\demo\stop-apex-demo.ps1'
