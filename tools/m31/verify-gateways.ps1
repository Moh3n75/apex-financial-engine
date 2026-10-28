param(
    [string]$MavenCommand = 'mvn.cmd',
    [switch]$Offline,
    [switch]$IncludeM30Regression
)
$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Push-Location $repositoryRoot
try {
    if (-not $env:JAVA_HOME) { throw 'Set JAVA_HOME to a Java 21 JDK.' }
    $version = & (Join-Path $env:JAVA_HOME 'bin/java') --version | Out-String
    if ($LASTEXITCODE -ne 0 -or $version -notmatch '(?m)^(java|openjdk) 21[\. ]') { throw 'Java 21 required.' }
    $options = @('-B','-ntp'); if ($Offline) { $options += '-o' }
    function Invoke-M31Maven([string[]]$goals) {
        & $MavenCommand @options @goals
        if ($LASTEXITCODE -ne 0) { throw "M31 Maven failed (exit $LASTEXITCODE)." }
    }
    Invoke-M31Maven @('-pl','apex-starter-grpc-server,apex-starter-grpc-client,apex-order-contracts','-am','clean','install')
    Invoke-M31Maven @('-f','sample-grpc-client/pom.xml','clean','verify')
    Invoke-M31Maven @('-f','order-cell/pom.xml','-Pgrpc-acceptance','clean','verify')
    # The old regression runner correctly rebuilds Order. Preserve the completed
    # gRPC acceptance evidence before that clean; never reuse its packaged JAR.
    New-Item -ItemType Directory -Path out/m31-acceptance -Force | Out-Null
    Copy-Item order-cell/target/grpc-e2e-reports/TEST-*.xml out/m31-acceptance -Force
    Copy-Item order-cell/target/grpc-e2e-evidence.json out/m31-acceptance -Force
    Copy-Item order-cell/target/grpc-*-process.log out/m31-acceptance -Force
    Copy-Item order-cell/target/grpc-order-restarted.log out/m31-acceptance -Force
    if ($IncludeM30Regression) {
        $runner = @('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/m30/verify-m30.ps1','-MavenCommand',$MavenCommand)
        if ($Offline) { $runner += '-Offline' }
        & powershell.exe @runner
        if ($LASTEXITCODE -ne 0) { throw 'M30 Financial/M22/M26/M27/M29/performance regression failed.' }
    }
} finally { Pop-Location }
