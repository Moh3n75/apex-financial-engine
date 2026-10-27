param(
    [string]$MavenCommand = 'mvn.cmd',
    [ValidateSet('All','Financial','Performance')][string]$Phase = 'All',
    [switch]$Offline
)
$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Push-Location $repositoryRoot
try {
    if (-not $env:JAVA_HOME) { throw 'Set JAVA_HOME to a Java 21 JDK.' }
    $javaVersion = & (Join-Path $env:JAVA_HOME 'bin/java') --version | Out-String
    if ($LASTEXITCODE -ne 0 -or $javaVersion -notmatch '(?m)^(java|openjdk) 21[\. ]') { throw 'Java 21 required.' }
    $options = @('-B','-ntp'); if ($Offline) { $options += '-o' }
    New-Item -ItemType Directory -Path out -Force | Out-Null
    function Invoke-M30Maven([string[]]$goals) {
        & $MavenCommand @options @goals
        if ($LASTEXITCODE -ne 0) { throw "M30 Maven failed (exit $LASTEXITCODE)" }
    }
    if ($Phase -in @('All','Financial')) {
        Invoke-M30Maven @('-pl','apex-starter-webmvc,apex-starter-cell-messaging','-am','clean','install')
        $resolvedMaven = (Get-Command $MavenCommand -ErrorAction Stop).Source
        # The test owns native PostgreSQL, applies Financial V1..V16, and launches
        # bounded nested reactor test-compile/focused tests with only its DB URL.
        Invoke-M30Maven @('-f','inventory-cell/pom.xml','-Pm30','test-compile',
            'surefire:test@financial-isolated-build',"-Dm30.maven=$resolvedMaven")
        New-Item -ItemType Directory -Path out/m30-financial-fixture-reports -Force | Out-Null
        Copy-Item -Path inventory-cell/target/financial-build-reports/TEST-*.xml -Destination out/m30-financial-fixture-reports -Force
    }
    if ($Phase -eq 'All') {
        $runner = @('-NoProfile','-ExecutionPolicy','Bypass','-File',
            'inventory-cell/tools/verify-operational-recovery.ps1','-MavenCommand',$MavenCommand)
        if ($Offline) { $runner += '-Offline' }
        & powershell.exe @runner
        if ($LASTEXITCODE -ne 0) { throw 'M22/M26/M27/M29 regression failed.' }
    }
    if ($Phase -in @('All','Performance')) {
        # Requires the current packaged artifacts and build receipts, validated
        # by the unchanged packaged fixture before any process is launched.
        Invoke-M30Maven @('-f','inventory-cell/pom.xml','-Pm30','test-compile','surefire:test@saga-performance')
    }
} finally { Pop-Location }
