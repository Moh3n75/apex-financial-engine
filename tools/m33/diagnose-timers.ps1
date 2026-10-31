param([ValidateRange(1,10000)][int]$Iterations = 1000)
$ErrorActionPreference = 'Stop'
if (-not $env:JAVA_HOME) { throw 'Set JAVA_HOME to an installed Java 21 JDK.' }
$java = Join-Path $env:JAVA_HOME 'bin/java.exe'
$version = & $java --version | Out-String
if ($LASTEXITCODE -ne 0 -or $version -notmatch '(?m)^(java|openjdk) 21[\. ]') {
    throw 'Java 21 required.'
}
# Java source-file launch: no Maven install, network, DB, Kafka, or application startup.
& $java (Join-Path $PSScriptRoot 'ScheduledTimerProbe.java') $Iterations
exit $LASTEXITCODE
