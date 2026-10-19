param(
    [string]$MavenCommand = 'mvn',
    [switch]$Offline
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$evidencePath = Join-Path $repositoryRoot 'out/m22-build-evidence.properties'
$modules = @('apex-platform-core', 'apex-platform-events', 'apex-platform-context-autoconfigure',
    'apex-platform-webmvc', 'apex-platform-messaging', 'apex-starter-webmvc',
    'apex-starter-cell-messaging', 'customer-cell')

function Get-SourceFingerprint {
    $paths = [System.Collections.Generic.List[string]]::new()
    $paths.Add('pom.xml')
    $paths.Add('tools/m22/verify-cells.ps1')
    foreach ($module in $modules) {
        $paths.Add("$module/pom.xml")
        $mainSources = Join-Path $repositoryRoot "$module/src/main"
        if (Test-Path -LiteralPath $mainSources) {
            Get-ChildItem -LiteralPath $mainSources -File -Recurse |
                ForEach-Object { $paths.Add($_.FullName.Substring($repositoryRoot.Length + 1).Replace('\', '/')) }
        }
    }
    $orderedPaths = $paths.ToArray()
    [Array]::Sort($orderedPaths, [StringComparer]::Ordinal)
    $text = [Text.StringBuilder]::new()
    foreach ($path in $orderedPaths) {
        $hash = (Get-FileHash -LiteralPath (Join-Path $repositoryRoot $path) -Algorithm SHA256).Hash.ToLowerInvariant()
        [void]$text.Append($path).Append("`n").Append($hash).Append("`n")
    }
    $sha = [Security.Cryptography.SHA256]::Create()
    try { return ([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($text.ToString())))).Replace('-', '').ToLowerInvariant() }
    finally { $sha.Dispose() }
}

function Invoke-CellMaven([string]$directory, [string[]]$arguments) {
    Push-Location $directory
    try {
        $options = @('-B', '-ntp')
        if ($Offline) { $options += '-o' }
        & $MavenCommand @options @arguments
        if ($LASTEXITCODE -ne 0) { throw "Maven failed in $directory (exit $LASTEXITCODE)" }
    } finally { Pop-Location }
}

# Remove inherited application/JVM overrides for these child builds only. Restore
# the caller's environment even on failure; no shared endpoint is probed here.
$savedEnvironment = @{}
Get-ChildItem Env: | Where-Object {
    $_.Name -match '^(SPRING_|CELL_|APEX_|KAFKA_|SERVER_PORT$|JAVA_TOOL_OPTIONS$|JDK_JAVA_OPTIONS$|_JAVA_OPTIONS$|MAVEN_OPTS$|MAVEN_ARGS$)'
} | ForEach-Object {
    $savedEnvironment[$_.Name] = $_.Value
    [Environment]::SetEnvironmentVariable($_.Name, $null, 'Process')
}
try {
    if (-not $env:JAVA_HOME) { throw 'Set JAVA_HOME to a Java 21 JDK before verification.' }
    $javaVersion = & (Join-Path $env:JAVA_HOME 'bin/java') --version | Out-String
    if ($LASTEXITCODE -ne 0 -or $javaVersion -notmatch '(?m)^(java|openjdk) 21[\. ]') { throw 'Verification requires Java 21.' }
    New-Item -ItemType Directory -Path (Split-Path $evidencePath) -Force | Out-Null
    $fingerprint = Get-SourceFingerprint
    Invoke-CellMaven $repositoryRoot @('-pl', 'apex-starter-webmvc,apex-starter-cell-messaging', '-am', 'clean', 'install')
    Invoke-CellMaven (Join-Path $repositoryRoot 'customer-cell') @('clean', 'verify')
    if ((Get-SourceFingerprint) -ne $fingerprint) { throw 'Source changed during the build; rerun verification.' }
    $evidence = @('schemaVersion=1', "sourceSha256=$fingerprint")
    foreach ($module in $modules) {
        $jar = if ($module -eq 'customer-cell') { 'customer-cell/target/customer-cell.jar' }
               else { "$module/target/$module-1.0-SNAPSHOT.jar" }
        $hash = (Get-FileHash -LiteralPath (Join-Path $repositoryRoot $jar) -Algorithm SHA256).Hash.ToLowerInvariant()
        $evidence += "$module.sha256=$hash"
    }
    [IO.File]::WriteAllLines($evidencePath, $evidence, [Text.UTF8Encoding]::new($false))
    Invoke-CellMaven (Join-Path $repositoryRoot 'reporting-cell') @('clean', 'verify')
    if ((Get-SourceFingerprint) -ne $fingerprint) { throw 'Source changed during verification; results are invalid.' }
} finally {
    foreach ($name in $savedEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process')
    }
}
