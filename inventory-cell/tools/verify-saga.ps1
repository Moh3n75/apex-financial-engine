param(
    [string]$MavenCommand = 'mvn',
    [switch]$Offline
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$modules = @('apex-platform-core', 'apex-platform-events', 'apex-spring-boot-starter', 'apex-platform-grpc-api',
     'apex-platform-messaging', 'apex-starter-webmvc',
    'apex-starter-cell-messaging', 'order-cell')

function Get-SourceFingerprint {
    $paths = [System.Collections.Generic.List[string]]::new()
    $paths.Add('pom.xml')
    $paths.Add('inventory-cell/tools/verify-saga.ps1')
    foreach ($module in $modules) {
        $paths.Add("$module/pom.xml")
        Get-ChildItem -LiteralPath (Join-Path $repositoryRoot "$module/src/main") -Recurse -File -ErrorAction SilentlyContinue |
            ForEach-Object { $paths.Add($_.FullName.Substring($repositoryRoot.Length + 1).Replace('\', '/')) }
    }
    $ordered = $paths.ToArray()
    [Array]::Sort($ordered, [StringComparer]::Ordinal)
    $text = [Text.StringBuilder]::new()
    foreach ($path in $ordered) {
        $hash = (Get-FileHash -LiteralPath (Join-Path $repositoryRoot $path) -Algorithm SHA256).Hash.ToLowerInvariant()
        [void]$text.Append($path).Append("`n").Append($hash).Append("`n")
    }
    $sha = [Security.Cryptography.SHA256]::Create()
    try { return ([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($text.ToString())))).Replace('-', '').ToLowerInvariant() }
    finally { $sha.Dispose() }
}

function Invoke-Maven([string[]]$arguments) {
    $options = @('-B', '-ntp')
    if ($Offline) { $options += '-o' }
    & $MavenCommand @options @arguments
    if ($LASTEXITCODE -ne 0) { throw "Saga verification Maven failure (exit $LASTEXITCODE)" }
}

# Sanitize only this build process, restoring the caller's environment afterward.
$saved = @{}
Get-ChildItem Env: | Where-Object { $_.Name -match '^(SPRING_|APEX_|CELL_|ORDER_|INVENTORY_|KAFKA_|SERVER_|JAVA_TOOL_OPTIONS$|JDK_JAVA_OPTIONS$|_JAVA_OPTIONS$|MAVEN_OPTS$)' } |
    ForEach-Object { $saved[$_.Name] = $_.Value; Remove-Item -LiteralPath "Env:$($_.Name)" }
Push-Location $repositoryRoot
try {
    New-Item -ItemType Directory -Path out -Force | Out-Null
    $before = Get-SourceFingerprint
    Invoke-Maven @('-pl', 'apex-starter-webmvc,apex-starter-cell-messaging', '-am', 'clean', 'install')
    Invoke-Maven @('-f', 'order-cell/pom.xml', 'clean', 'verify')
    if ((Get-SourceFingerprint) -ne $before) { throw 'Order/Platform source changed during build' }
    $receipt = @('schemaVersion=1', "sourceSha256=$before")
    foreach ($module in $modules) {
        $relative = if ($module -eq 'order-cell') { 'order-cell/target/order-cell.jar' }
                    else { "$module/target/$module-1.0-SNAPSHOT.jar" }
        $hash = (Get-FileHash -LiteralPath $relative -Algorithm SHA256).Hash.ToLowerInvariant()
        $receipt += "$module.sha256=$hash"
    }
    [IO.File]::WriteAllLines((Join-Path $repositoryRoot 'out/m26-order-build.properties'), $receipt,
        [Text.UTF8Encoding]::new($false))
    Invoke-Maven @('-f', 'apex-replay-tool/pom.xml', 'clean', 'verify')
    Invoke-Maven @('-f', 'inventory-cell/pom.xml', 'clean', 'verify')
    if ((Get-SourceFingerprint) -ne $before) { throw 'Order/Platform source changed during E2E' }
} finally {
    Pop-Location
    foreach ($name in $saved.Keys) { Set-Item -LiteralPath "Env:$name" -Value $saved[$name] }
}
