$ErrorActionPreference = 'Stop'
$DemoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$DemoOut = Join-Path $DemoRoot 'out\demo'
function Read-DemoManifest {
    $pointer = Join-Path $DemoOut 'current.txt'
    if (-not (Test-Path -LiteralPath $pointer)) { throw 'No demo has been launched. Run start-apex-demo.ps1.' }
    $directory = [IO.Path]::GetFullPath((Get-Content -LiteralPath $pointer -Raw).Trim())
    if (-not $directory.StartsWith($DemoOut + '\', [StringComparison]::OrdinalIgnoreCase)) { throw 'Invalid demo directory.' }
    Get-Content -LiteralPath (Join-Path $directory 'manifest.json') -Raw | ConvertFrom-Json
}
function Test-DemoIdentity($Identity) {
    $p = Get-Process -Id $Identity.pid -ErrorAction SilentlyContinue
    if (-not $p) { return $false }
    $expected = [DateTimeOffset]::Parse($Identity.startTime).UtcDateTime
    return [Math]::Abs(($p.StartTime.ToUniversalTime() - $expected).TotalMilliseconds) -lt 1500
}
function Get-DemoTools {
    $java = 'C:\Program Files\Java\jdk-21'
    if (-not (Test-Path "$java\bin\java.exe") -and $env:JAVA_HOME) { $java = $env:JAVA_HOME }
    $node = 'C:\Program Files\nodejs\node.exe'
    $pnpm = Join-Path $env:USERPROFILE '.local-node\pnpm.cmd'
    $mvn = Get-ChildItem (Join-Path $env:USERPROFILE '.m2\wrapper\dists') -Filter mvn.cmd -Recurse |
        Where-Object { $_.FullName -match 'apache-maven-3\.9\.' } | Sort-Object FullName -Descending | Select-Object -ExpandProperty FullName -First 1
    if (-not $mvn) { $mvn = Get-Command mvn.cmd -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -First 1 }
    foreach ($p in @("$java\bin\java.exe", $node, $pnpm, $mvn)) { if (-not $p -or -not (Test-Path -LiteralPath $p)) { throw "Missing demo tool: $p" } }
    $env:JAVA_HOME = $java
    $env:Path = "$java\bin;$(Split-Path $node);$(Split-Path $pnpm);$env:Path"
    $version = (& "$java\bin\java.exe" --version | Out-String)
    if ($version -notmatch '(java|openjdk) 21(\.|\s)') { throw 'Demo requires Java 21.' }
    if ((& $node --version) -notmatch '^v20\.') { throw 'Demo requires Node 20.' }
    if ((& $mvn --version | Out-String) -notmatch 'Apache Maven 3\.9\.') { throw 'Demo requires Maven 3.9.x.' }
    if ((& $pnpm --version) -notmatch '^9\.') { throw 'Demo requires pnpm 9.x.' }
    return @{ Java = "$java\bin\java.exe"; Maven = $mvn; Node = $node; Pnpm = $pnpm }
}
