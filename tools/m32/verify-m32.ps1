param(
    [string]$MavenCommand = 'mvn.cmd',
    [switch]$Offline,
    [switch]$Focused
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Push-Location $root
try {
    if (-not $env:JAVA_HOME) { throw 'Set JAVA_HOME to a Java 21 JDK.' }
    $version = & (Join-Path $env:JAVA_HOME 'bin/java') --version | Out-String
    if ($LASTEXITCODE -ne 0 -or $version -notmatch '(?m)^(java|openjdk) 21[\. ]') { throw 'Java 21 required.' }
    $options = @('-B','-ntp'); if ($Offline) { $options += '-o' }
    New-Item -ItemType Directory -Path out -Force | Out-Null
    $started = [DateTimeOffset]::UtcNow.ToString('o')
    $modules = @('apex-platform-core','apex-spring-boot-starter','apex-platform-events',
        'apex-platform-messaging','apex-platform-grpc-api','apex-platform-grpc',
        'apex-starter-grpc-client','apex-starter-grpc-server','apex-starter-webmvc','apex-starter-cell-messaging',
        'apex-order-contracts','order-cell','inventory-cell','payment-cell','sample-grpc-client')
    function Source-Hashes {
        $paths = @('pom.xml','tools/m32/verify-m32.ps1')
        foreach ($module in $modules) {
            $paths += "$module/pom.xml"
            $paths += @(Get-ChildItem "$module/src/main" -Recurse -File -ErrorAction SilentlyContinue |
                ForEach-Object { $_.FullName.Substring($root.Length + 1).Replace('\','/') })
        }
        @(foreach ($path in ($paths | Sort-Object -Unique)) {
            [pscustomobject]@{path=$path;sha256=(Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()}
        })
    }
    function Invoke-M32Maven([string[]]$goals) {
        & $MavenCommand @options @goals
        if ($LASTEXITCODE -ne 0) { throw "M32 Maven failure (exit $LASTEXITCODE)." }
    }
    $before = Source-Hashes
    Invoke-M32Maven @('-pl','apex-starter-grpc-server,apex-starter-grpc-client,apex-order-contracts','-am','clean','install')
    if ($Focused) {
        # Development assembly + actual M32 E2E only. This explicitly does NOT
        # qualify the prior regression gate and is not the default acceptance.
        foreach ($cell in @('order-cell','inventory-cell','payment-cell','sample-grpc-client')) {
            Invoke-M32Maven @('-f',"$cell/pom.xml",'test-compile','jar:jar','spring-boot:repackage')
        }
    } else {
        $legacy = @('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/m31/verify-gateways.ps1',
            '-IncludeM30Regression','-MavenCommand',$MavenCommand)
        if ($Offline) { $legacy += '-Offline' }
        & powershell.exe @legacy
        if ($LASTEXITCODE -ne 0) { throw 'Complete M31 / M30 / Saga / Replay / Financial regression failed.' }
    }
    $after = Source-Hashes
    if (($before | ConvertTo-Json -Depth 4 -Compress) -ne ($after | ConvertTo-Json -Depth 4 -Compress)) {
        throw 'Production inputs changed during build; refuse a stale packaged test.'
    }
    $files = @($after)
    foreach ($cell in @('order-cell','inventory-cell','payment-cell','sample-grpc-client')) {
        $path = "$cell/target/$cell.jar"
        $files += [pscustomobject]@{path=$path;sha256=(Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()}
    }
    $receipt = [pscustomobject]@{startedUtc=$started;fullRegression=(-not $Focused);files=$files}
    [IO.File]::WriteAllText((Join-Path $root 'out/m32-build-receipt.json'),
        ($receipt | ConvertTo-Json -Depth 5), (New-Object Text.UTF8Encoding($false)))
    Invoke-M32Maven @('-f','order-cell/pom.xml','-Pm32-acceptance','test-compile','surefire:test@multi-responder-security')
    New-Item -ItemType Directory -Path out/m32-acceptance -Force | Out-Null
    Copy-Item order-cell/target/m32-reports/TEST-*.xml out/m32-acceptance -Force
    Copy-Item order-cell/target/m32-evidence/acceptance.json out/m32-acceptance -Force
    git diff --check
    if ($LASTEXITCODE -ne 0) { throw 'Git whitespace check failed.' }
} finally { Pop-Location }
