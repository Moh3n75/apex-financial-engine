param(
    [long]$Seed = 20261109,
    [ValidateRange(10,1000)][int]$Orders = 100,
    [ValidateRange(0.001,0.999)][double]$FailureRate = 0.10,
    [string]$MavenCommand = 'mvn',
    [switch]$Offline
)
$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$modules = @('apex-platform-core','apex-platform-events','apex-spring-boot-starter','apex-platform-grpc-api',
    'apex-platform-messaging','apex-starter-webmvc','apex-starter-cell-messaging','order-cell')
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


$run = Join-Path $repositoryRoot ('out/saga-chaos-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Path $run -Force | Out-Null
$commands = [Collections.Generic.List[object]]::new()
function Invoke-ChaosMaven([string]$Name,[string[]]$Arguments) {
    $options = @('-B','-ntp'); if ($Offline) { $options += '-o' }
    $log = Join-Path $run ($Name + '.log')
    & $MavenCommand @options @Arguments *> $log
    $code = $LASTEXITCODE
    $commands.Add(@{name=$Name; executable=$MavenCommand; arguments=($options + $Arguments); exitCode=$code; log=$log})
    Get-Content -LiteralPath $log -Tail 12 | Out-Host
    return $code
}
$saved = @{}
Get-ChildItem Env: | Where-Object { $_.Name -match '^(SPRING_|APEX_|CELL_|ORDER_|INVENTORY_|PAYMENT_|KAFKA_|SERVER_|REPLAY_|EUREKA_|JAVA_TOOL_OPTIONS$|JDK_JAVA_OPTIONS$|_JAVA_OPTIONS$|MAVEN_OPTS$|MAVEN_ARGS$)' } |
    ForEach-Object { $saved[$_.Name]=$_.Value; Remove-Item -LiteralPath "Env:$($_.Name)" }
$env:EUREKA_CLIENT_ENABLED = 'false'
Push-Location $repositoryRoot
$failure = $null
try {
    $before = Get-SourceFingerprint
    $platform = 'apex-starter-webmvc,apex-starter-cell-messaging,apex-starter-grpc-server,apex-discovery-eureka,apex-operations-owner-jdbc,apex-starter-observability,apex-order-contracts'
    if ((Invoke-ChaosMaven 'platform-build' @('-pl',$platform,'-am','clean','install','-DskipTests')) -ne 0) { throw 'Platform build failed' }
    foreach ($cell in @('order-cell','payment-cell','apex-replay-tool','inventory-cell')) {
        $skip = if($cell -eq 'inventory-cell'){'-DskipTests'}else{'-Dmaven.test.skip=true'}
        if ((Invoke-ChaosMaven ($cell+'-build') @('-f',"$cell/pom.xml",'clean','package',$skip)) -ne 0) { throw "$cell build failed" }
    }
    if ((Get-SourceFingerprint) -ne $before) { throw 'Sources changed during build' }
    $receipt=@('schemaVersion=1',"sourceSha256=$before")
    foreach($module in $modules) {
        $jar=if($module -eq 'order-cell'){'order-cell/target/order-cell.jar'}else{"$module/target/$module-1.0-SNAPSHOT.jar"}
        $receipt += "$module.sha256=" + (Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash.ToLowerInvariant()
    }
    [IO.File]::WriteAllLines((Join-Path $repositoryRoot 'out/m26-order-build.properties'),$receipt,[Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllLines((Join-Path $repositoryRoot 'out/m27-payment-build.properties'),@('schemaVersion=1',
        ('paymentJarSha256='+(Get-FileHash -LiteralPath payment-cell/target/payment-cell.jar -Algorithm SHA256).Hash.ToLowerInvariant())),[Text.UTF8Encoding]::new($false))
    $common=@('-f','inventory-cell/pom.xml','test',"-Dsaga.repositoryRoot=$repositoryRoot",
        "-Dsaga.inventoryJar=$repositoryRoot/inventory-cell/target/inventory-cell.jar")
    $rateText=$FailureRate.ToString([Globalization.CultureInfo]::InvariantCulture)
    $chaos=Invoke-ChaosMaven 'chaos' ($common+@('-Dtest=SagaChaosIT',"-Dsaga.chaos.seed=$Seed","-Dsaga.chaos.orders=$Orders","-Dsaga.chaos.failureRate=$rateText"))
    $chaosEvidence=Join-Path $repositoryRoot 'inventory-cell/target/saga-chaos-evidence.json'
    if(Test-Path -LiteralPath $chaosEvidence){Copy-Item -LiteralPath $chaosEvidence -Destination (Join-Path $run 'saga-chaos-evidence.json')}
    $regression=Invoke-ChaosMaven 'regressions' ($common+@('-Dtest=OperationalRecoverySagaIT,SagaPerformanceIT,OrderInventorySagaIT'))
    if($chaos -ne 0 -or $regression -ne 0){throw 'Chaos or regression verification failed; inspect JSON and logs'}
} catch { $failure=$_.Exception.Message; throw }
finally {
    $suites = @()
    foreach($name in @('SagaChaosIT','OperationalRecoverySagaIT','SagaPerformanceIT','OrderInventorySagaIT')) {
        $xmlPath = Join-Path $repositoryRoot "inventory-cell/target/surefire-reports/TEST-com.apex.inventory.$name.xml"
        if(Test-Path -LiteralPath $xmlPath) {
            [xml]$xml = Get-Content -LiteralPath $xmlPath -Raw
            $suite = $xml.testsuite
            $assertions = @($suite.testcase | ForEach-Object {
                if($_.failure){$_.failure.InnerText}; if($_.error){$_.error.InnerText}
            })
            $suites += @{name=$name;tests=[int]$suite.tests;failures=[int]$suite.failures;
                errors=[int]$suite.errors;skipped=[int]$suite.skipped;failedAssertions=$assertions;report=$xmlPath}
            Copy-Item -LiteralPath $xmlPath -Destination $run
        }
    }
    Get-ChildItem -LiteralPath (Join-Path $repositoryRoot 'inventory-cell/target') -File -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -like 'chaos-*.log' -or $_.Name -eq 'saga-chaos-evidence.json' } |
        Copy-Item -Destination $run -Force
    @{seed=$Seed;orders=$Orders;failureRate=$FailureRate;commands=$commands.ToArray();failure=$failure;
      suites=$suites;result=$(if($failure){'FAIL'}else{'PASS'});evidenceDirectory=$run} |
        ConvertTo-Json -Depth 16 | Set-Content -LiteralPath (Join-Path $run 'execution.json') -Encoding UTF8
    Pop-Location
    Remove-Item -LiteralPath Env:EUREKA_CLIENT_ENABLED -ErrorAction SilentlyContinue
    foreach($name in $saved.Keys){Set-Item -LiteralPath "Env:$name" -Value $saved[$name]}
    Write-Host "Evidence: $run"
}
