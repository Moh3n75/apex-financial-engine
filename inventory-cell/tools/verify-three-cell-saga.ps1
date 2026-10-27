param([string]$MavenCommand='mvn',[switch]$Offline)
$ErrorActionPreference='Stop'
$repositoryRoot=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Push-Location $repositoryRoot
try {
    $options=@('-B','-ntp'); if($Offline){$options+='-o'}
    $runner=@('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/m22/verify-cells.ps1','-MavenCommand',$MavenCommand)
    if($Offline){$runner+='-Offline'}
    & powershell.exe @runner
    if($LASTEXITCODE -ne 0){throw 'M22 regression failed'}
    $runner=@('-NoProfile','-ExecutionPolicy','Bypass','-File','inventory-cell/tools/verify-saga.ps1','-MavenCommand',$MavenCommand)
    if($Offline){$runner+='-Offline'}
    & powershell.exe @runner
    if($LASTEXITCODE -ne 0){throw 'M26 replay regression failed'}
    & $MavenCommand @options -f payment-cell/pom.xml clean verify
    if($LASTEXITCODE -ne 0){throw 'Payment verification failed'}
    $receipt=@('schemaVersion=1',('paymentJarSha256='+(Get-FileHash -LiteralPath payment-cell/target/payment-cell.jar -Algorithm SHA256).Hash.ToLowerInvariant()))
    [IO.File]::WriteAllLines((Join-Path $repositoryRoot 'out/m27-payment-build.properties'),$receipt,[Text.UTF8Encoding]::new($false))
    & $MavenCommand @options -f inventory-cell/pom.xml -Pthree-cell verify
    if($LASTEXITCODE -ne 0){throw 'Three-cell acceptance failed'}
} finally { Pop-Location }
