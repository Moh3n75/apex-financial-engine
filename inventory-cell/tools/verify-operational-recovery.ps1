param([string]$MavenCommand='mvn',[switch]$Offline)
$ErrorActionPreference='Stop'
$repositoryRoot=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Push-Location $repositoryRoot
try {
    $runner=@('-NoProfile','-ExecutionPolicy','Bypass','-File','inventory-cell/tools/verify-three-cell-saga.ps1','-MavenCommand',$MavenCommand)
    if($Offline){$runner+='-Offline'}
    & powershell.exe @runner
    if($LASTEXITCODE -ne 0){throw 'Existing M22/M26/M27/M28 verification failed'}
    $options=@('-B','-ntp'); if($Offline){$options+='-o'}
    & $MavenCommand @options -f inventory-cell/pom.xml -Poperational-recovery verify
    if($LASTEXITCODE -ne 0){throw 'M29 packaged operational recovery failed'}
} finally { Pop-Location }
