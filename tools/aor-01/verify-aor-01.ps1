param([string]$MavenCommand='mvn.cmd',[string]$JavaHome=$env:JAVA_HOME)
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
if(-not $JavaHome){throw 'Supply JavaHome for JDK 21.'}
$env:JAVA_HOME=$JavaHome
$evidence=Join-Path $root ('out/aor-01/acceptance-'+[DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))
New-Item -ItemType Directory -Path $evidence -Force | Out-Null
function Run([string]$exe,[string[]]$arguments,[string]$name) {
    $ErrorActionPreference='Continue'
    & $exe @arguments *> (Join-Path $evidence "$name.log")
    $code=$LASTEXITCODE
    $ErrorActionPreference='Stop'
    if($code -ne 0){throw "$name failed ($code); see $evidence"}
    Write-Host "$name PASS"
}
Push-Location $root
try {
    Run 'powershell' @('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/sdk-0/verify-sdk.ps1','-Sdk1','-Runs','3','-EvidenceRoot','out/aor-01/repro','-MavenCommand',$MavenCommand) 'sdk'
    $modules=(Get-ChildItem -Directory -Filter 'apex-sdk-*' | Sort-Object Name | Select-Object -ExpandProperty Name) -join ','
    Run $MavenCommand @('-o','-B','-ntp','-pl',$modules,'-am','-DskipTests','install') 'install'
    Run 'powershell' @('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/sdk-1/verify-negative-controls.ps1') 'negative-controls'
    Run 'powershell' @('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/aor-01/verify-neutral-consumer.ps1','-MavenCommand',$MavenCommand) 'neutral'
    Run 'powershell' @('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/sdk-0/verify-installed-messaging.ps1','-MavenCommand',$MavenCommand) 'legacy-messaging'
    Run $MavenCommand @('-o','-B','-ntp','-f','apex-discovery-server/pom.xml','clean','package','-DskipTests') 'registry'
    $registry=Join-Path $root 'apex-discovery-server/target/apex-discovery-server.jar'
    Run 'powershell' @('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/m35-6/verify-external-consumer.ps1','-Offline','-Sdk1','-RegistryJar',$registry,'-MavenCommand',$MavenCommand) 'external-matrix'
    Write-Host "AOR-01 acceptance complete: $evidence"
} finally {Pop-Location}
