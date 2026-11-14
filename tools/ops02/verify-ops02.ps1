param([string]$MavenCommand='mvn.cmd',[switch]$Offline,
    [ValidateSet('Foundation','PackagedMeasurements','Full')][string]$Phase='Full')
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$evidence=Join-Path $root ('out/ops02/run-'+[DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))
New-Item -ItemType Directory -Path $evidence -Force|Out-Null
$options=@('-B','-ntp');if($Offline){$options+='-o'}
function Maven([string[]]$goals,[string]$log){
    $prior=$ErrorActionPreference;$ErrorActionPreference='Continue'
    & $MavenCommand @options @goals *> (Join-Path $evidence $log)
    $code=$LASTEXITCODE;$ErrorActionPreference=$prior
    if($code -ne 0){throw "Maven failed ($code): $log"}
}
Push-Location $root
try{
    if(-not $env:JAVA_HOME){throw 'Java 21 required.'}
    # Resource policy fixed before measurements. No owner-approved latency/CPU budget exists.
    [ordered]@{sourceHead=(& git rev-parse HEAD);policy='No business errors; count <=1024, accounted queue bytes <=1048576, active <=256. Overhead verdict INCONCLUSIVE without an approved numeric budget.';
        admissionSamples=30000;admissionRounds=3;matrix=@('disabled','healthy','disabled','failing','disabled');
        packaged='M30 unchanged default traffic and durability assertions; three owner DBs; four modes disabled/healthy/failing/disabled';startedUtc=[DateTimeOffset]::UtcNow.ToString('o')}|
        ConvertTo-Json -Depth 4|Set-Content (Join-Path $evidence 'predeclared-experiment.json')
    if($Phase -in @('Foundation','Full')){
        Maven @('-pl','apex-sdk-observability,apex-sdk-messaging,apex-sdk-grpc','-am','clean','install') 'foundation.log'
        Copy-Item apex-sdk-observability/target/ops02-admission.csv (Join-Path $evidence 'admission.csv')
    }
    if($Phase -in @('PackagedMeasurements','Full')){
        # Only this wrapper selects the disposable test-JAR profile. No default deployment changes.
        $launcher=Join-Path $evidence 'maven-acceptance.cmd'
        $absoluteMaven=(Get-Command $MavenCommand -ErrorAction Stop).Source
        @('@echo off',('call "'+$absoluteMaven+'" -Pops02-acceptance %*'),'exit /b %errorlevel%')|Set-Content $launcher
        $arguments=@('-NoProfile','-ExecutionPolicy','Bypass','-File','inventory-cell/tools/verify-three-cell-saga.ps1','-MavenCommand',$launcher)
        if($Offline){$arguments+='-Offline'}
        $ErrorActionPreference='Continue'
        & powershell.exe @arguments *> (Join-Path $evidence 'packaged-build-and-correctness.log')
        $code=$LASTEXITCODE;$ErrorActionPreference='Stop'
        if($code -ne 0){throw 'Packaged prerequisite/three-Cell acceptance failed.'}
        $index=0
        foreach($mode in @('disabled','healthy','failing','disabled')){
            $index++;$destination=Join-Path $evidence "$index-$mode";New-Item -ItemType Directory $destination|Out-Null
            Maven @('-f','inventory-cell/pom.xml','-Pops02-acceptance,m30','test-compile','surefire:test@saga-performance',"-Dops02.mode=$mode") "$index-$mode.log"
            Copy-Item inventory-cell/target/m30-performance-evidence.json $destination
            Get-ChildItem inventory-cell/target -Filter '*.ops02.properties'|Copy-Item -Destination $destination
            Copy-Item inventory-cell/target/performance-reports/TEST-*.xml $destination
            Copy-Item inventory-cell/target/m30-*.log $destination
        }
    }
    if($Phase -eq 'Full'){
        $arguments=@('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/m35-6/verify-platform-product.ps1','-MavenCommand',$MavenCommand)
        if($Offline){$arguments+='-Offline'}
        $ErrorActionPreference='Continue'
        & powershell.exe @arguments *> (Join-Path $evidence 'full-regression.log')
        $code=$LASTEXITCODE;$ErrorActionPreference='Stop'
        if($code -ne 0){throw 'Full supported product regression failed.'}
        Copy-Item apex-sdk-observability/target/ops02-admission.csv (Join-Path $evidence 'admission-final.csv')
    }
    [ordered]@{phase=$Phase;exit=0;finishedUtc=[DateTimeOffset]::UtcNow.ToString('o')}|ConvertTo-Json|Set-Content (Join-Path $evidence 'result.json')
    Write-Host "OPS-02 $Phase completed: $evidence"
}finally{Pop-Location}
