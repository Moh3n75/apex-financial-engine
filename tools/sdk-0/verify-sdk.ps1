param([string]$MavenCommand='mvn.cmd', [int]$Runs=3, [string]$EvidenceRoot='out/sdk-0')
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Set-Location $root
$evidence=Join-Path $root ($EvidenceRoot+'/repeated-'+[DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))
New-Item -ItemType Directory -Path $evidence -Force | Out-Null
$modules=@(Get-ChildItem -Directory -Filter 'apex-sdk-*' | Sort-Object Name)
function Fingerprint {
    $files=@(Get-Item pom.xml)
    foreach($module in $modules){$files+=Get-Item (Join-Path $module.FullName 'pom.xml');$files+=Get-ChildItem (Join-Path $module.FullName 'src') -Recurse -File -ErrorAction SilentlyContinue}
    $files+=Get-Item $PSCommandPath
    $files+=Get-Item (Join-Path $root 'tools/m35-6/external-consumer/pom.xml'),(Join-Path $root 'tools/sdk-0/verify-installed-messaging.ps1')
    $files+=Get-ChildItem (Join-Path $root 'tools/m35-6/external-consumer/src') -Recurse -File
    @($files | Sort-Object FullName | ForEach-Object { [ordered]@{path=$_.FullName.Substring($root.Length+1);sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash} }) | ConvertTo-Json -Depth 4
}
$fingerprint=Fingerprint
$fingerprint | Set-Content (Join-Path $evidence 'source-fingerprint.json')
$arguments=@('-o','-B','-ntp','-fae','-pl',(($modules | Select-Object -ExpandProperty Name) -join ','),'-am','clean','verify')
[ordered]@{javaHome=$env:JAVA_HOME;maven=$MavenCommand;arguments=$arguments;head=(& git rev-parse HEAD);environment=@(Get-ChildItem Env: | Where-Object {$_.Name -match '^(JAVA_HOME|MAVEN_OPTS|MAVEN_ARGS|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)$'})} | ConvertTo-Json -Depth 4 | Set-Content (Join-Path $evidence 'configuration.json')
$ledger=@()
for($run=1;$run -le $Runs;$run++) {
    if((Fingerprint) -ne $fingerprint){throw 'Source changed between clean runs.'}
    $runDir=Join-Path $evidence "run-$run"
    New-Item -ItemType Directory -Path $runDir -Force | Out-Null
    $started=[DateTimeOffset]::UtcNow.ToString('o')
    # Windows PowerShell treats native stderr (including JVM warnings) as ErrorRecords.
    $ErrorActionPreference='Continue'
    & $MavenCommand @arguments *> (Join-Path $runDir 'build.log')
    $code=$LASTEXITCODE
    $ErrorActionPreference='Stop'
    $tests=0;$failures=0;$errors=0;$skipped=0
    foreach($module in $modules) {
        # Preserve supplemental failure dumps and measurements before the next clean.
        $target=Join-Path $module.FullName 'target'
        $diagnostics=Join-Path $runDir ($module.Name+'/diagnostics')
        if(Test-Path $target){
            $extra=@(Get-ChildItem $target -File | Where-Object {$_.Extension -in @('.log','.csv','.txt','.jfr')})
            $extra+=@(Get-ChildItem (Join-Path $target 'surefire-reports') -File -ErrorAction SilentlyContinue | Where-Object {$_.Name -match '\.dump(stream)?$'})
            if($extra.Count){New-Item -ItemType Directory -Path $diagnostics -Force | Out-Null; $extra | ForEach-Object {Copy-Item -LiteralPath $_.FullName -Destination $diagnostics}}
        }
        $reports=Join-Path $module.FullName 'target/surefire-reports'
        if(Test-Path $reports){
            $dest=Join-Path $runDir $module.Name
            New-Item -ItemType Directory -Path $dest -Force | Out-Null
            Get-ChildItem $reports -Filter 'TEST-*.xml' | ForEach-Object {
                Copy-Item -LiteralPath $_.FullName -Destination $dest
                [xml]$xml=Get-Content -LiteralPath $_.FullName -Raw
                $tests += [int]$xml.testsuite.tests; $failures += [int]$xml.testsuite.failures
                $errors += [int]$xml.testsuite.errors; $skipped += [int]$xml.testsuite.skipped
            }
        }
    }
    $ledger += [ordered]@{run=$run;startedUtc=$started;completedUtc=[DateTimeOffset]::UtcNow.ToString('o');exit=$code;tests=$tests;failures=$failures;errors=$errors;skipped=$skipped}
    $ledger | ConvertTo-Json -Depth 4 | Set-Content (Join-Path $evidence 'ledger.json')
    Write-Host "Run $run exit=$code tests=$tests failures=$failures errors=$errors skipped=$skipped"
}
Write-Host "Evidence: $evidence"
if(@($ledger | Where-Object { $_.exit -ne 0 -or $_.failures -ne 0 -or $_.errors -ne 0 -or $_.skipped -ne 0 }).Count){exit 1}
exit 0
