param([string]$MavenCommand='mvn.cmd',[switch]$Offline,[ValidateSet('Focused','Full')][string]$Phase='Full')
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$stamp=[guid]::NewGuid().ToString('N').Substring(0,6)
$source=Join-Path $root ('out/b-'+$stamp)
$cache=Join-Path $root ('out/c-'+$stamp)
$evidence=Join-Path $root ('out/ops03b/run-'+$stamp)
New-Item -ItemType Directory -Force $evidence,$cache|Out-Null
Push-Location $root
try {
 git clone --quiet --no-hardlinks -- $root $source
 if($LASTEXITCODE -ne 0){throw 'Clone failed'}
 robocopy $root $source /E /XD .git target out .idea .agents .codex .aws /NFL /NDL /NJH /NJS /NP|Out-Null
 if($LASTEXITCODE -gt 7){throw 'Source overlay failed'}
 $files=@(git ls-files --cached --others --exclude-standard|Where-Object {$_ -match '(^|/)pom\.xml$|(^|/)src/|^tools/ops03b/'})
 $hashes=@(foreach($file in $files){if(Test-Path -LiteralPath $file -PathType Leaf){[pscustomobject]@{path=$file;sha256=(Get-FileHash -LiteralPath $file).Hash}}})
 $hashes|ConvertTo-Json -Depth 3|Set-Content (Join-Path $evidence 'tested-source.json')
 $thirdParty=Join-Path $env:USERPROFILE '.m2/repository'
 robocopy $thirdParty $cache /E /XD (Join-Path $thirdParty 'com/apex') /NFL /NDL /NJH /NJS /NP|Out-Null
 if($LASTEXITCODE -gt 7){throw 'Dependency copy failed'}
 if(Test-Path (Join-Path $cache 'com/apex')){throw 'APEX cache was not empty'}
 [pscustomobject]@{source=$source;cache=$cache;head=(git rev-parse HEAD);phase=$Phase;apexBefore=0;performance='INCONCLUSIVE: no approved numeric owner-read budget';startedUtc=[DateTimeOffset]::UtcNow.ToString('o')}|ConvertTo-Json|Set-Content (Join-Path $evidence 'input.json')
 $launcher=Join-Path $evidence 'maven.cmd';$maven=(Get-Command $MavenCommand).Source
 @('@echo off',('call "'+$maven+'" "-Dmaven.repo.local='+$cache+'" %*'),'exit /b %errorlevel%')|Set-Content $launcher
 $options=@('-B','-ntp');if($Offline){$options+='-o'}
 function Maven([string[]]$goals,[string]$log){
  $ErrorActionPreference='Continue'; & $launcher @options @goals *> (Join-Path $evidence $log);$code=$LASTEXITCODE
  [pscustomobject]@{goals=$goals;exit=$code}|ConvertTo-Json|Set-Content (Join-Path $evidence ($log+'.result.json'))
  if($code -ne 0){throw "$log failed ($code)"}
 }
 Push-Location $source
 try {
  New-Item -ItemType Directory -Force out/ops03b|Out-Null
  Maven @('-pl','apex-sdk-webmvc,apex-operations-console,apex-sdk-operations,apex-sdk-observability,apex-sdk-grpc-server,apex-sdk-messaging,apex-order-contracts','-am','install','-DskipTests') 'install.log'
  Maven @('-pl','apex-operations-console,apex-sdk-operations','test') 'focused.log'
  foreach($cell in @('order-cell','inventory-cell','payment-cell')){Maven @('-f',"$cell/pom.xml",'package','-DskipTests') "$cell-package.log"}
  Maven @('-f','inventory-cell/pom.xml','test','-Dtest=InventoryEvidenceTest') 'inventory-owner.log'
  Maven @('-f','payment-cell/pom.xml','test','-Dtest=PaymentEvidenceTest') 'payment-owner.log'
  Maven @('-f','order-cell/pom.xml','-Pops03','package','-DskipTests') 'ops03-owner-package.log'
  Maven @('-f','order-cell/pom.xml','-Pops03','test','-Dtest=Ops03ProcessIT') 'ops03-v1.log'
  $jars=@{};foreach($cell in @('order-cell','inventory-cell','payment-cell','apex-operations-console')){
   $name=if($cell -eq 'apex-operations-console'){$cell+'-1.0-SNAPSHOT.jar'}else{$cell+'.jar'}
   $jars[$cell]=(Get-FileHash -LiteralPath "$cell/target/$name").Hash.ToLowerInvariant()
  }
  $jars|ConvertTo-Json|Set-Content out/ops03b/build-receipt.json
  Maven @('-f','inventory-cell/pom.xml','test','-Dtest=Ops03bKafkaEvidenceIT',"-Dsaga.repositoryRoot=$source","-Dsaga.inventoryJar=$source/inventory-cell/target/inventory-cell.jar") 'kafka-console.log'
  if($Phase -eq 'Full'){
   $arguments=@('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/m35-6/verify-platform-product.ps1','-MavenCommand',$launcher);if($Offline){$arguments+='-Offline'}
   $ErrorActionPreference='Continue';& powershell.exe @arguments *> (Join-Path $evidence 'full-product.log');$code=$LASTEXITCODE
   [pscustomobject]@{exit=$code}|ConvertTo-Json|Set-Content (Join-Path $evidence 'full-product.result.json')
   if($code -ne 0){throw 'Full regression failed; original log retained'}
  }
  [pscustomobject]@{exit=0;phase=$Phase;source=$source;finishedUtc=[DateTimeOffset]::UtcNow.ToString('o')}|ConvertTo-Json|Set-Content (Join-Path $evidence 'result.json')
 }finally{Pop-Location}
 Write-Host "OPS-03B evidence: $evidence"
}finally{Pop-Location}
