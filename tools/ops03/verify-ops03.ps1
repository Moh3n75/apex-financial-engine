param([string]$MavenCommand='mvn.cmd',[switch]$Offline,[ValidateSet('Focused','Full')][string]$Phase='Full')
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$out=Join-Path $root ('out/ops03/run-'+[DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))
New-Item -ItemType Directory -Force $out|Out-Null
$options=@('-B','-ntp');if($Offline){$options+='-o'}
function RunMaven([string[]]$goals,[string]$name){
 $ErrorActionPreference='Continue'; & $MavenCommand @options @goals *> (Join-Path $out $name);$code=$LASTEXITCODE
 [pscustomobject]@{command=($goals -join ' ');exit=$code}|ConvertTo-Json|Set-Content (Join-Path $out ($name+'.result.json'))
 if($code -ne 0){throw "Failed $name ($code); original log retained"}
}
Push-Location $root
try {
 [pscustomobject]@{head=(git rev-parse HEAD);phase=$Phase;performance='INCONCLUSIVE: no agreed numeric budget';policy='16 event IDs; 16 workflow IDs; 8 logical owners; serial polling; 1s RPC; 5s poll; <=80s backoff; one owner read connection; 300ms pool wait; 1s SQL statement timeout';started=[DateTimeOffset]::UtcNow.ToString('o')}|ConvertTo-Json|Set-Content (Join-Path $out 'policy.json')
 # Install current prerequisites; do not trust previously installed SNAPSHOTs.
 RunMaven @('-pl','apex-sdk-operations,apex-operations-console,apex-sdk-observability','-am','install','-DskipTests') 'prerequisites.log'
 RunMaven @('-pl','apex-sdk-operations,apex-operations-console','test') 'focused.log'
 RunMaven @('-f','order-cell/pom.xml','-Pops03','package','-DskipTests') 'owner-package.log'
 RunMaven @('-f','order-cell/pom.xml','-Pops03','test','-Dtest=Ops03ProcessIT') 'packaged.log'
 if($Phase -eq 'Full'){
  $args=@('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/ops02/verify-ops02.ps1','-MavenCommand',$MavenCommand,'-Phase','Full');if($Offline){$args+='-Offline'}
  $ErrorActionPreference='Continue'; & powershell.exe @args *> (Join-Path $out 'ops02-full.log');$code=$LASTEXITCODE
  if($code -ne 0){throw 'OPS-02 full chain failed; no OPS-03 full PASS.'}
 }
 # Fresh OPS-03 identities only; full regression remains independently counted by its gate.
 $cases=@();foreach($dir in @('apex-sdk-operations/target/surefire-reports','apex-operations-console/target/surefire-reports','order-cell/target/ops03-build/surefire-reports')){
  Get-ChildItem $dir -Filter 'TEST-*.xml'|ForEach-Object {Copy-Item $_.FullName $out;[xml]$xml=Get-Content $_.FullName;foreach($c in $xml.testsuite.testcase){$cases+=($c.classname+'#'+$c.name)}}
 }
 $cases|Sort-Object -Unique|Set-Content (Join-Path $out 'distinct-cases.txt')
 [pscustomobject]@{phase=$Phase;exit=0;distinctOps03=($cases|Sort-Object -Unique).Count;scope='Focused never establishes full acceptance'}|ConvertTo-Json|Set-Content (Join-Path $out 'result.json')
} finally {Pop-Location}
