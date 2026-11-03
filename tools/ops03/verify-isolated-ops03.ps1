param([string]$MavenCommand='mvn.cmd',[switch]$Offline,[ValidateSet('Focused','Full')][string]$Phase='Full')
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$snapshot=Join-Path $root ('out/o3-'+[guid]::NewGuid().ToString('N').Substring(0,6))
New-Item -ItemType Directory -Force (Join-Path $root 'out')|Out-Null
Push-Location $root
try {
 & git clone --quiet --no-hardlinks -- $root $snapshot
 if($LASTEXITCODE -ne 0){throw 'Source clone failed'}
 & robocopy $root $snapshot /E /XD .git target out .idea .agents .codex .aws /NFL /NDL /NJH /NJS /NP | Out-Null
 if($LASTEXITCODE -gt 7){throw 'Source overlay failed'}
 # Retain all evidence and source; no recursive deletion or stopping unrelated services.
 $receipt=Join-Path $root ('out/ops03/isolated-'+[DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))
 New-Item -ItemType Directory -Force $receipt|Out-Null
 [pscustomobject]@{head=(git rev-parse HEAD);snapshot=$snapshot;phase=$Phase}|ConvertTo-Json|Set-Content (Join-Path $receipt 'source.json')
 $args=@('-NoProfile','-ExecutionPolicy','Bypass','-File',(Join-Path $snapshot 'tools/ops03/verify-ops03.ps1'),'-Phase',$Phase,'-MavenCommand',$MavenCommand);if($Offline){$args+='-Offline'}
 $ErrorActionPreference='Continue';& powershell.exe @args *> (Join-Path $receipt 'runner.log');$code=$LASTEXITCODE
 [pscustomobject]@{exit=$code;evidence=(Join-Path $snapshot 'out/ops03')}|ConvertTo-Json|Set-Content (Join-Path $receipt 'result.json')
 Write-Host "Evidence retained: $receipt; source: $snapshot"
 exit $code
}finally{Pop-Location}
