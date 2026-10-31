param([string]$MavenCommand='mvn.cmd',[switch]$Offline,[switch]$Fast)
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$saved=@{}
Get-ChildItem Env:|Where-Object {$_.Name -match '^(SPRING_|APEX_|CELL_|IDENTITY_|ORDER_|INVENTORY_|PAYMENT_|KAFKA_|EUREKA_|KUBERNETES_|SERVER_|JAVA_TOOL_OPTIONS$|JDK_JAVA_OPTIONS$|_JAVA_OPTIONS$|MAVEN_OPTS$|MAVEN_ARGS$)'}|ForEach-Object {$saved[$_.Name]=$_.Value;Remove-Item -LiteralPath "Env:$($_.Name)"}
Push-Location $root
try {
    if(-not $env:JAVA_HOME){throw 'Set JAVA_HOME to Java 21.'}
    $version=& (Join-Path $env:JAVA_HOME 'bin/java') --version | Out-String
    if($LASTEXITCODE -ne 0 -or $version -notmatch '(?m)^(java|openjdk) 21[\. ]'){throw 'Java 21 required.'}
    $started=[DateTimeOffset]::UtcNow
    $destination=Join-Path $root ('out/m35-6/product-'+$started.ToString('yyyyMMddHHmmss'))
    New-Item -ItemType Directory -Path $destination -Force|Out-Null
    $config=@(git status --porcelain=v1 --untracked-files=all | ForEach-Object {$_.Substring(3)} |
        Where-Object {$_ -match '(application.*\.ya?ml$|^apex-identity-server/|^tools/m35/|^APEX_M35_SPRING|^APEX_M3[234]_)'} |
        ForEach-Object {if(Test-Path -LiteralPath $_ -PathType Leaf){[ordered]@{path=$_;sha256=(Get-FileHash -LiteralPath $_ -Algorithm SHA256).Hash}}})
    $config|ConvertTo-Json -Depth 4|Set-Content (Join-Path $destination 'protected.json')
    $archive=Join-Path $destination 'prior-junit'
    New-Item -ItemType Directory -Path $archive -Force|Out-Null
    foreach($module in Get-ChildItem -Directory){
        Get-ChildItem (Join-Path $module.FullName 'target') -Filter TEST-*.xml -File -Recurse -ErrorAction SilentlyContinue|
            ForEach-Object {Copy-Item -LiteralPath $_.FullName -Destination (Join-Path $archive ($module.Name+'-'+$_.Directory.Name+'-'+$_.Name))}
    }
    $options=@('-B','-ntp');if($Offline){$options+='-o'}
    & $MavenCommand @options -pl apex-spring-boot-starter,apex-starter-webmvc -am clean install
    if($LASTEXITCODE -ne 0){throw 'SDK build failed.'}
    $arguments=@('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/m35/verify-m35.ps1','-MavenCommand',$MavenCommand)
    if($Offline){$arguments+='-Offline'}
    if(-not $Fast){
        & powershell.exe @arguments
        if($LASTEXITCODE -ne 0){throw 'Full M35 infrastructure/Business Cell/Financial regression failed.'}
    }
    $arguments=@('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/m35-6/verify-external-consumer.ps1','-MavenCommand',$MavenCommand)
    if($Offline){$arguments+='-Offline'}
    if($Fast){$arguments+='-Minimal'}
    & powershell.exe @arguments
    if($LASTEXITCODE -ne 0){throw 'Out-of-repository acceptance failed.'}
    foreach($file in $config){if((Get-FileHash -LiteralPath $file.path -Algorithm SHA256).Hash -ne $file.sha256){throw "Protected file changed: $($file.path)"}}
    git -c core.safecrlf=false diff --check
    if($LASTEXITCODE -ne 0){throw 'Whitespace check failed.'}
    [ordered]@{scope=$(if($Fast){'FAST ONLY - NOT FULL ACCEPTANCE'}else{'Full M35 chain and external consumer'});exit=0;startedUtc=$started.ToString('o');completedUtc=[DateTimeOffset]::UtcNow.ToString('o')}|
        ConvertTo-Json|Set-Content (Join-Path $destination 'result.json')
    Write-Host "Completed: $destination. Fast mode never establishes full product PASS."
} finally {Pop-Location;foreach($name in $saved.Keys){Set-Item -LiteralPath "Env:$name" -Value $saved[$name]}}
exit 0
