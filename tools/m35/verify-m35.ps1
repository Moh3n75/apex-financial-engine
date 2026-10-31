param([string]$MavenCommand='mvn.cmd',[switch]$Offline,
      [ValidateSet('All','Identity','Regression')][string]$Phase='All')
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$saved=@{}
Get-ChildItem Env: | Where-Object {$_.Name -match '^(SPRING_|APEX_|IDENTITY_|CELL_|ORDER_|INVENTORY_|PAYMENT_|KAFKA_|EUREKA_|KUBERNETES_|SERVER_|JAVA_TOOL_OPTIONS$|JDK_JAVA_OPTIONS$|_JAVA_OPTIONS$|MAVEN_OPTS$)'} |
    ForEach-Object {$saved[$_.Name]=$_.Value;Remove-Item -LiteralPath "Env:$($_.Name)"}
Push-Location $root
try {
    if(-not $env:JAVA_HOME){throw 'Set JAVA_HOME to Java 21.'}
    $version=& (Join-Path $env:JAVA_HOME 'bin/java') --version | Out-String
    if($LASTEXITCODE -ne 0 -or $version -notmatch '(?m)^(java|openjdk) 21[\. ]'){throw 'Java 21 required.'}
    $options=@('-B','-ntp');if($Offline){$options+='-o'}
    if($Phase -ne 'Regression') {
        & $MavenCommand @options -f apex-identity-server/pom.xml clean verify
        if($LASTEXITCODE -ne 0){throw "M35 Identity verification failed: $LASTEXITCODE"}
        New-Item -ItemType Directory out/m35-acceptance -Force|Out-Null
        Copy-Item apex-identity-server/target/surefire-reports/TEST-*.xml out/m35-acceptance -Force
        Copy-Item apex-identity-server/target/failsafe-reports/TEST-*.xml out/m35-acceptance -Force
        Copy-Item apex-identity-server/target/identity-evidence out/m35-acceptance -Recurse -Force
        Get-FileHash apex-identity-server/target/apex-identity-server.jar -Algorithm SHA256 |
            Select-Object Hash | ConvertTo-Json | Set-Content out/m35-acceptance/artifact.json
    }
    if($Phase -ne 'Identity') {
        $arguments=@('-NoProfile','-ExecutionPolicy','Bypass','-File','tools/m34/verify-m34.ps1','-MavenCommand',$MavenCommand)
        if($Offline){$arguments+='-Offline'}
        & powershell.exe @arguments
        if($LASTEXITCODE -ne 0){throw 'Prior M34/full M32 regression failed; M35 focused PASS does not replace it.'}
    }
    git -c core.safecrlf=false diff --check
    if($LASTEXITCODE -ne 0){throw 'Whitespace check failed.'}
} finally {Pop-Location;foreach($name in $saved.Keys){Set-Item -LiteralPath "Env:$name" -Value $saved[$name]}}
exit 0
