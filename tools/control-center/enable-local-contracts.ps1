param(
  [Parameter(Mandatory=$true)][ValidatePattern('^[A-Za-z0-9._-]{1,64}$')][string]$KafkaCluster,
  [string[]]$Cells = @('order-cell','inventory-cell','payment-cell'),
  [switch]$RestartRegisteredCells,
  [switch]$StartMissing,
  [string]$ArtifactsDirectory
)
$ErrorActionPreference='Stop'
if(-not $env:JAVA_HOME){throw 'Set JAVA_HOME to the existing Java 21 installation first'}
$taskRoot=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$taskOut=Join-Path $taskRoot 'out/communication-local'
New-Item -ItemType Directory -Force -Path $taskOut|Out-Null
if($KafkaCluster -eq 'unknown'){throw 'An explicit logical cluster identity is required'}
$taskRegistry=Invoke-RestMethod 'http://127.0.0.1:8761/eureka/apps' -Headers @{Accept='application/json'} -TimeoutSec 8
$taskLaunches=@()
if(Test-Path -LiteralPath (Join-Path $taskOut 'launches.json')){$taskLaunches=@(Get-Content -LiteralPath (Join-Path $taskOut 'launches.json') -Raw|ConvertFrom-Json|Where-Object {$_ -and $_.service -match '^[a-z][a-z0-9-]{0,62}$'})}
$Cells=@($Cells|ForEach-Object {$_ -split ','})
foreach($taskCell in $Cells){
  if($taskCell -notmatch '^[a-z][a-z0-9-]{0,62}$'){throw 'Invalid Cell name'}
  $taskJar=if($ArtifactsDirectory){Join-Path ([IO.Path]::GetFullPath((Join-Path $taskRoot $ArtifactsDirectory))) "$taskCell.jar"}else{Join-Path $taskRoot "$taskCell/target/$taskCell.jar"}
  if(-not $taskJar.StartsWith($taskRoot+[IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)){throw 'Use a packaged artifact within this repository'}
  if(-not(Test-Path -LiteralPath $taskJar)){throw "Build the existing $taskCell Maven module first"}
  $taskInstances=@($taskRegistry.applications.application|Where-Object {$_.name -ieq $taskCell}|ForEach-Object {$_.instance}|Where-Object {$_.status -eq 'UP'})
  $taskPort=0;$taskRegistryInstanceId=$null
  foreach($taskInstance in $taskInstances){
    $candidate=[int]$taskInstance.port.'$'
    $connection=Get-NetTCPConnection -State Listen -LocalPort $candidate -ErrorAction SilentlyContinue|Select-Object -First 1
    if(-not $connection){continue}
    $process=Get-CimInstance Win32_Process -Filter "ProcessId=$($connection.OwningProcess)"
    if($process.Name -ne 'java.exe' -or $process.CommandLine -notlike "*$taskRoot*" -or $process.CommandLine -notlike "*$taskCell*"){continue}
    $taskPort=$candidate
    $taskRegistryInstanceId=$taskInstance.instanceId
    if($taskRegistryInstanceId -notmatch '^[A-Za-z0-9._:-]{1,200}$'){throw 'Registry identity cannot be safely passed as a Java argument'}
    if(-not $RestartRegisteredCells){throw "$taskCell is running; use -RestartRegisteredCells to restart this verified repository process"}
    Stop-Process -Id $process.ProcessId
    break
  }
  if($taskPort -eq 0){
    if(-not $StartMissing){throw "$taskCell has no verified local listener; use -StartMissing for its existing packaged app"}
    $listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0);$listener.Start();$taskPort=$listener.LocalEndpoint.Port;$listener.Stop()
    $taskRegistryInstanceId="local-ops:${taskCell}:$taskPort"
  }
  # Use the existing packaged Cell and its ordinary datasource/Kafka configuration. Never inject security bypasses.
  $taskArgs=Join-Path $taskOut "$taskCell.args"
  [IO.File]::WriteAllLines($taskArgs,@('-XX:TieredStopAtLevel=1','-jar',('"'+$taskJar.Replace('\','/')+'"'),
    '--spring.profiles.active=local-ops','--server.address=127.0.0.1',"--server.port=$taskPort",
    '--apex.contract-discovery.enabled=true','--apex.contract-discovery.local-http.enabled=true',
    '--apex.contract-discovery.environment=local',"--apex.contract-discovery.cluster=$KafkaCluster",
    '--eureka.client.enabled=true','--eureka.client.register-with-eureka=true',
    "--eureka.instance.instance-id=$taskRegistryInstanceId",
    '--eureka.instance.metadata-map.apex-contract-discovery-http=true'))
  $taskStart=@{FilePath=(Join-Path $env:JAVA_HOME 'bin/java.exe');ArgumentList=('@"'+$taskArgs+'"');WorkingDirectory=$taskRoot;
    WindowStyle='Hidden';RedirectStandardOutput=(Join-Path $taskOut "$taskCell.log");RedirectStandardError=(Join-Path $taskOut "$taskCell.err.log");PassThru=$true}
  $taskProcess=Start-Process @taskStart
  $taskLaunches=@($taskLaunches|Where-Object {$_.service -ne $taskCell})
  $taskLaunches+=@{service=$taskCell;pid=$taskProcess.Id;port=$taskPort;environment='local';cluster=$KafkaCluster;phase='STARTED_NOT_YET_VERIFIED'}
  Write-Host "Started actual $taskCell process $($taskProcess.Id); effective declarations must be verified separately."
}
$taskLaunches|ConvertTo-Json -Depth 4|Set-Content -LiteralPath (Join-Path $taskOut 'launches.json') -Encoding UTF8
Write-Host 'Configure Console with APEX_OPS_LOCAL_CONTRACTS_ENABLED=true and the same APEX_KAFKA_LOGICAL_CLUSTER, then restart it.'
