param([string]$ReceiptPath,[string]$ExpectedCluster='apex-local-kafka')
$ErrorActionPreference='Stop'
$taskRoot=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
if(-not $ReceiptPath){$ReceiptPath=Join-Path $taskRoot 'out/communication-live.json'}
$taskRegistry=Invoke-RestMethod 'http://127.0.0.1:8761/eureka/apps' -Headers @{Accept='application/json'} -TimeoutSec 8
$taskJava=Invoke-RestMethod 'http://127.0.0.1:9190/ops/v1/topology?schemaVersion=2' -TimeoutSec 8
$taskBff=Invoke-RestMethod 'http://127.0.0.1:3000/api/ops/topology' -TimeoutSec 8
$taskRoutes=Invoke-RestMethod 'http://127.0.0.1:8080/actuator/gateway/routes' -TimeoutSec 8
$taskPage=Invoke-WebRequest 'http://127.0.0.1:3000/topology' -UseBasicParsing -TimeoutSec 8
if($taskJava.schemaVersion -ne 2 -or $taskBff.schemaVersion -ne 2){throw 'v2 required'}
if((@($taskJava.connections.id|Sort-Object)-join ',') -cne (@($taskBff.connections.id|Sort-Object)-join ',')){throw 'Java/BFF connections differ'}
$taskManifests=@();$taskCalls=@();$taskServices=@()
foreach($taskApp in $taskRegistry.applications.application){
  $taskUp=@($taskApp.instance|Where-Object {$_.status -eq 'UP'})
  if(-not $taskUp.Count){continue};$taskService=$taskApp.name.ToLowerInvariant();$taskServices+=$taskService
  if(-not @($taskBff.nodes|Where-Object {$_.kind -eq 'SERVICE' -and $_.name -eq $taskService}).Count){throw 'A real registry service is absent from the graph'}
  foreach($taskInstance in $taskUp){
    if($taskInstance.metadata.'apex-contract-discovery-http' -ne 'true'){continue}
    $taskPort=[int]$taskInstance.port.'$'
    try {
      # Test-side loopback only. The browser does not perform this registry or Cell read.
      $m=Invoke-RestMethod "http://127.0.0.1:$taskPort/ops/v1/contracts" -TimeoutSec 4
      if($m.service -ne $taskService -or $m.environment -ne 'local' -or $m.cluster -ne $ExpectedCluster -or $m.sourceMode -ne 'EFFECTIVE_DECLARATION'){throw 'Manifest scope mismatch'}
      $taskManifests+=$m;$taskCalls+=@{service=$taskService;status='AVAILABLE';source='LIVE_LOCAL_HTTP_EFFECTIVE_DECLARATION'}
    }catch{$taskCalls+=@{service=$taskService;status='SOURCE_UNAVAILABLE'};throw 'An advertised live manifest could not be independently verified'}
  }
}
foreach($edge in $taskBff.connections){
  if($edge.evidence.kind -contains 'OBSERVED'){throw 'This acceptance session has no telemetry source; unexpected observed edge'}
  if($edge.lastSeen){throw 'Declared interactions must not have traffic timestamps'}
  if($edge.protocol -eq 'HTTP'){
    if(-not @($taskRoutes|Where-Object {$edge.contract -eq "ROUTE:$($_.route_id)"}).Count){throw 'HTTP edge lacks actual registered route'}
    if(-not $edge.details.http.predicates.Count){throw 'Registered route predicates missing'}
  }
  if($edge.protocol -eq 'KAFKA'){
    foreach($match in $edge.details.kafka){
      $publisher=@($taskManifests|Where-Object {"local:service:$($_.service)" -eq $match.publisherServiceId -and $_.cluster -eq $match.cluster})
      $topic=@($taskBff.nodes|Where-Object {$_.id -eq $match.topicId})[0].name
      $publication=@($publisher.publications|Where-Object {$_.topic -eq $topic -and $_.contract.type -eq $match.eventType -and $_.contract.version -eq $match.eventVersion})
      if(-not $publication.Count){throw 'Kafka publisher/type/version/cluster/topic mismatch'}
      if($match.matchStatus -eq 'EXACT_DECLARED_CONTRACT_MATCH'){
        $consumer=@($taskManifests|Where-Object {"local:service:$($_.service)" -eq $match.consumerServiceId -and $_.cluster -eq $match.cluster})
        if(-not @($consumer|Where-Object {$_.subscriptions -contains $topic -and "local:group:${ExpectedCluster}:$($_.consumerGroup)" -eq $match.consumerGroupId}).Count){throw 'Kafka consumer subscription/group mismatch'}
        if(-not @($consumer.eventHandlers|Where-Object {$_.type -eq $match.eventType -and $_.version -eq $match.eventVersion}).Count){throw 'Kafka handler mismatch'}
      }
    }
  }
  if($edge.protocol -eq 'GRPC'){
    $d=$edge.details.grpc;$caller=@($taskManifests|Where-Object {"local:service:$($_.service)" -eq $d.callerServiceId})
    if(-not @($caller.grpcDependencies|Where-Object {$_.kind -eq $d.kind -and "$($_.contract.type):$($_.contract.version)" -eq $edge.contract -and "local:service:$($_.target)" -eq $d.targetServiceId}).Count){throw 'gRPC caller declaration missing'}
    $target=@($taskManifests|Where-Object {"local:service:$($_.service)" -eq $d.targetServiceId})
    if(-not @($target.grpcHandlers|Where-Object {$_.kind -eq $d.kind -and "$($_.contract.type):$($_.contract.version)" -eq $edge.contract}).Count){throw 'gRPC target handler mismatch'}
  }
}
$receipt=[ordered]@{source='LIVE_REGISTRY_CELL_MANIFESTS_GATEWAY_JAVA_BFF_PAGE';collectedAt=[DateTime]::UtcNow.ToString('o');
  services=$taskServices;manifestCalls=$taskCalls;manifestCount=$taskManifests.Count;pageStatus=$taskPage.StatusCode;
  protocolCounts=@($taskBff.connections|Group-Object protocol|ForEach-Object {@{protocol=$_.Name;count=$_.Count}});
  manifests=$taskManifests;topology=$taskBff}
$directory=Split-Path -Parent ([IO.Path]::GetFullPath($ReceiptPath));New-Item -ItemType Directory -Force -Path $directory|Out-Null
$json=$receipt|ConvertTo-Json -Depth 40
if($json -match '(?i)"(password|access_token|client_secret|hostname|endpoint|metadata|bootstrap-servers)"\s*:|eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.'){throw 'Receipt redaction check failed'}
$json|Set-Content -LiteralPath $ReceiptPath -Encoding UTF8
Write-Host "Verified $($taskServices.Count) real services, $($taskManifests.Count) effective manifests and $(@($taskBff.connections).Count) declared connections. Receipt: $ReceiptPath"
