# Maintained metadata for properties read via Environment/@Value rather than typed binding.
$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$groups=@{}
function Add($module,$name,$type,$default,$description) {
    if(-not $groups.ContainsKey($module)){$groups[$module]=@()}
    $entry=[ordered]@{name=$name;type=$type;description=$description}
    if($null -ne $default){$entry.defaultValue=$default}
    $groups[$module]+=$entry
}
Add 'spring-boot' 'apex.cell.id' 'java.lang.String' $null 'Cell identity. Required by selected HTTP/messaging; gRPC fallback before spring.application.name. Not a registry metadata alias.'
Add 'webmvc' 'apex.webmvc.context.enabled' 'java.lang.Boolean' $true 'Servlet context filter. Also requires servlet classes/application, provider, spring.application.name and apex.cell.id. Backs off by bean name.'
Add 'messaging' 'apex.messaging.cell.enabled' 'java.lang.Boolean' $false 'Select JDBC/Kafka messaging; owner provides datasource, serializers and migrations under db/platform.'
Add 'messaging' 'apex.messaging.outbox.publisher.enabled' 'java.lang.Boolean' $true 'Publisher selected only inside enabled messaging. Requires publisher.topic.'
Add 'messaging' 'apex.messaging.outbox.publisher.interval-ms' 'java.lang.Long' 1000 'Fixed delay in milliseconds; must be positive when the publisher is selected.'
Add 'messaging' 'apex.messaging.publisher.topic' 'java.lang.String' $null 'One exact Kafka destination required when publisher selected; never inferred from event type.'
Add 'messaging' 'apex.messaging.consumer.enabled' 'java.lang.Boolean' $false 'Select annotated consumer and Inbox. Requires topics and native Kafka group-id.'
Add 'messaging' 'apex.messaging.consumer.topics' 'java.lang.String' $null 'Comma-delimited distinct exact Kafka names; required for selected consumer. No wildcard/regex.'
Add 'messaging' 'apex.messaging.consumer.concurrency' 'java.lang.Integer' 1 'Positive listener concurrency when consumer selected.'
Add 'messaging' 'apex.messaging.publisher.routing.routes' 'java.util.List<com.apex.platform.messaging.jdbc.PublisherRouting$Route>' @() 'Explicit event-type/event-version/topic route list; required when routing selected.'
Add 'messaging' 'apex.messaging.publisher.routing.required-contracts' 'java.util.List<java.lang.String>' @() 'Contract keys required to appear in explicit route list.'
Add 'discovery-grpc' 'apex.discovery.grpc.enabled' 'java.lang.Boolean' $true 'Select bridge only with installed directory and enabled gRPC client.'
Add 'discovery-eureka' 'apex.discovery.membership.enabled' 'java.lang.Boolean' $false 'Eureka membership identity requires contract-discovery environment/instance and cell.id.'
Add 'discovery-eureka' 'apex.discovery.registration.enabled' 'java.lang.Boolean' $false 'Apex registration opt-in, separate from native eureka.client.register-with-eureka.'
Add 'discovery-eureka' 'apex.discovery.grpc-advertised-port' 'java.lang.Integer' $null 'Optional explicit gRPC port override; otherwise actual bound server port. 1..65535.'
Add 'discovery-eureka' 'apex.discovery.max-stale-ms' 'java.lang.Long' 60000 'Eureka freshness limit 1000..120000ms; Kubernetes default/bounds differ by provider.'
Add 'discovery-eureka' 'apex.discovery.registry-id' 'java.lang.String' 'unknown' 'Owner-selected registry identity; unknown does not establish authoritative membership.'
Add 'discovery-kubernetes' 'apex.discovery.insecure-loopback-test-only' 'java.lang.Boolean' $false 'Test-only loopback HTTP API; production requires HTTPS/CA/token. No authentication redesign.'
Add 'discovery-kubernetes' 'apex.discovery.kubernetes.ca-file' 'java.lang.String' $null 'Required CA file outside explicit loopback test mode.'
Add 'discovery-kubernetes' 'apex.discovery.kubernetes.token-file' 'java.lang.String' $null 'Required bounded nonempty service-account token file outside test mode; rotation qualification remains deferred.'
Add 'discovery-kubernetes' 'apex.discovery.max-stale-ms' 'java.lang.Long' 15000 'Kubernetes freshness limit at least two refresh intervals and at most 120000ms.'
Add 'discovery-kubernetes' 'apex.discovery.registry-id' 'java.lang.String' 'unknown' 'Owner-selected registry identity; unknown does not establish authoritative membership.'
Add 'operations' 'apex.contract-discovery.enabled' 'java.lang.Boolean' $false 'Effective declarations only; currently requires both messaging and RPC classes. No traffic/commit proof.'
Add 'operations' 'apex.contract-discovery.local-http.enabled' 'java.lang.Boolean' $false 'Also requires local-ops profile, explicit loopback bind and local/local-dev environment.'
Add 'operations' 'apex.contract-discovery.environment' 'java.lang.String' 'unknown' 'Environment label; explicit value required for membership/local HTTP.'
Add 'operations' 'apex.contract-discovery.instance' 'java.lang.String' 'unknown' 'Instance label; explicit value required for membership.'
Add 'operations' 'apex.contract-discovery.cluster' 'java.lang.String' 'unknown' 'Declared cluster label; no inferred authoritative cluster membership.'
# Defaults the processor cannot infer from factory calls or collection expressions.
Add 'grpc' 'apex.grpc.max-deadline' 'java.time.Duration' '30s' 'Maximum per-call deadline 1ms..30s; does not establish inherited parent budget propagation.'
Add 'grpc' 'apex.grpc.test-clients' 'java.util.List<java.lang.String>' @() 'Explicit loopback test caller allowlist; never a production authorization fallback.'
Add 'grpc' 'apex.grpc.destinations' 'java.util.Map<java.lang.String,com.apex.platform.grpc.internal.GrpcProperties$Destination>' @{} 'At most 32 static routes; local or explicit Boot channel; discovery may supply routes.'
Add 'grpc' 'apex.grpc.collector.allowed-responders' 'java.util.List<java.lang.String>' @() 'Explicit nonempty unique allowlist with routes when collection selected.'
Add 'grpc' 'apex.grpc.security.peers' 'java.util.Map<java.lang.String,com.apex.platform.grpc.internal.PeerSecurityProperties$Grant>' @{} 'Existing certificate policy grants; no OAuth/SSO SDK functionality added.'
Add 'discovery' 'apex.discovery.refresh-interval' 'java.time.Duration' '1s' '250ms..30s. Provider none/eureka/kubernetes selected explicitly.'
Add 'discovery' 'apex.discovery.instance-id' 'java.lang.String' $null 'Eureka defaults to generated replica UUID if absent when registering.'
Add 'discovery' 'apex.discovery.authority' 'java.lang.String' $null 'Eureka defaults to logical cell when registering; trusted TLS authority remains local policy.'
Add 'discovery' 'apex.discovery.targets' 'java.util.Map<java.lang.String,com.apex.platform.discovery.DiscoveryProperties$Target>' @{} 'At most 32 canonical lowercase service names with pinned TLS authorities.'
Add 'observability' 'apex.observability.active-ttl' 'java.time.Duration' '30s' 'Finite diagnostic expiry; 1ms..5m.'
Add 'observability' 'apex.observability.export-interval' 'java.time.Duration' '100ms' 'Finite exporter poll interval; 1ms..5m.'
Add 'observability' 'apex.observability.export-timeout' 'java.time.Duration' '1s' 'Finite best-effort exporter timeout; 1ms..5m.'
Add 'observability' 'apex.observability.shutdown-timeout' 'java.time.Duration' '1s' 'Finite diagnostic shutdown budget; 1ms..5m.'
foreach($module in $groups.Keys) {
    $dir=Join-Path $root "apex-sdk-$module/src/main/resources/META-INF"
    New-Item -ItemType Directory -Force $dir | Out-Null
    $json=[ordered]@{properties=@($groups[$module] | Sort-Object name)} | ConvertTo-Json -Depth 8
    [IO.File]::WriteAllText((Join-Path $dir 'additional-spring-configuration-metadata.json'),$json,(New-Object Text.UTF8Encoding($false)))
}
