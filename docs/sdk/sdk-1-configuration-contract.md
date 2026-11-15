# SDK-1 configuration contract

Checkpoint `8a2f822`; Java 21. Canonical supported SDK keys, metadata defaults and module ownership are frozen in [configuration-v1.json](../../tools/sdk-1/baseline/configuration-v1.json). Generated `META-INF/spring-configuration-metadata.json` is included in SDK JARs through Boot's compile-time annotation processor. Environment/@Value properties and defaults not inferred from factory expressions are maintained in `additional-spring-configuration-metadata.json`. The processor is not a runtime dependency. [Boot's metadata processor documentation](https://docs.spring.io/spring-boot/specification/configuration-metadata/annotation-processor.html) explains these inference limits.

`verify-configuration.ps1` compares generated names/types/defaults/descriptions to the reviewed catalogue, verifies additional metadata was merged, and audits SDK `apex.*` string literals against the catalogue. Explicit registry metadata fields and metric names are excluded from that property audit. `ConfigurationMetadataContractTest` compares typed metadata defaults to actual Boot-bound defaults, including duration and numeric values. False, empty and absent are distinct. `<not-declared>` means no literal metadata default; conditional/computed behavior is described below.

The script also checks literal `Environment.getProperty` and `${apex.key:default}` fallbacks against the catalogue. Two internal Operations lookup sentinels were aligned with canonical `none` / `unknown`; neither previously allowed a provider nor passed the local HTTP environment allowlist, so allowed settings and access behavior are unchanged. Calculated identity/port/UUID fallbacks remain documented rather than treated as literal defaults. Metadata processor paths are scoped to SDK POMs with metadata; root-only compiler configuration is deliberately avoided so non-SDK modules retain existing processors such as Lombok.

## Activation, prerequisites and aliases

| Capability | Selection / defaults | Required settings and validation |
| --- | --- | --- |
| Default context | Always with SDK Boot; bean-type backoff | No application/cell identity required until a selected boundary needs it |
| HTTP | Servlet app/classes and `apex.webmvc.context.enabled=true` if absent | `spring.application.name`, `apex.cell.id`; nonblank; filter backs off by bean name |
| Messaging | `apex.messaging.cell.enabled=true`; publisher true, consumer false inside selection | Owner JDBC/transactions/Kafka config; publisher requires exact topic; consumer requires exact distinct topics and native group-id. Selected interval/concurrency must be positive, now checked before infrastructure/listener creation |
| Reliability | Cell + consumer + reliability all true | Native MANUAL_IMMEDIATE, no auto-commit/async ACKs/consumer transaction; max attempts 1..100; retry/send/preflight 1ms..1m; safe suffix; unchanged preflight policy |
| Provisioning | Cell + provisioning true | Partitions >=1; factor -1 or positive; 1ms..1m **total** budget for all topics/creation/metadata/cleanup. No automatic partition expansion |
| Publisher routing | Routing enabled | Explicit unique event-type/version/topic routes; every required contract key covered; never falls back to legacy topic |
| gRPC | Server/client flags false until selected; role POM alone does not enable SDK runtime | Identity precedence `apex.grpc.cell-id` → `apex.cell.id` → `spring.application.name`; 1..64 safe characters. Client needs explicit destinations or installed discovery. Payload 128..1048576 bytes; per-call maximum 1ms..30s. Native Boot channels/SSL/server bind are owner settings |
| Collector | Explicit collector flag plus common gRPC role | 1..16 responders; in-flight >= max responders and <=256; 1..32 collections; unique nonempty allowlist (<=32) with routes; no mutation fanout/retry |
| Existing certificate policy | Explicit security flag within selected gRPC | Explicit tenant and URI SAN/contract grants; native TLS trust/client-auth remains required. No OAuth/SSO SDK functionality added |
| Discovery | Provider `none`, `eureka` or `kubernetes`; refresh 250ms..30s; <=32 canonical targets | Base preflight now rejects invalid policy before creating a provider. Provider `none` creates no SDK directory/client. Native Eureka lifecycle flags remain separate; selecting a dependency does not promise native client defaults are disabled |
| Eureka | Explicit provider; Apex registration and membership false | Native Eureka URL/register/fetch policy. Registered cell uses cell.id → grpc.cell-id → application name; instance defaults to generated UUID; authority to logical cell. Optional advertised port 1..65535; otherwise actual bound gRPC port. Freshness 1000..120000ms, default 60000 |
| Kubernetes | Explicit provider | Explicit `spring.cloud.kubernetes.client.master-url` and namespace; HTTPS/CA/token outside explicit loopback test mode. Max-stale >= two refresh intervals and <=120000ms, default 15000. No live-cluster or rotating-token qualification implied |
| Discovery gRPC bridge | Bridge true if absent, directory exists and SDK client enabled | Trust authority pinned locally; no endpoint metadata becomes credential/trust material |
| Observability | Enabled/active/export false | Bounded buffers/records/batch/expiry and safe instance label; all durations 1ms..5m. Exporter SPI must be supplied when selected; no tracing bridge inferred |
| Operations declarations | Explicit contract-discovery enabled | Currently requires both event and RPC classes. Identity/cluster labels default unknown. Local HTTP additionally requires local-ops profile, explicit loopback bind and local/local-dev environment. Public owner readers require owner-supplied DB/policy/endpoints; they are not universal property-bound servers |

No canonical property names were removed. Identity fallback keys above are **fallbacks**, not equivalent renamed properties: precedence and boundary-specific validation remain unchanged and are tested. Boot relaxed kebab/camel/environment binding remains native Boot behavior. `apex.cell-id`, `apex.grpc.port`, `apex.grpc.authority`, `apex.grpc.enabled`, `apex.environment`, `apex.instance-id`, `apex.epoch` are registry metadata fields, not configuration aliases. `apex.outcome`, `apex.ops.*` and admission/drop/export/queue metric names are not properties.

Nested collection/map bindings retain existing record field names:

- `apex.messaging.publisher.routing.routes[index].event-type`, `.event-version`, `.topic`; `required-contracts[index]` contains `TYPE:version` keys.
- `apex.grpc.destinations.<cell>.local` / `.channel`; explicit Boot channels use native `spring.grpc.client.channel.<channel>.*`.
- `apex.grpc.security.peers.<name>.uri` / `.contracts[index]`; existing certificate policy only.
- `apex.discovery.targets.<cell>.authority`; cell names must be canonical lowercase while gRPC static logical identity retains its broader existing syntax.

These shapes are encoded by generic property types and frozen Java record signatures; wildcard examples are not literal configuration keys.

## Invalid configuration and migration rules

SDK-1 deliberately changes only previously invalid configuration handling: selected Messaging nonpositive interval/concurrency now fail in preflight with the exact key named; invalid discovery provider/refresh/target policy now fails when the base discovery capability is installed. Disabled Messaging ignores unused interval/concurrency values. No runtime limit was increased. Existing typed validators continue their previous bounds; descriptions do not imply a new global validation system or rejection of every unknown/misspelled arbitrary property. No authentication behavior changes.

Do not edit a reviewed catalogue to hide a default change. Review changes to the source/default, generated metadata, catalogue and migration guidance together; preserve prior fixture versions. Raw native `spring.*`, Kafka TLS/SASL, serializers, DB credentials, registry URLs and TLS bundles remain owned by their native integrations and are not duplicated as Apex aliases.

For SDK messaging migrations an application must explicitly install `classpath:db/platform` using its migration mechanism, e.g. Flyway `spring.flyway.locations=classpath:db/platform,classpath:db/migration` when both apply. The Financial application's datasource/Flyway defaults are not SDK defaults. SDK messaging does not auto-install schemas or supply credentials.

## Complete generated property/default catalogue

The table below is generated from the reviewed 73 module/property entries; duplicate provider-owned names deliberately retain different provider defaults. Types and descriptions remain in the JSON catalogue.

| Module | Property | Default |
| --- | --- | --- |
| apex-sdk-discovery | `apex.discovery.authority` | `<not-declared>` |
| apex-sdk-discovery | `apex.discovery.instance-id` | `<not-declared>` |
| apex-sdk-discovery | `apex.discovery.provider` | `"none"` |
| apex-sdk-discovery | `apex.discovery.refresh-interval` | `"1s"` |
| apex-sdk-discovery | `apex.discovery.targets` | `{}` |
| apex-sdk-discovery-eureka | `apex.discovery.grpc-advertised-port` | `<not-declared>` |
| apex-sdk-discovery-eureka | `apex.discovery.max-stale-ms` | `60000` |
| apex-sdk-discovery-eureka | `apex.discovery.membership.enabled` | `false` |
| apex-sdk-discovery-eureka | `apex.discovery.registration.enabled` | `false` |
| apex-sdk-discovery-eureka | `apex.discovery.registry-id` | `"unknown"` |
| apex-sdk-discovery-grpc | `apex.discovery.grpc.enabled` | `true` |
| apex-sdk-discovery-kubernetes | `apex.discovery.insecure-loopback-test-only` | `false` |
| apex-sdk-discovery-kubernetes | `apex.discovery.kubernetes.ca-file` | `<not-declared>` |
| apex-sdk-discovery-kubernetes | `apex.discovery.kubernetes.token-file` | `<not-declared>` |
| apex-sdk-discovery-kubernetes | `apex.discovery.max-stale-ms` | `15000` |
| apex-sdk-discovery-kubernetes | `apex.discovery.registry-id` | `"unknown"` |
| apex-sdk-grpc | `apex.grpc.cell-id` | `<not-declared>` |
| apex-sdk-grpc | `apex.grpc.client.enabled` | `false` |
| apex-sdk-grpc | `apex.grpc.collector.allowed-responders` | `[]` |
| apex-sdk-grpc | `apex.grpc.collector.enabled` | `false` |
| apex-sdk-grpc | `apex.grpc.collector.max-collections` | `16` |
| apex-sdk-grpc | `apex.grpc.collector.max-in-flight` | `64` |
| apex-sdk-grpc | `apex.grpc.collector.max-responders` | `16` |
| apex-sdk-grpc | `apex.grpc.destinations` | `{}` |
| apex-sdk-grpc | `apex.grpc.insecure-loopback-test-only` | `false` |
| apex-sdk-grpc | `apex.grpc.max-deadline` | `"30s"` |
| apex-sdk-grpc | `apex.grpc.max-payload-bytes` | `65536` |
| apex-sdk-grpc | `apex.grpc.security.enabled` | `false` |
| apex-sdk-grpc | `apex.grpc.security.peers` | `{}` |
| apex-sdk-grpc | `apex.grpc.security.tenant` | `<not-declared>` |
| apex-sdk-grpc | `apex.grpc.server.enabled` | `false` |
| apex-sdk-grpc | `apex.grpc.test-clients` | `[]` |
| apex-sdk-messaging | `apex.messaging.cell.enabled` | `false` |
| apex-sdk-messaging | `apex.messaging.consumer.concurrency` | `1` |
| apex-sdk-messaging | `apex.messaging.consumer.enabled` | `false` |
| apex-sdk-messaging | `apex.messaging.consumer.reliability.dlt-suffix` | `"-dlt"` |
| apex-sdk-messaging | `apex.messaging.consumer.reliability.enabled` | `false` |
| apex-sdk-messaging | `apex.messaging.consumer.reliability.max-attempts` | `3` |
| apex-sdk-messaging | `apex.messaging.consumer.reliability.preflight-enabled` | `true` |
| apex-sdk-messaging | `apex.messaging.consumer.reliability.preflight-timeout` | `"10s"` |
| apex-sdk-messaging | `apex.messaging.consumer.reliability.retry-delay` | `"1s"` |
| apex-sdk-messaging | `apex.messaging.consumer.reliability.send-timeout` | `"10s"` |
| apex-sdk-messaging | `apex.messaging.consumer.topics` | `<not-declared>` |
| apex-sdk-messaging | `apex.messaging.outbox.publisher.enabled` | `true` |
| apex-sdk-messaging | `apex.messaging.outbox.publisher.interval-ms` | `1000` |
| apex-sdk-messaging | `apex.messaging.provisioning.enabled` | `false` |
| apex-sdk-messaging | `apex.messaging.provisioning.partitions` | `1` |
| apex-sdk-messaging | `apex.messaging.provisioning.replication-factor` | `-1` |
| apex-sdk-messaging | `apex.messaging.provisioning.timeout` | `"10s"` |
| apex-sdk-messaging | `apex.messaging.publisher.routing.enabled` | `false` |
| apex-sdk-messaging | `apex.messaging.publisher.routing.required-contracts` | `[]` |
| apex-sdk-messaging | `apex.messaging.publisher.routing.routes` | `[]` |
| apex-sdk-messaging | `apex.messaging.publisher.topic` | `<not-declared>` |
| apex-sdk-observability | `apex.observability.active-enabled` | `false` |
| apex-sdk-observability | `apex.observability.active-ttl` | `"30s"` |
| apex-sdk-observability | `apex.observability.batch-size` | `64` |
| apex-sdk-observability | `apex.observability.enabled` | `false` |
| apex-sdk-observability | `apex.observability.export-enabled` | `false` |
| apex-sdk-observability | `apex.observability.export-interval` | `"100ms"` |
| apex-sdk-observability | `apex.observability.export-timeout` | `"1s"` |
| apex-sdk-observability | `apex.observability.instance-id` | `"local"` |
| apex-sdk-observability | `apex.observability.max-active` | `256` |
| apex-sdk-observability | `apex.observability.max-bytes` | `1048576` |
| apex-sdk-observability | `apex.observability.max-record-bytes` | `2048` |
| apex-sdk-observability | `apex.observability.max-records` | `1024` |
| apex-sdk-observability | `apex.observability.shutdown-timeout` | `"1s"` |
| apex-sdk-operations | `apex.contract-discovery.cluster` | `"unknown"` |
| apex-sdk-operations | `apex.contract-discovery.enabled` | `false` |
| apex-sdk-operations | `apex.contract-discovery.environment` | `"unknown"` |
| apex-sdk-operations | `apex.contract-discovery.instance` | `"unknown"` |
| apex-sdk-operations | `apex.contract-discovery.local-http.enabled` | `false` |
| apex-sdk-spring-boot | `apex.cell.id` | `<not-declared>` |
| apex-sdk-webmvc | `apex.webmvc.context.enabled` | `true` |
