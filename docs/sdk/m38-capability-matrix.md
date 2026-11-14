# M38 SDK capability matrix

Audit date: 2026-11-14. Repository: Moh3n75/apex-financial-engine. Branch: `credit-engine-event-sourcing`. Source baseline and audited HEAD: `a9202ee69a368cb8b9d8691be20db5ac831a11d0`. This is a source and evidence audit, not an enterprise readiness certification. No production implementation was changed.

## Findings first

The SDK is **PARTIAL for the stated enterprise scope**. It already provides useful Java contracts, reliable Kafka/SQL processing, bounded gRPC request/query/collection runtimes, optional discovery and operational evidence readers. The critical missing pieces are cross-service HTTP context, general asynchronous context propagation, reusable OAuth2 Resource Server integration and authentication context, and qualified distributed tracing. Kubernetes credentials/membership, nested RPC deadlines, Outbox operational bounds and release compatibility also need work.

M37's 246 passing SDK tests are historical evidence. M38's first clean verification failed a provisioning test; the subsequent run and exact reconciliation are recorded in [test coverage](m38-test-coverage.md). Neither SDK success nor historical standalone consumer success clears the failed M37 full-product gate.

## Classification rules

- **COMPLETE**: the narrowly named behavior is implemented and exercised by relevant tests; no implication that the entire module is enterprise complete.
- **PARTIAL**: useful implementation exists, but a critical behavior or qualification is absent. Every such row references a gap with impact, work, dependencies, priority, acceptance and complexity.
- **MISSING**: required integration is absent. Negative evidence names the source boundary inspected, rather than inventing a nonexistent file.
- **OPTIONAL**: outside the agreed SDK scope or no defined integration use case. This does not mean the capability exists.
- **BLOCKED**: qualification needs the explicitly identified external environment or application gate. Implementation gaps remain PARTIAL/MISSING even when qualification is also blocked.

Paths below are relative to the repository root. Simple class names refer to the exact source-path index at the end of this document. Test names resolve through the test-suite index in the coverage document.

## All 13 Maven modules

| Module | Overall | Actual API/runtime and dependency boundary | Evidence and qualification | Gaps |
| --- | --- | --- | --- | --- |
| apex-sdk-api | PARTIAL | 41 Java sources: context, events, Java RPC annotations/gateways/options/results and Operations contracts. Only Jackson annotations at runtime; no Spring/Kafka/transport. | `pom.xml`, ExecutionContext, EventMetadata, RequestOptions; exercised indirectly, zero module-local tests. | G02, G03, G12, G13 |
| apex-sdk-spring-boot | PARTIAL | API plus Boot starter; one replaceable ThreadLocal provider, no network runtime. | ApexExecutionContextAutoConfiguration; DefaultSdkBoundaryTest, ApexExecutionContextAutoConfigurationTest. | G02, G03 |
| apex-sdk-webmvc | PARTIAL | Servlet starter; conditional root context filter. | ApexWebMvcAutoConfiguration, ApexHttpExecutionContextFilter; WebMvcStarterTest verifies cleanup, not inbound propagation. | G01, G03 |
| apex-sdk-messaging | PARTIAL | API/context + Kafka, JDBC, Jackson, PostgreSQL runtime; publishers/handlers, lease Outbox, transactional Inbox, opt-in reliability/provisioning. | Three registered auto-configurations, two SQL migrations; real embedded Kafka/PostgreSQL suites. | G04, G05, G06, G12, G14 |
| apex-sdk-grpc | PARTIAL | Java-model runtime and generated generic wire protocol; optional Boot client/server support; Spring TX/AOP and Micrometer. | Five auto-configurations, GrpcRuntimeTest, ContractValidationTest, collection/deadline suites. | G03, G07, G08, G09, G12 |
| apex-sdk-grpc-client | COMPLETE | Role selector POM: SDK gRPC + Boot client starter. No Java or auto-configuration of its own. | `pom.xml`; client isolation in installed external consumer and DefaultSdkBoundaryTest-related graph checks; not zero tests interpreted as runtime proof. | Runtime inherits grpc gaps above. |
| apex-sdk-grpc-server | COMPLETE | Role selector POM: SDK gRPC + Boot server starter. | `pom.xml`; server isolation in installed external consumer and gRPC runtime boot suites. | Runtime inherits grpc gaps above. |
| apex-sdk-discovery | PARTIAL | Spring Cloud Commons directory/endpoint/membership abstractions; no provider registration. | CloudCellServiceDirectory, DiscoveryProperties; DiscoveryTest covers metadata identity, freshness and scoped membership. | G10, G11 |
| apex-sdk-discovery-eureka | PARTIAL | Explicit Eureka provider, actuator and Boot server event API; no servlet requirement. | EurekaCellAutoConfiguration; EurekaMetadataTest plus historical owned registry consumer. | G10, G14 |
| apex-sdk-discovery-kubernetes | PARTIAL | Explicit Fabric8 provider; allowlisted namespaced ready IPv4 Pod EndpointSlices. | EndpointSliceDiscoveryClient, KubernetesCellAutoConfiguration; HTTP API fixture, no live cluster qualification. | G10, G11 |
| apex-sdk-discovery-grpc | PARTIAL | Explicit bridge to grpc-client and grpc-util; refreshing round-robin channels and freshness interceptor. | DiscoveredGrpcAutoConfiguration, DirectoryNameResolverFactory; DynamicCellChannelsTest, DirectoryRoundRobinTest use real sockets and TLS. | G09, G10, G14 |
| apex-sdk-observability | PARTIAL | Optional bounded diagnostics, Micrometer metrics and exporter SPI; no distributed tracing bridge/exporter dependency. | ObservabilityAutoConfiguration, BoundedOperationalObserver; ObservabilityTest/ObservationCostTest. | G08 |
| apex-sdk-operations | PARTIAL | Contracts/directory plus optional messaging, RPC, JDBC and servlet adapters. Bounded owner evidence readers; no Console server. | ContractDiscoveryAutoConfiguration, LocalContractHttpAutoConfiguration, MessagingEvidenceReader; 37 historical tests. | G11, G15 |

## Capabilities by audit dimension

| ID | Capability | Status | Actual evidence / boundary | Follow-up |
| --- | --- | --- | --- | --- |
| A01 | Java record contracts, typed event append, annotated handler discovery | COMPLETE | EventContracts, CellEvents, Contracts, HandlerRegistry, AnnotatedEventHandlerDiscovery; validation/proxy/duplicate tests. UUID-based append is the supported facade. | Preserve Java models and packages. |
| A02 | Stable public API lifecycle and actionable consumer documentation | PARTIAL | Public `internal` RPC helpers are consumed by Operations; 1.0-SNAPSHOT, no automated API baseline. | G12, G13 |
| C01 | Same-thread context scope and cleanup | COMPLETE | ThreadLocalExecutionContextProvider; HTTP/Kafka/RPC cleanup tests. Nested LIFO/thread-owner guards exist but need direct misuse tests under G02/G13. | No authentication meaning implied. |
| C02 | HTTP ingress and egress correlation/causation | PARTIAL | Filter always invokes ExecutionContext.root; no HTTP client integration. Gateway EdgeHeaders emits X-Correlation-ID that the filter ignores. | G01 |
| C03 | Kafka business context propagation | COMPLETE | EventEnvelopeFactory persists correlation/causation; executor sets causation to incoming event ID and local identity. SimpleTransactionalEventHandlerExecutorTest. | Trace/auth continuity separately below. |
| C04 | gRPC business context propagation | COMPLETE | GrpcInvocationClient + RpcServerAuth + RpcDispatcher validate wire/header IDs and scope handler execution; runtime tests. | Arbitrary callbacks and nested deadline budget separately below. |
| C05 | General Executor/@Async/scheduled callback context capture/restore | MISSING | API provider is a ThreadLocal only; SDK Boot configuration registers no TaskDecorator, context accessor or task wrapper. SDK-owned local query/collector workers explicitly scope context. | G02 |
| M01 | Atomic business transaction plus Outbox intent through CellEvents | COMPLETE | DefaultCellEvents requires active transaction; JdbcOutboxAppender and V1001; rollback/commit integration tests. | Caller must use same database/transaction manager. |
| M02 | Concurrent lease claim and token-fenced state changes | PARTIAL | JdbcOutboxStore uses SKIP LOCKED and claim-token predicates; normal publication exercises claim/mark, but targeted concurrent stale-owner/crash qualification is absent from SDK suites. | G05/G14. |
| M03 | Consumer Inbox deduplication and commit-before-ACK | COMPLETE | JdbcProcessedEventStore + SimpleTransactionalEventHandlerExecutor; reliable consumer tests include deferred DB commit failure. | Exactly-once external effects are not promised. |
| M04 | Bounded retries, strict unknown-contract quarantine, confirmed DLT then offset recovery | COMPLETE | ReliableKafkaConsumerRuntime and factory validation; ReliableConsumerIntegrationTest. Enabled only by explicit reliability opt-in. | Legacy mode can ACK IGNORED. |
| M05 | Topic provisioning with safe startup under delayed metadata | PARTIAL | ApexTopicProvisioner.ensure performs one post-create describe; M38 provisioning failure. | G04 |
| M06 | Outbox finite send/lease lifecycle, configurable throughput and drain | PARTIAL | ConfigurableOutboxPublisher uses join(), fixed 100-row batch and five-minute lease; scheduler fixed-delay property. | G05 |
| M07 | Replay consumption preserving original IDs and same Inbox group | COMPLETE | Multi-topic consumer + ReliableConsumerIntegrationTest.exactMultiTopicSubscriptionSharesInboxAndReliableReplayDlt. | Administrative replay is a separate tool. |
| M08 | SDK ConsumerFailureReplay/recovery/statistics implementation | OPTIONAL | Public interfaces have no SDK implementation; replay tool and financial infrastructure own separate persistence. | G06 documents the distinction; do not duplicate operator tooling. |
| M09 | Event evolution and bounded decoding guarantees | PARTIAL | EventContract.schemaId is not serialized in EventMetadata; dispatcher uses general JsonMapper; no version-pair compatibility suite. | G06, G12 |
| R01 | Request/Query Java records, safe response checks, status mapping and ambiguity | COMPLETE | RpcCodec, GrpcInvocationClient, RpcErrors; runtime/contract tests. Mutations have no automatic retry. | Owner receipt implementation is an SPI, not supplied universally. |
| R02 | Persistent transactional request idempotency across replicas | PARTIAL | TransactionalRequestExecutor contract + server startup preflight; actual atomic storage belongs to owner. | G07 |
| R03 | Finite individual call deadlines, cancellation, bounded local queues | COMPLETE | RequestOptions/QueryOptions, RpcServerAuth, DefaultQueryGateway; GrpcRuntimeTest. | Nested budget propagation: G07. |
| R04 | Parent deadline/cancellation through nested outbound calls | PARTIAL | GrpcInvocationClient applies supplied Duration; no explicit min(parent remaining, requested) policy for worker/collector boundaries. | G07 |
| R05 | Explicit multi-responder read-only collection | COMPLETE | DefaultMultiResponderGateway, CollectionOptions; distinct IDs, sorted slots, total deadline, strict/partial results, cancellation and saturation tests. | One logical responder per supplied Cell; not all replicas. |
| R06 | Broadcast commands, central command bus, streaming RPC | OPTIONAL | No defined enterprise integration use case; mutation fanout intentionally absent. | Do not add. |
| D01 | Directory identity/authority validation and stale refusal | COMPLETE | CloudCellServiceDirectory, DiscoveryFreshnessInterceptor; forged metadata/freshness tests. | No registry metadata becomes trust material. |
| D02 | Replica addition/removal, round-robin selection, no mutation resend | COMPLETE | DirectoryNameResolverFactory and discovered channels; real socket/mTLS tests. | End-to-end failover/drain qualification remains G10/G14. |
| D03 | Eureka bound-port publication and optional registration | COMPLETE | GrpcServerEndpoint listener; zero port/disabled/actual allocated-port tests. | Native Eureka registration flags must also be configured. |
| D04 | Production discovery lifecycle, graceful drain and partition behavior | PARTIAL | SDK shutdown relies on native runtimes; no complete readiness-to-withdraw-to-drain evidence. | G10 |
| D05 | Kubernetes usable long-lived credentials and complete operational membership | PARTIAL | Token read once into Fabric8 config; emitted metadata lacks environment/epoch/replica membership fields. | G11 |
| D06 | Live Kubernetes qualification | BLOCKED | Existing tests use an HTTP EndpointSlice API fixture. | Requires disposable cluster, scoped RBAC, rotating service account token, TLS, deployment/termination fixtures. G10/G11. |
| S01 | Production RPC mTLS service identity and per-contract grants | COMPLETE | CertificatePeerPolicy, PeerSecurityProperties, InvocationAuthorizer; DynamicCellChannelsTest includes wrong-name/peer rejection. | This is service identity, not end-user OAuth. |
| S02 | Reusable HTTP OAuth2/OIDC Resource Server integration | MISSING | No SDK Spring Security Resource Server dependency/configuration; Console has app-specific OpsM36aSecurityConfiguration. | G03 |
| S03 | Authenticated principal/tenant/authorities context and authorization adapter | MISSING | ExecutionContext has four business provenance fields; RequestContext carries RPC service caller identity only. | G03 |
| S04 | Service OAuth token acquisition/relay policy for HTTP/gRPC | MISSING | GrpcInvocationClient attaches only Cell/correlation/request headers; Gateway currently strips Authorization. | G03; first define machine vs delegated user use cases. |
| S05 | Central Identity/SSO issuance, login, consent, user administration | OPTIONAL | apex-identity-server is a standalone Spring authorization server application. | Not SDK features; its availability is a security acceptance dependency. |
| O01 | Bounded best-effort operational samples and safe low-cardinality metrics | COMPLETE | BoundedOperationalObserver, ActiveOperations, metric filter; saturation/export timeout/cost tests. Disabled by default. | Samples are not durable commit evidence. |
| O02 | Cross-HTTP/Kafka/gRPC tracing with span hierarchy and async propagation | MISSING | No tracing bridge, W3C carrier integration or cross-transport trace test in SDK; observations alone do not supply this guarantee. | G08 |
| O03 | Durable owner messaging evidence with bounded reads | COMPLETE | MessagingEvidenceReader, BoundedOwnerSearch, BoundedRecentSearch; DB permission, timeout, cursor, commit uncertainty tests. | Applications supply endpoints/security/owner state. |
| O04 | Metadata discovery independent of installed transport APIs | PARTIAL | ContractDiscoveryAutoConfiguration requires both messaging and RPC classes, even though dependencies are optional. | G15 |
| F01 | Optional capability isolation and bean backoff | COMPLETE | Default starter, role POMs, named imports and filtered-classloader tests; seven historical external modes. | Full combinations need G14. |
| F02 | Consistent typed properties, validation and generated metadata | PARTIAL | Mixed @Value/Environment/ConfigurationProperties, identity aliases and fixed runtime bounds; no SDK configuration processor declaration. | G13 |
| V01 | Same-version reactor and Java 21 build | COMPLETE | Parent and 13 POMs use 1.0-SNAPSHOT; Boot 4.1.1 inheritance; Cloud 2025.1.3 in three modules. | Does not certify upstream support compatibility. |
| V02 | Consumer dependency alignment, API/wire/SQL compatibility and publication gate | PARTIAL | No SDK-wide dependency management/version-compatibility gate or documented immutable release process. | G12 |
| T01 | External installed packaged consumer behavior | PARTIAL | Seven successful M37 receipt modes; source checked, not re-executed or freshly installed by M38. | G14 |
| T02 | Full product acceptance | BLOCKED | M37 platform chain failed at Order; Financial and later packaged stages were not reached. | Resolve known Cell/profile failures and run full chain at one fingerprint. G14. |

## Integration ownership

Gateway (`apex-api-gateway/src/main/java/com/apex/gateway/EdgeHeaders.java`) sanitizes ingress and strips credentials; SDK needs an agreed correlation and authenticated forwarding contract. Changing Gateway authentication is separate application work. Identity (`apex-identity-server/src/main/java/com/apex/identity/IdentityConfiguration.java`) already uses Spring authorization-server APIs: do not rebuild issuance in the SDK. Console (`apex-operations-console/src/main/java/com/apex/operations/console/security/OpsM36aSecurityConfiguration.java` and `security/m36b/OpsM36bResourceAuthorizer.java`) demonstrates audience/tenant/scope/resource authorization requirements, not universal business policy to copy verbatim.

Order's SagaSubscriptionPreflight requires EventHandlerRegistry while SDK registration is conditional on consumer enablement. That is a profile/wiring integration issue to qualify, not evidence that all SDK consumers need a mandatory event registry. Customer/Reporting source-and-artifact receipts establish why independently deployed services need installed-artifact tests. Payment decline rules, Inventory pause policy, Order saga transitions and all Cell data models remain business responsibilities.

The user-linked `apex-infrastructure/src/main/resources/application.yml` is Financial application configuration: it defines a local datasource and Flyway `classpath:db/migration`. SDK messaging migrations are at `classpath:db/platform` and are not installed automatically by the SDK. Its credentials/defaults must not become SDK defaults. Applications must select and verify migrations in their own databases.

No Cell boundary changes, command bus, new module, cryptography implementation, commits or pushes are proposed by this audit.

## Configuration and activation audit

The following table covers SDK-owned configuration families and their native-runtime prerequisites. Metadata keys such as `apex.grpc.port`, `apex.epoch` and `apex.cell-id` in registry maps are transport metadata, not aliases for application properties. Defaults below are source defaults, not recommended production settings. Manual validation is often strong, but inconsistent binding/metadata remains PARTIAL under G13.

| Family | Defaults and required values | Validation / optionality / evidence |
| --- | --- | --- |
| Default context | No properties; provider replaceable by bean type. | Always available with sdk-spring-boot. ApexExecutionContextAutoConfiguration. COMPLETE for same-thread use. |
| `apex.webmvc.context.enabled` | true if absent; requires `spring.application.name`, `apex.cell.id` for an enabled servlet filter. | Servlet class/application conditions and named filter backoff. Identity text nonblank. PARTIAL propagation G01. |
| `apex.messaging.cell.enabled` | Opt-in true; publisher defaults on once selected; consumer defaults off. | PlatformCellMessagingAutoConfiguration activates scheduling, SQL stores, typed facade and mapper. Selected messaging requires app-owned JDBC/Kafka settings. COMPLETE selection; G13 diagnostics. |
| `apex.messaging.outbox.publisher.enabled`, `.interval-ms` | true / 1000ms; `apex.messaging.publisher.topic` required when on. | Scheduling string is not a typed validated duration group; fixed batch/lease/retries G05. |
| `apex.messaging.consumer.enabled`, `.topics`, `.concurrency` | false / required exact topics / 1. Requires `spring.kafka.consumer.group-id`. | ConsumerTopics parses comma-delimited exact names, rejects regex/wildcards/duplicates/invalid names. Legacy unknown contract is IGNORED then ACKed. Reliable mode rejects it. |
| `apex.messaging.publisher.routing` | enabled=false, routes=[], required-contracts=[]; each route event-type/event-version/topic. | PublisherRouting validates duplicate/missing routes; enabled routing never falls back to legacy topic. COMPLETE routing. |
| `apex.messaging.consumer.reliability` | enabled=false; max-attempts=3 including initial; retry-delay=1s; send-timeout=10s; dlt-suffix=-dlt; preflight-enabled=true; preflight-timeout=10s. | Attempts 1–100, durations 1ms–1m, suffix syntax/length. Factory requires record mode, MANUAL_IMMEDIATE, auto-commit=false, synchronous commits, no async ACKs or container TX; transactional producer rejected. COMPLETE selected policy. |
| `apex.messaging.provisioning` | enabled=false; partitions=1; replication-factor=-1 broker default; timeout=10s. | Positive partitions, factor -1 or positive, timeout ≤1m. Only explicit selected names; existing topics not mutated. PARTIAL metadata readiness G04. |
| SQL migrations | SDK ships V1001 Outbox/V1002 Inbox under db/platform; no Flyway runtime dependency/automatic migration bean. | App selects Flyway/native migration mechanism and schema. Same datasource/transaction required. Financial db/migration alone does not install SDK schema. PARTIAL consumer qualification G13/G14. |
| `apex.grpc.server.enabled`, `.client.enabled` | Both false unless explicitly true; role POM alone does not enable SDK behavior. | GrpcEnabled enables common infrastructure if either role true. Local query can use common runtime without a remote client. Collector alone is not a valid role selection. |
| `apex.grpc.cell-id` | Falls back to apex.cell.id then application name. | 1–64 letters/digits/period/underscore/hyphen; discovery has narrower lowercase logical naming. G13. |
| `apex.grpc.max-payload-bytes`, `.max-deadline` | 65536 bytes / 30s. | 128–1048576 bytes; deadline 1ms–30s. Request/query options default 5s and validate their own bounds; wire message adds 8192-byte envelope budget. COMPLETE individual limits. |
| `apex.grpc.destinations` | Empty unless explicit routes; each destination is local or points to channel. | Static routes require safe configured Boot TLS target; automatic discovery allows absent destination map. Collector still requires explicit routes for its allowed responders. |
| `apex.grpc.insecure-loopback-test-only`, `.test-clients` | false / []; plaintext test server needs explicit loopback bind and client allowlist. | Production requires verifier, SSL bundle and client-auth=require; test client plaintext limited to explicit loopback targets. Never a production authentication shortcut. |
| `apex.grpc.security` | enabled=false, tenant required when enabled, peers map of URI SAN/contract grants. | PeerSecurityProperties validates ≤64 peers, exact SPIFFE-style URI, tenant/cell and REQUEST/QUERY type/version grants; duplicate verifiers/policies rejected. G03 OAuth independent. |
| `apex.grpc.collector` | enabled=false; allowed-responders=[]; max-responders=16, max-in-flight=64, max-collections=16. | Explicit nonempty unique allowlist (≤32) must have routes; max responders ≤16, in-flight ≤256, collections ≤32. Requires QueryGateway/common role. COMPLETE explicit collection. |
| Native `spring.grpc.*`, SSL bundles | Applications own channel targets/default TLS bundles and server bind/port/client-auth. | SDK adds resource limits and disables retries. Native Boot lifecycle remains authoritative; G07/G10 deployment qualification. |
| `apex.discovery.provider` | none; explicitly eureka or kubernetes. | DiscoveryProperties validates recognized provider only when bound/used; provider artifact must be installed. No implicit provider selection. |
| `apex.discovery.refresh-interval`, `.targets` | 1s, empty map; target authority pinned locally; otherwise logical name is authority. | 250ms–30s, ≤32 explicit targets, lowercase canonical names and authority syntax. Kubernetes enumerates configured targets only. |
| `apex.discovery.grpc.enabled` | true if bridge installed, directory exists and grpc client enabled. | Role/client class guard; default verified TLS required even before replicas exist. COMPLETE conditional bridge. |
| `apex.discovery.registration.enabled`, `.instance-id`, `.authority`, `.grpc-advertised-port` | Apex registration false; random replica/default logical authority; bound gRPC port unless advertised override. | Eureka metadata only rewritten when enabled; native `eureka.client.register-with-eureka` is independently configured. Invalid replica/authority/port conflict fails. G10 lifecycle. |
| `apex.discovery.max-stale-ms`, `.registry-id` | Eureka 60000, K8s 15000 / unknown registry ID. | Eureka 1000–120000ms; K8s ≥2 refresh periods and ≤2m. Unknown registry ID prevents authoritative membership. |
| `apex.discovery.membership.enabled` | false; Eureka runtime identity then needs `apex.contract-discovery.environment` / `.instance`, `apex.cell.id`. | Generates boot UUID shared with Operations manifest. K8s parity absent G11. |
| `spring.cloud.kubernetes.client.master-url`, `.namespace`; `apex.discovery.kubernetes.ca-file`, `.token-file` | Required explicit API/namespace; CA/token required outside test loopback. `apex.discovery.insecure-loopback-test-only` defaults false. | Explicit HTTPS (loopback test exception); scoped namespace; token nonempty ≤32768; connection/request timeouts 2s, retries 0. Credential refresh G11. |
| `apex.observability.enabled`, `.active-enabled`, `.export-enabled` | All false. | Enabled configuration registers active tracking/observer and meter filter; export also requires caller exporter bean. Diagnostics isolated from business results. |
| Observability resource bounds | max-records=1024, max-bytes=1048576, max-record-bytes=2048, max-active=256, batch-size=64. | Validated finite capacities; actual sample reservation 256 bytes; queue/drop metrics and no unbounded raw metadata. |
| Observability time/identity | active-ttl=30s, export-interval=100ms, export-timeout=1s, shutdown-timeout=1s, instance-id=local. | All durations 1ms–5m; instance label validated, not automatically discovery identity. Need explicit deployment value G13. |
| `apex.contract-discovery.enabled`, `.environment`, `.instance`, `.cluster` | false; identity/cluster fallback unknown, but application/cell name required. | ContractDiscoveryProvider labels effective declarations and gaps; no inference that configured route was used. Both messaging/RPC APIs required G15. |
| `apex.contract-discovery.local-http.enabled` | false; additionally `local-ops` profile, explicit loopback server.address and local/local-dev environment. | LocalContractRead rejects remote requests; no-store response. Not a secured production Console endpoint. |
| Owner evidence adapters | Constructor parameters, not a universal application property group: datasource, owner/environment/instance, allowed states/groups/sources/index policy. | Read-only transactions, admission/rate/SQL/lock/page/window bounds; owner supplies restricted DB role/endpoints. COMPLETE bounded read semantics; integration G15. |

Native Kafka TLS/SASL/producer/consumer tuning, Eureka registry URLs/lease/cache controls, actuator exposure and telemetry exporter connection settings remain owned by their native Spring integrations and application deployment. The SDK supplies no universal enterprise values for them. Missing documented qualification is tracked by G10/G13/G14; replacing these runtimes is not recommended.

## Exact source and registration index

This index enumerates all 154 current SDK production Java files, the wire/schema/resources and all 16 auto-configuration registrations. It excludes generated target classes and obsolete pre-M37 directories. Module/capability statuses above apply to these source families; a filename or public declaration alone is not a COMPLETE claim. Tests are indexed separately in the coverage document. Public types in internal packages remain compatibility-sensitive where externally consumed.

### apex-sdk-api

POM: [apex-sdk-api/pom.xml](../../apex-sdk-api/pom.xml).

`apex-sdk-api/src/main/java/com/apex/operations/contracts/`: [ContractDiscoveryQuery.java](../../apex-sdk-api/src/main/java/com/apex/operations/contracts/ContractDiscoveryQuery.java), [ContractDiscoveryQueryV2.java](../../apex-sdk-api/src/main/java/com/apex/operations/contracts/ContractDiscoveryQueryV2.java), [ContractManifest.java](../../apex-sdk-api/src/main/java/com/apex/operations/contracts/ContractManifest.java), [EvidenceQuery.java](../../apex-sdk-api/src/main/java/com/apex/operations/contracts/EvidenceQuery.java), [OwnerRecentSearch.java](../../apex-sdk-api/src/main/java/com/apex/operations/contracts/OwnerRecentSearch.java), [OwnerSearch.java](../../apex-sdk-api/src/main/java/com/apex/operations/contracts/OwnerSearch.java), [OwnerSnapshot.java](../../apex-sdk-api/src/main/java/com/apex/operations/contracts/OwnerSnapshot.java), [TopologySnapshot.java](../../apex-sdk-api/src/main/java/com/apex/operations/contracts/TopologySnapshot.java).

`apex-sdk-api/src/main/java/com/apex/platform/core/context/`: [ExecutionContext.java](../../apex-sdk-api/src/main/java/com/apex/platform/core/context/ExecutionContext.java), [ExecutionContextProvider.java](../../apex-sdk-api/src/main/java/com/apex/platform/core/context/ExecutionContextProvider.java), [ThreadLocalExecutionContextProvider.java](../../apex-sdk-api/src/main/java/com/apex/platform/core/context/ThreadLocalExecutionContextProvider.java).

`apex-sdk-api/src/main/java/com/apex/platform/events/`: [EventEnvelope.java](../../apex-sdk-api/src/main/java/com/apex/platform/events/EventEnvelope.java), [EventEnvelopeFactory.java](../../apex-sdk-api/src/main/java/com/apex/platform/events/EventEnvelopeFactory.java), [EventMetadata.java](../../apex-sdk-api/src/main/java/com/apex/platform/events/EventMetadata.java), [IntegrationEvent.java](../../apex-sdk-api/src/main/java/com/apex/platform/events/IntegrationEvent.java).

`apex-sdk-api/src/main/java/com/apex/platform/events/api/`: [CellEvents.java](../../apex-sdk-api/src/main/java/com/apex/platform/events/api/CellEvents.java), [EventContract.java](../../apex-sdk-api/src/main/java/com/apex/platform/events/api/EventContract.java), [EventContracts.java](../../apex-sdk-api/src/main/java/com/apex/platform/events/api/EventContracts.java).

`apex-sdk-api/src/main/java/com/apex/platform/events/saga/`: [SagaEvent.java](../../apex-sdk-api/src/main/java/com/apex/platform/events/saga/SagaEvent.java), [SagaTimeoutDetectedEvent.java](../../apex-sdk-api/src/main/java/com/apex/platform/events/saga/SagaTimeoutDetectedEvent.java).

`apex-sdk-api/src/main/java/com/apex/platform/grpc/api/`: [CellQuery.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/CellQuery.java), [CellQueryHandler.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/CellQueryHandler.java), [CellRequest.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/CellRequest.java), [CellRequestHandler.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/CellRequestHandler.java), [CollectionCancelledException.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/CollectionCancelledException.java), [CollectionFailedException.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/CollectionFailedException.java), [CollectionOptions.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/CollectionOptions.java), [CollectionResult.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/CollectionResult.java), [GatewayCancellationException.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/GatewayCancellationException.java), [GatewayException.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/GatewayException.java), [GrpcCallerDeclarations.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/GrpcCallerDeclarations.java), [GrpcServerEndpoint.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/GrpcServerEndpoint.java), [InstanceQueryGateway.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/InstanceQueryGateway.java), [MultiResponderGateway.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/MultiResponderGateway.java), [QueryGateway.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/QueryGateway.java), [QueryOptions.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/QueryOptions.java), [RequestContext.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/RequestContext.java), [RequestOptions.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/RequestOptions.java), [RequestReplyGateway.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/RequestReplyGateway.java), [ResponderResult.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/ResponderResult.java), [TransactionalRequestExecutor.java](../../apex-sdk-api/src/main/java/com/apex/platform/grpc/api/TransactionalRequestExecutor.java).



### apex-sdk-discovery

POM: [apex-sdk-discovery/pom.xml](../../apex-sdk-discovery/pom.xml).

`apex-sdk-discovery/src/main/java/com/apex/platform/discovery/`: [CellEndpoint.java](../../apex-sdk-discovery/src/main/java/com/apex/platform/discovery/CellEndpoint.java), [CellMembership.java](../../apex-sdk-discovery/src/main/java/com/apex/platform/discovery/CellMembership.java), [CellRuntimeIdentity.java](../../apex-sdk-discovery/src/main/java/com/apex/platform/discovery/CellRuntimeIdentity.java), [CellServiceDirectory.java](../../apex-sdk-discovery/src/main/java/com/apex/platform/discovery/CellServiceDirectory.java), [CloudCellServiceDirectory.java](../../apex-sdk-discovery/src/main/java/com/apex/platform/discovery/CloudCellServiceDirectory.java), [DiscoveryProperties.java](../../apex-sdk-discovery/src/main/java/com/apex/platform/discovery/DiscoveryProperties.java).



### apex-sdk-discovery-eureka

POM: [apex-sdk-discovery-eureka/pom.xml](../../apex-sdk-discovery-eureka/pom.xml).

`apex-sdk-discovery-eureka/src/main/java/com/apex/discovery/eureka/`: [EurekaCellAutoConfiguration.java](../../apex-sdk-discovery-eureka/src/main/java/com/apex/discovery/eureka/EurekaCellAutoConfiguration.java).

- [apex-sdk-discovery-eureka/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports](../../apex-sdk-discovery-eureka/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports)


### apex-sdk-discovery-grpc

POM: [apex-sdk-discovery-grpc/pom.xml](../../apex-sdk-discovery-grpc/pom.xml).

`apex-sdk-discovery-grpc/src/main/java/com/apex/discovery/grpc/`: [DirectoryNameResolverFactory.java](../../apex-sdk-discovery-grpc/src/main/java/com/apex/discovery/grpc/DirectoryNameResolverFactory.java), [DiscoveredGrpcAutoConfiguration.java](../../apex-sdk-discovery-grpc/src/main/java/com/apex/discovery/grpc/DiscoveredGrpcAutoConfiguration.java), [DiscoveryFreshnessInterceptor.java](../../apex-sdk-discovery-grpc/src/main/java/com/apex/discovery/grpc/DiscoveryFreshnessInterceptor.java).

- [apex-sdk-discovery-grpc/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports](../../apex-sdk-discovery-grpc/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports)


### apex-sdk-discovery-kubernetes

POM: [apex-sdk-discovery-kubernetes/pom.xml](../../apex-sdk-discovery-kubernetes/pom.xml).

`apex-sdk-discovery-kubernetes/src/main/java/com/apex/discovery/kubernetes/`: [EndpointSliceDiscoveryClient.java](../../apex-sdk-discovery-kubernetes/src/main/java/com/apex/discovery/kubernetes/EndpointSliceDiscoveryClient.java), [KubernetesCellAutoConfiguration.java](../../apex-sdk-discovery-kubernetes/src/main/java/com/apex/discovery/kubernetes/KubernetesCellAutoConfiguration.java).

- [apex-sdk-discovery-kubernetes/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports](../../apex-sdk-discovery-kubernetes/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports)


### apex-sdk-grpc

POM: [apex-sdk-grpc/pom.xml](../../apex-sdk-grpc/pom.xml).

`apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/`: [CellGrpcChannelProvider.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/CellGrpcChannelProvider.java), [CertificatePeerPolicy.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/CertificatePeerPolicy.java), [CollectorProperties.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/CollectorProperties.java), [Contracts.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/Contracts.java), [DefaultMultiResponderGateway.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/DefaultMultiResponderGateway.java), [DefaultQueryGateway.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/DefaultQueryGateway.java), [DiscoveredCellChannelFactory.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/DiscoveredCellChannelFactory.java), [GenericGrpcService.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/GenericGrpcService.java), [GrpcClientAutoConfiguration.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/GrpcClientAutoConfiguration.java), [GrpcCollectorAutoConfiguration.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/GrpcCollectorAutoConfiguration.java), [GrpcCommonAutoConfiguration.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/GrpcCommonAutoConfiguration.java), [GrpcEnabled.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/GrpcEnabled.java), [GrpcInvocationClient.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/GrpcInvocationClient.java), [GrpcPeerSecurityAutoConfiguration.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/GrpcPeerSecurityAutoConfiguration.java), [GrpcProperties.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/GrpcProperties.java), [GrpcServerAutoConfiguration.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/GrpcServerAutoConfiguration.java), [HandlerRegistry.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/HandlerRegistry.java), [ObservedQueryGateway.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/ObservedQueryGateway.java), [PeerSecurityProperties.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/PeerSecurityProperties.java), [RpcCodec.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/RpcCodec.java), [RpcContexts.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/RpcContexts.java), [RpcDispatcher.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/RpcDispatcher.java), [RpcErrors.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/RpcErrors.java), [RpcFuture.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/RpcFuture.java), [RpcObservation.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/RpcObservation.java), [RpcServerAuth.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/RpcServerAuth.java).

`apex-sdk-grpc/src/main/java/com/apex/platform/grpc/security/`: [InvocationAuthorizer.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/security/InvocationAuthorizer.java), [PeerIdentityVerifier.java](../../apex-sdk-grpc/src/main/java/com/apex/platform/grpc/security/PeerIdentityVerifier.java).

- [apex-sdk-grpc/src/main/proto/cell_gateway.proto](../../apex-sdk-grpc/src/main/proto/cell_gateway.proto)
- [apex-sdk-grpc/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports](../../apex-sdk-grpc/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports)


### apex-sdk-grpc-client

POM: [apex-sdk-grpc-client/pom.xml](../../apex-sdk-grpc-client/pom.xml).

No production Java sources; role selector dependency artifact.



### apex-sdk-grpc-server

POM: [apex-sdk-grpc-server/pom.xml](../../apex-sdk-grpc-server/pom.xml).

No production Java sources; role selector dependency artifact.



### apex-sdk-messaging

POM: [apex-sdk-messaging/pom.xml](../../apex-sdk-messaging/pom.xml).

`apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/`: [ConsumerContext.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/ConsumerContext.java), [ConsumerFailureRecoveryStore.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/ConsumerFailureRecoveryStore.java), [ConsumerFailureReplay.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/ConsumerFailureReplay.java), [ConsumerFailureStatistics.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/ConsumerFailureStatistics.java), [ConsumerFailureStatus.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/ConsumerFailureStatus.java), [ContractEventHandler.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/ContractEventHandler.java), [DefaultEventHandlerRegistry.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/DefaultEventHandlerRegistry.java), [DispatchResult.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/DispatchResult.java), [EventHandler.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/EventHandler.java), [EventHandlerExecutor.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/EventHandlerExecutor.java), [EventHandlerKey.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/EventHandlerKey.java), [EventHandlerRegistry.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/EventHandlerRegistry.java), [EventMessageDispatcher.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/EventMessageDispatcher.java), [MalformedEventException.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/MalformedEventException.java), [NonRetryableEventException.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/NonRetryableEventException.java), [OnCellEvent.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/OnCellEvent.java), [ProcessedEventStore.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/ProcessedEventStore.java), [UnsupportedEventContractException.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/UnsupportedEventContractException.java).

`apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/internal/`: [AnnotatedEventHandlerDiscovery.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/internal/AnnotatedEventHandlerDiscovery.java), [DiscoveringEventHandlerRegistry.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/internal/DiscoveringEventHandlerRegistry.java), [ReflectiveEventHandlerAdapter.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/consumer/internal/ReflectiveEventHandlerAdapter.java).

`apex-sdk-messaging/src/main/java/com/apex/platform/messaging/internal/`: [DefaultCellEvents.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/internal/DefaultCellEvents.java).

`apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/`: [ApexTopicProvisioner.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/ApexTopicProvisioner.java), [ConfigurableKafkaEventConsumer.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/ConfigurableKafkaEventConsumer.java), [ConfigurableOutboxPublisher.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/ConfigurableOutboxPublisher.java), [ConsumerReliabilityProperties.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/ConsumerReliabilityProperties.java), [ConsumerTopics.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/ConsumerTopics.java), [JacksonEventMessageDispatcher.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/JacksonEventMessageDispatcher.java), [JacksonOutboxEventMapper.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/JacksonOutboxEventMapper.java), [JdbcOutboxAppender.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/JdbcOutboxAppender.java), [JdbcOutboxStore.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/JdbcOutboxStore.java), [JdbcProcessedEventStore.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/JdbcProcessedEventStore.java), [OutboxPublisherScheduler.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/OutboxPublisherScheduler.java), [PlatformCellMessagingAutoConfiguration.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/PlatformCellMessagingAutoConfiguration.java), [PlatformConsumerReliabilityAutoConfiguration.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/PlatformConsumerReliabilityAutoConfiguration.java), [PlatformTopicProvisioningAutoConfiguration.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/PlatformTopicProvisioningAutoConfiguration.java), [PublisherRouting.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/PublisherRouting.java), [ReliableKafkaConsumerRuntime.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/ReliableKafkaConsumerRuntime.java), [SimpleTransactionalEventHandlerExecutor.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/SimpleTransactionalEventHandlerExecutor.java), [TopicProvisioningProperties.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/TopicProvisioningProperties.java).

`apex-sdk-messaging/src/main/java/com/apex/platform/messaging/kafka/`: [KafkaMessagePublisher.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/kafka/KafkaMessagePublisher.java), [SpringKafkaMessagePublisher.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/kafka/SpringKafkaMessagePublisher.java).

`apex-sdk-messaging/src/main/java/com/apex/platform/messaging/observation/`: [MessagingObservation.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/observation/MessagingObservation.java).

`apex-sdk-messaging/src/main/java/com/apex/platform/messaging/outbox/`: [OutboxAppender.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/outbox/OutboxAppender.java), [OutboxEventMapper.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/outbox/OutboxEventMapper.java), [OutboxPublisher.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/outbox/OutboxPublisher.java), [OutboxRecord.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/outbox/OutboxRecord.java), [OutboxStatistics.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/outbox/OutboxStatistics.java), [OutboxStore.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/outbox/OutboxStore.java), [RetryPolicy.java](../../apex-sdk-messaging/src/main/java/com/apex/platform/messaging/outbox/RetryPolicy.java).

- [apex-sdk-messaging/src/main/resources/db/platform/V1001__platform_outbox_event.sql](../../apex-sdk-messaging/src/main/resources/db/platform/V1001__platform_outbox_event.sql)
- [apex-sdk-messaging/src/main/resources/db/platform/V1002__platform_processed_event.sql](../../apex-sdk-messaging/src/main/resources/db/platform/V1002__platform_processed_event.sql)
- [apex-sdk-messaging/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports](../../apex-sdk-messaging/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports)


### apex-sdk-observability

POM: [apex-sdk-observability/pom.xml](../../apex-sdk-observability/pom.xml).

`apex-sdk-observability/src/main/java/com/apex/observability/`: [ActiveOperations.java](../../apex-sdk-observability/src/main/java/com/apex/observability/ActiveOperations.java), [BoundedOperationalObserver.java](../../apex-sdk-observability/src/main/java/com/apex/observability/BoundedOperationalObserver.java), [ObservabilityAutoConfiguration.java](../../apex-sdk-observability/src/main/java/com/apex/observability/ObservabilityAutoConfiguration.java), [ObservabilityProperties.java](../../apex-sdk-observability/src/main/java/com/apex/observability/ObservabilityProperties.java), [ObservationBatchExporter.java](../../apex-sdk-observability/src/main/java/com/apex/observability/ObservationBatchExporter.java), [OperationalObservationHandler.java](../../apex-sdk-observability/src/main/java/com/apex/observability/OperationalObservationHandler.java), [OperationalObserver.java](../../apex-sdk-observability/src/main/java/com/apex/observability/OperationalObserver.java).

- [apex-sdk-observability/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports](../../apex-sdk-observability/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports)


### apex-sdk-operations

POM: [apex-sdk-operations/pom.xml](../../apex-sdk-operations/pom.xml).

`apex-sdk-operations/src/main/java/com/apex/operations/owner/`: [BoundedOwnerSearch.java](../../apex-sdk-operations/src/main/java/com/apex/operations/owner/BoundedOwnerSearch.java), [BoundedRecentSearch.java](../../apex-sdk-operations/src/main/java/com/apex/operations/owner/BoundedRecentSearch.java), [MessagingEvidenceReader.java](../../apex-sdk-operations/src/main/java/com/apex/operations/owner/MessagingEvidenceReader.java), [MessagingRecentSource.java](../../apex-sdk-operations/src/main/java/com/apex/operations/owner/MessagingRecentSource.java), [MessagingSearchSource.java](../../apex-sdk-operations/src/main/java/com/apex/operations/owner/MessagingSearchSource.java), [RecentCursor.java](../../apex-sdk-operations/src/main/java/com/apex/operations/owner/RecentCursor.java), [RecentIndex.java](../../apex-sdk-operations/src/main/java/com/apex/operations/owner/RecentIndex.java), [SearchCursor.java](../../apex-sdk-operations/src/main/java/com/apex/operations/owner/SearchCursor.java).

`apex-sdk-operations/src/main/java/com/apex/platform/contracts/`: [ContractDiscoveryAutoConfiguration.java](../../apex-sdk-operations/src/main/java/com/apex/platform/contracts/ContractDiscoveryAutoConfiguration.java), [ContractDiscoveryProvider.java](../../apex-sdk-operations/src/main/java/com/apex/platform/contracts/ContractDiscoveryProvider.java), [LocalContractHttpAutoConfiguration.java](../../apex-sdk-operations/src/main/java/com/apex/platform/contracts/LocalContractHttpAutoConfiguration.java), [LocalContractRead.java](../../apex-sdk-operations/src/main/java/com/apex/platform/contracts/LocalContractRead.java), [PublishedContracts.java](../../apex-sdk-operations/src/main/java/com/apex/platform/contracts/PublishedContracts.java).

- [apex-sdk-operations/src/main/resources/application-local-ops.properties](../../apex-sdk-operations/src/main/resources/application-local-ops.properties)
- [apex-sdk-operations/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports](../../apex-sdk-operations/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports)


### apex-sdk-spring-boot

POM: [apex-sdk-spring-boot/pom.xml](../../apex-sdk-spring-boot/pom.xml).

`apex-sdk-spring-boot/src/main/java/com/apex/platform/context/autoconfigure/`: [ApexExecutionContextAutoConfiguration.java](../../apex-sdk-spring-boot/src/main/java/com/apex/platform/context/autoconfigure/ApexExecutionContextAutoConfiguration.java).

- [apex-sdk-spring-boot/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports](../../apex-sdk-spring-boot/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports)


### apex-sdk-webmvc

POM: [apex-sdk-webmvc/pom.xml](../../apex-sdk-webmvc/pom.xml).

`apex-sdk-webmvc/src/main/java/com/apex/platform/webmvc/`: [ApexHttpExecutionContextFilter.java](../../apex-sdk-webmvc/src/main/java/com/apex/platform/webmvc/ApexHttpExecutionContextFilter.java), [ApexWebMvcAutoConfiguration.java](../../apex-sdk-webmvc/src/main/java/com/apex/platform/webmvc/ApexWebMvcAutoConfiguration.java).

- [apex-sdk-webmvc/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports](../../apex-sdk-webmvc/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports)

