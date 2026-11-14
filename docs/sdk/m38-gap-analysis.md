# M38 SDK gap analysis

Baseline: `a9202ee69a368cb8b9d8691be20db5ac831a11d0`, 2026-11-14. Status definitions and complete module coverage are in [capability matrix](m38-capability-matrix.md); actual execution evidence is in [test coverage](m38-test-coverage.md).

P0 means blocks enterprise readiness or trust/reliability qualification; P1 means needed before the relevant capability is adopted in production; P2 means bounded ergonomics/documentation improvement. Complexity estimates are relative engineering effort including tests: S = 1–3 engineering days, M = 4–8, L = 9–15, XL = 16–25. They are planning estimates, not delivery commitments; environments/application fixes are additional. Entries distinguish demonstrated defects, source-level limitations and unqualified behavior.

## G01 — HTTP propagation: PARTIAL, P0, M

**Evidence:** `apex-sdk-webmvc/src/main/java/com/apex/platform/webmvc/ApexHttpExecutionContextFilter.java#doFilterInternal` always creates a root and never reads request headers or writes response headers. `ApexWebMvcAutoConfiguration.java` only installs that filter. `apex-api-gateway/src/main/java/com/apex/gateway/EdgeHeaders.java` supplies X-Correlation-ID/X-Request-ID. No outbound HTTP adapter is registered by SDK modules.

**Limitation/impact:** a Gateway-to-Cell request starts unrelated business correlation; HTTP downstream calls cannot share causation consistently. Existing HTTP cleanup tests do not validate propagation. Operational searches cannot reliably follow the customer request.

**Recommendation:** agree canonical bounded UUID headers, trust rules and causation semantics; preserve valid correlation, replace invalid input, emit response IDs and add explicitly supported Spring HTTP client interceptors. Keep trace IDs and credentials separate. Support servlet async redispatch/task capture together with G02, preserving the public ExecutionContext record.

**Dependencies:** Gateway header contract, G02, independent G03 authentication policy. **Acceptance:** packaged Gateway/header fixture → servlet A → HTTP B → event → RPC retains one correlation; distinct causation at each operation; invalid/duplicate/oversized headers, errors, async completion and thread reuse clean up; filters back off and egress is opt-in.

## G02 — General asynchronous context: MISSING, P0, M

**Evidence:** `apex-sdk-api/src/main/java/com/apex/platform/core/context/ThreadLocalExecutionContextProvider.java` stores only thread-local scopes. `apex-sdk-spring-boot/src/main/java/com/apex/platform/context/autoconfigure/ApexExecutionContextAutoConfiguration.java` registers only the provider. `DefaultQueryGateway.java` and `DefaultMultiResponderGateway.java` explicitly scope their own workers, demonstrating a bounded special case rather than a general adapter.

**Limitation/impact:** @Async, application executors, scheduled work and CompletionStage callbacks lose context or require manual scopes; callbacks may execute on RPC completion threads. Missing context can prevent event append or break causation.

**Recommendation:** additive capture/wrap helpers and an opt-in Spring TaskDecorator or supported context-propagation accessor; establish how root context is created for scheduled jobs. Restore previous context in finally and preserve scope thread ownership. Do not imply database transactions travel with tasks. Reactor support is OPTIONAL until a Cell use case exists; Gateway is already reactive application code.

**Dependencies:** existing provider SPI, G01; authentication adapter G03 when enabled. **Acceptance:** nested tasks, pooled/virtual threads, success/exception/rejection/cancellation, tasks without context and CompletionStage handoff; no thread leakage, no reuse of caller transaction, no forced global executor replacement.

## G03 — Security Resource Server/context integration: MISSING, P0, XL

**Evidence:** all 13 SDK POMs lack an OAuth2 Resource Server dependency. `ExecutionContext.java` has no principal/tenant/authorities. `apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/GrpcInvocationClient.java` attaches three provenance headers. `RpcDispatcher.java` puts the verified mTLS service caller in RequestContext; this is not end-user identity. Console's `OpsM36aSecurityConfiguration.java` validates issuer, audience, tenant and scope with Spring/Nimbus. Gateway `EdgeHeaders.java` currently strips Authorization.

**Limitation/impact:** independently deployed Cells have no common SDK recipe/adapter for validating access tokens, obtaining authenticated context or integrating authorization. Copying Console policy risks app-specific restrictions and divergent validation. An unverified tenant header must never become authority.

**Recommendation:** first specify machine credentials vs delegated-user calls and token audience rules with Identity/Gateway owners. Add opt-in Spring Security Resource Server integration using supported decoder/validator APIs and explicit issuer/audience configuration. Expose a separate immutable authenticated-context/accessor contract and authorization extension point; retain ExecutionContext constructors and wire contracts. Use Spring OAuth2 client support for the defined machine-token use case; do not blindly forward a user's bearer token to arbitrary audiences. Integrate with InvocationAuthorizer where appropriate while retaining mTLS service identity. Keep business resource decisions in each owner and login/issuance in Identity. Initially use optional dependencies in existing SDK boundaries; a separate module requires a demonstrated isolation need.

**Dependencies:** agreed Identity discovery/JWKS and claims contract, application resource policies, Gateway authentication/forwarding decision, G02. These design/runtime inputs block final qualification, not discovery of the missing SDK support. **Acceptance:** external Resource Server boot with no token/bad signature/wrong issuer/audience/expired/not-yet-valid tokens → 401; valid identity lacking scope or wrong tenant → 403; valid user and machine fixtures, key rotation/cache outage, no claims/header spoofing; context cleanup across HTTP/async/RPC; disabled mode has no security dependency activation; custom application chain backoff; no token or claims leakage in logs. No manual OAuth2/JWT cryptography.

## G04 — Topic provisioning metadata readiness: PARTIAL, P0, M

**Evidence:** `apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/ApexTopicProvisioner.java#ensure` describes immediately after create, then rejects missing leader/replicas without another readiness poll. `TopicProvisioningIntegrationTest#provisionsLegacyOutgoingAndValidatedContractRoutesOnly` failed in M38's first clean run with "Required topic metadata is unavailable or incompatible". M37 retained a separate provisioning failure XML before its passing isolated chain.

**Limitation/impact:** creation acknowledgement is not proof that every metadata read already has a leader. Startup can fail during normal convergence. The source and observed error suggest a readiness race; the exact broker timing cause is not proven by a debugger/packet trace. Do not mark it an environment-only failure or fixed because a retry passes.

**Recommendation:** bounded monotonic readiness polling within the configured startup budget for transient metadata states; keep authorization, impossible replication, incompatible partition counts and invalid names fail-closed. Preserve no automatic partition mutation and redacted diagnostics.

**Dependencies:** Kafka Admin API; source/DLT topology. **Acceptance:** deterministic delayed leader/visibility fixture, create races, budget exhaustion, ACL failure, incompatible existing DLT and repeated embedded-broker startup; prove listeners never start early and total initialization is bounded even across multiple topics. Retain first failure evidence.

## G05 — Outbox operational bounds and lease semantics: PARTIAL, P0, L

**Evidence:** `ConfigurableOutboxPublisher.java#publishBatch` claims 100 records with a fixed five-minute lease and calls CompletionStage.join() for each send; `OutboxPublisherScheduler.java` has only interval-ms configuration. `RetryPolicy.java` hardcodes ten retries/delays and uses Instant.now rather than injected Clock. `JdbcOutboxStore.java` fences updates by token, but there is no claim renewal. `JdbcOutboxAppender.java` is publicly callable without the facade's transaction guard.

**Limitation/impact:** a custom publisher future can block the scheduler indefinitely; a slow batch can outlive leases and publish duplicates or reorder same-aggregate records across publishers. Kafka's own delivery timeout can bound the default producer, but the SDK abstraction does not enforce a budget. Token fencing prevents stale state updates, not duplicate transport delivery. Direct appender use can commit intent outside the intended business transaction.

**Recommendation:** typed bounded batch/send/lease/retry/shutdown policy, injectable Clock, validated lease versus batch strategy, and an explicit supported transaction contract for low-level append. Prefer smaller claims or renewal/fencing design justified by measurements; document at-least-once behavior and partition-key ordering limits. Do not promise global ordering or exactly-once external effects. Add strict aggregate ordering only if a specific contract requires it; otherwise consumers must tolerate reordering.

**Dependencies:** PostgreSQL same-owner transaction, Kafka delivery configuration, G13. **Acceptance:** two publishers, killed publisher, expired/reclaimed lease, old-token mark failure, broker ACK followed by process death before SQL mark, never-completing/slow send, DB mark failure, bounded stop, duplicate delivery with one Inbox business effect. Verify supported low-level append policy and two-transaction-manager misconfiguration.

## G06 — Event compatibility/replay boundary: PARTIAL, P1, M

**Evidence:** `apex-sdk-api/src/main/java/com/apex/platform/events/api/EventContract.java` exposes schemaId; `EventMetadata.java` does not carry it. `JacksonEventMessageDispatcher.java` uses a general mapper rather than RpcCodec-style explicit size/depth/duplicate-field policy. `ConsumerFailureReplay.java`, `ConsumerFailureRecoveryStore.java`, `ConsumerFailureStatistics.java` and `outbox/OutboxStatistics.java` are interfaces without SDK runtime beans. `apex-replay-tool/src/main/java/com/apex/replay/ReplayExecutor.java` is separate operator infrastructure.

**Limitation/impact:** event type/version drives routing, but schemaId is descriptive; there is no automated producer/consumer version-pair compatibility guarantee. Kafka record-size settings bound transport, not SDK decoder policy. Users may mistakenly expect the failure interfaces to provide a durable failure ledger/replay engine.

**Recommendation:** document exact event evolution rules and schemaId semantics; add versioned JSON fixtures and explicit configurable decode bounds for the supported contract. Preserve existing envelope compatibility. Document the failure SPIs as extension contracts; keep operator plan/approval/replay execution in the existing standalone replay tool. Do not add a second ledger or a speculative schema registry.

**Dependencies:** contract owners, current replay tool routing and same consumer-group dedup policy, G12. **Acceptance:** old producer/new consumer and reverse supported pairs, unknown version → quarantine in reliable mode, malformed/deep/oversized/duplicate JSON policy, original event ID and key retained through replay, replay crash duplicates yield one business effect. Existing bare SPIs are OPTIONAL runtime capabilities, not falsely classified COMPLETE.

## G07 — RPC deadline composition and owner idempotency qualification: PARTIAL, P0, L

**Evidence:** `GrpcInvocationClient.java#invoke` uses options.deadline to build a stub; `RpcServerAuth.java` bounds each inbound deadline; `RpcDispatcher.java` passes a deadline to owner RequestContext. `DefaultQueryGateway.java` creates local deadlines from now, and collector workers explicitly restore ExecutionContext but not the full incoming gRPC Context. `TransactionalRequestExecutor.java` specifies claim/business/result atomic commit; `GrpcServerAutoConfiguration.java` checks only bean presence.

**Limitation/impact:** individual calls are bounded, but remaining parent budget/cancellation is not explicitly preserved at all worker/collection boundaries. Native gRPC may inherit its current Context on direct calls; that does not qualify detached SDK workers. Bean presence cannot establish durable atomic receipts across replicas. Retrying an ambiguous mutation without the same identity/key can duplicate business work.

**Recommendation:** compute a monotonic effective deadline from requested and remaining parent budget; propagate cancellation intentionally through supported worker handoffs. Keep cancellation honest: a completed business commit can outlive a lost response. Supply an owner SPI conformance fixture/reference recipe rather than a central receipt store; business owners keep schemas and atomic transaction logic.

**Dependencies:** owning persistent executor implementation, G02, two-process fixture. **Acceptance:** parent with little budget → nested local/remote/collector calls never gain a new full budget; cancel interrupts outstanding reads; uncooperative handler classified honestly. Two replicas receive same key concurrently; crash after commit/before response; cached compatible response after restart; different payload/principal/tenant/contract version rejected; DB commit failure yields no success. No automatic mutation resend. Existing direct runtime tests remain useful but insufficient for all owner adapters.

## G08 — Distributed tracing: MISSING, P0, L

**Evidence:** `apex-sdk-observability/pom.xml` adds metrics, not a tracing bridge/exporter. `RpcObservation.java` and `MessagingObservation.java` start/stop observations without a complete cross-transport carrier/scope contract. `OperationalObserver.Sample` stores operation/outcome/duration only. HTTP filter does not bridge tracing to business context.

**Limitation/impact:** bounded diagnostics and low-cardinality metrics exist, but do not establish one distributed trace from edge through Outbox delay, Kafka consumer and gRPC workers. Correlation UUIDs are not W3C trace IDs. Installing an application tracing bridge alone does not prove continuity through SQL persistence.

**Recommendation:** select the supported Micrometer tracing/OpenTelemetry bridge and native HTTP/Kafka/gRPC instrumentation; add minimal adapters only where native support does not cover SDK handoffs. Define safe span link/parent behavior across durable Outbox and replay without changing existing records incompatibly. Keep exporter/backend optional and diagnostics independent of business success.

**Dependencies:** G01/G02, trace storage/carrier decision, disposable collector. **Acceptance:** packaged HTTP → SQL Outbox → Kafka → RPC → async path yields expected spans/links and durations, preserves trace/business ID distinction, no payload/principal tags, no unbounded baggage; sampling/exporter outage and disabled mode do not change transaction/ACK/cancellation semantics. Backend hosting is standalone infrastructure, not an SDK module.

## G09 — RPC channel cache and aggregate shutdown bounds: PARTIAL, P1, M

**Evidence:** `apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/CellGrpcChannelProvider.java#channel` caches discovered logical Cells without a global size/idle bound. `instanceChannel` evicts one arbitrary map entry only on the instance path at size 128. `close` waits up to five seconds separately for each channel.

**Limitation/impact:** callers supplying many syntactically valid discovered names can grow channels/resolvers; shutdown budget grows with channel count. The instance cap is not a cap on logical channel creation. Existing reuse/close tests do not establish high-cardinality boundedness.

**Recommendation:** validate trusted destination policy, bound all owned cache entries with explicit eviction/close semantics, and apply one aggregate shutdown deadline. Preserve ordinary channel reuse and static/discovered selection behavior.

**Dependencies:** discovery policy, Boot channel ownership semantics. **Acceptance:** high-cardinality logical/instance requests, concurrent create/evict/close, active calls during eviction, no leaked resolver threads, bounded total stop and no creation after close.

## G10 — Discovery lifecycle/failover qualification: PARTIAL, P1, L

**Evidence:** `EurekaCellAutoConfiguration.java` publishes actual bound port and freshness but uses separate apex registration and native Eureka flags. `DirectoryNameResolverFactory.java` refreshes address groups; `DiscoveryFreshnessInterceptor.java` checks current freshness. Server workers use destroyMethod shutdown; no SDK-wide tested readiness withdrawal/drain sequence. `EndpointSliceApiTest.java` uses a local API fixture, not a cluster.

**Limitation/impact:** round-robin refresh and stale refusal are tested; orderly rollout, registry partition, in-flight drain and abrupt process kill are not comprehensively qualified as a deployed system. Eureka is eventually consistent and native retry disabling deliberately prevents mutation replay; failover means a later call can choose a healthy endpoint, not guaranteed transparent recovery of an in-flight mutation.

**Recommendation:** document native registration/readiness/shutdown properties; build disposable two-process qualification against owned Eureka and Kubernetes deployment fixtures. Add SDK lifecycle hooks only if these tests demonstrate missing behavior. Avoid replacing native discovery/runtime features.

**Dependencies:** owned Eureka, disposable TLS/RBAC-enabled Kubernetes, G07/G09/G11. **Acceptance:** add/remove/restart replicas; readiness false before drain; SIGTERM/forced kill; registry outage beyond max-stale; old endpoints cannot accept new work after policy deadline; one mutation attempt, ambiguous errors retained; bounded shutdown. **BLOCKED qualification:** live Kubernetes environment/fixture is not supplied by existing tests.

## G11 — Kubernetes credential rotation and membership parity: PARTIAL, P1, L

**Evidence:** `KubernetesCellAutoConfiguration.java#apexKubernetesClient` reads token-file once and sets withOauthToken; no file refresh hook is configured. `EndpointSliceDiscoveryClient.java#refresh` emits Cell/authority/port/namespace only. `CloudCellServiceDirectory.java#membership` requires apex.environment/apex.instance-id/apex.epoch, which the Kubernetes adapter does not emit. Eureka optionally creates CellRuntimeIdentity but Kubernetes does not.

**Limitation/impact:** there is no demonstrated rotation of projected service account credentials after startup. Operational membership cannot be declared complete for Kubernetes from current emitted metadata; this is separate from successful gRPC endpoint routing.

**Recommendation:** use supported Fabric8 credential refresh/file facilities after verifying the resolved library behavior; qualify replacement token reads. Define where environment/replica/boot epoch come from in Kubernetes and produce truthful membership scope; do not invent process epoch from Pod UID if container restarts share that UID. IPv6-only routing is OPTIONAL until a deployment requirement exists; current contract explicitly supports IPv4.

**Dependencies:** cluster RBAC and projected token fixture; Operations identity contract, G10. **Acceptance:** token file rotates, old credential revoked, poll succeeds with new credential; unreadable/expired credential eventually makes directory unavailable; correct restart epoch/environment isolation, missing metadata returns partial, no cross-namespace reads. Live qualification remains BLOCKED until cluster fixture is available.

## G12 — Compatibility and release management: PARTIAL, P0, L

**Evidence:** root `pom.xml` aggregates SDK with Financial modules and inherits Boot 4.1.1; SDK artifacts use 1.0-SNAPSHOT. Cloud BOM 2025.1.3 is repeated in discovery modules. No parent-owned SDK dependencyManagement, API compatibility plugin, protobuf compatibility gate, immutable release profile or release-consumer matrix is declared. `RpcCodec.java` rejects unknown JSON fields, so additive record changes at the same contract version are not automatically compatible. `docs/m37-report.md` explicitly changes Maven coordinates without relocation JARs.

**Limitation/impact:** independent consumers must align versions themselves; source/package preservation does not guarantee binary/wire compatibility or successful publication outside the monorepo. Cached receipt bytes can outlive deployments. SDK API includes public classes in internal packages used across modules.

**Recommendation:** manage existing SDK coordinates/Cloud versions in the existing parent first; document consumer platform alignment and migration from old coordinates. Freeze public API/config/wire/SQL baselines and enforce compatible additive changes or explicit version bumps. Define supported Java/Boot/Cloud matrix and immutable publication with source/Javadoc artifacts, checksums and packaged external-consumer gates. A separate BOM module is OPTIONAL until external consumers demonstrate need; do not create modules simply for the audit.

**Dependencies:** release artifact repository/policy, contract owners, G14. **Acceptance:** consumer without reactor parent compiles/boots from installed/published artifacts; deliberate binary-breaking signature and protobuf/config/JSON change fail the compatibility gate; supported cross-version request/query/event/receipt fixtures pass; old coordinates have clear migration behavior; SDK and application graphs contain no business/executable dependencies. Upstream support-policy verification is a release task, not inferred from successful compilation.

## G13 — Configuration consistency and developer contract: PARTIAL, P1, M

**Evidence:** `ApexWebMvcAutoConfiguration.java` requires spring.application.name and apex.cell.id; `GrpcCommonAutoConfiguration.java` falls back from apex.grpc.cell-id to apex.cell.id to application name. Discovery normalizes to lowercase while gRPC accepts broader identifiers. Messaging combines @Value toggles and typed properties; fixed Outbox bounds are not properties. `ObservabilityProperties.java` defaults instanceId to local. SDK POMs declare no configuration annotation processor. SDK API has no local tests.

**Limitation/impact:** a service may boot differently under HTTP versus gRPC/discovery; misspelled or partial optional configuration can produce late bean errors. Configuration metadata and examples do not constitute an exhaustive validated consumer contract. Multiple instances can share a diagnostics instance label unless explicitly set.

**Recommendation:** publish an exhaustive property/alias/activation catalog, common identity rules, typed validated groups and generated metadata without removing existing keys. Validate required dependencies early with actionable errors only when selected. Add focused API contract tests for scope misuse, options/results immutability and boundary validation; improve a small external-consumer guide instead of broad refactoring.

**Dependencies:** G05/G12, established property compatibility. **Acceptance:** property table matches generated metadata/defaults; disabled/minimal/client/server/consumer/publisher combinations boot independently; invalid bounds and conflicting identities fail early; custom bean backoff; no forced database/discovery/security settings for default starter; application.yml example demonstrates explicit migration locations without inheriting Financial datasource defaults.

## G14 — Evidence and product regression gate: PARTIAL, P0, L

**Evidence:** `docs/m37-verification.json`, `out/m37/platform-retry.log`, `out/m37/order-baseline.log`, `inventory-baseline.log`, `payment-baseline.log`, `customer-reporting-isolated.log`, and M38 logs described in coverage. `tools/m35-6/verify-external-consumer.ps1` tests seven installed modes; capability mode requires an owned registry JAR at out/m34-artifacts. Root reactor omits most standalone applications/Cells, and SDK module tests alone cannot qualify them.

**Limitation/impact:** M37 full-product acceptance failed. Current clean SDK failure and historical transient provisioning evidence preclude an unconditional green SDK claim. IT-suffixed application tests require explicit execution wiring. Historical source/artifact fingerprints must not be silently reused after implementation changes.

**Recommendation:** keep a run-specific ledger of commands, exact baseline, exits, test counts/skips, environment, owned services, source/artifact fingerprints and preserved failures. Resolve SDK provisioning stability and classify Cell/profile failures with their owners; rerun the complete chain only after prerequisites. Add deployed multi-instance reliability/security/tracing scenarios as capabilities are implemented, not speculative application redesign.

**Dependencies:** G04 and relevant milestone fixtures; Order/Inventory/Payment owners; packaged Identity/discovery and Financial acceptance environment. **Acceptance:** clean SDK run, fresh isolated installed-consumer matrix, standalone application tests including explicit IT phases, fingerprinted Customer/Reporting chain, full product command all pass at the same source set; retained known failures are either fixed with evidence or explicitly prevent release. Repeated clean runs qualify the diagnosed race without deleting failure evidence. **BLOCKED:** full-product readiness until the known application regressions and unreached stages are resolved; not a reason to stop SDK audit work.

## G15 — Operations optional capability semantics: PARTIAL, P2, S–M

**Evidence:** `apex-sdk-operations/pom.xml` makes messaging/RPC/JDBC/servlet optional. `ContractDiscoveryAutoConfiguration.java` nevertheless has ConditionalOnClass requiring both HandlerRegistry and EventHandlerRegistry. `OperationsOptionalCapabilitiesTest.java` checks backing off without both; it does not prove useful metadata for every one-transport consumer. `BoundedOwnerSearch.java`/`BoundedRecentSearch.java` require owner-provided policies/read connections; no universal owner endpoint is installed.

**Limitation/impact:** an HTTP-only or RPC-only consumer can install Operations successfully yet receive no contract manifest. Owner evidence adapters are not a fully secured operational application. The SDK correctly avoids claiming a durable progress history from bounded scans.

**Recommendation:** define supported manifest behavior for one-transport applications; either support partial descriptors through capability-isolated factories or fail selected unsupported combinations with a clear explanation. Do not add Console topology storage or business owner endpoints to the SDK. Keep read-only DB role/index prerequisites explicit.

**Dependencies:** real HTTP-only/RPC-only metadata use case, G12/G13. **Acceptance:** filtered-classloader matrix for neither/events-only/RPC-only/both; chosen behavior documented; partial manifests say which descriptors are unavailable; no optional class linkage error or new transport activation. Verify owner readers never select payload/token columns and authorization stays at the owning endpoint.
