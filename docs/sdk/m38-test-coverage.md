# M38 test coverage and evidence

Audit source: `a9202ee69a368cb8b9d8691be20db5ac831a11d0`, branch `credit-engine-event-sourcing`; clean working tree before this audit. Date: 2026-11-14. Java `C:/Program Files/Java/jdk-21`, Maven 3.9.11 from the user's local Maven wrapper distribution. M38 uses offline dependencies and clean target rebuilds; no commit, installation or push was performed. Generated logs under `out/m38` are local evidence, not portable release artifacts.

## New execution evidence

The initial command selected all 13 current SDK modules and prerequisites, rather than the full root product reactor:

```powershell
$auditModules = (Get-ChildItem -Directory -Filter 'apex-sdk-*' | Select-Object -ExpandProperty Name) -join ','
& $auditMaven -o -B -pl $auditModules -am clean verify
```

`out/m38/sdk-clean-verify.log`: Maven exit **1**. API compiled (no tests), SDK Boot passed 4 tests, messaging ran **126 tests, 1 failure, 0 errors, 0 skipped**. Reactor stopped at messaging; the other ten SDK modules were skipped by that command, not considered passing. Failure reports were preserved at `out/m38/first-messaging/TEST-*.xml` before another clean build.

The failure was `com.apex.platform.messaging.integration.TopicProvisioningIntegrationTest#provisionsLegacyOutgoingAndValidatedContractRoutesOnly`, test assertion line 84. Context startup failed at `ApexTopicProvisioner.java:91` with `Required topic metadata is unavailable or incompatible; check partition leaders and replicas`. Source `ensure()` creates then immediately describes once before checking leader/replicas. This supports G04's readiness-race hypothesis; it is not a proven environmental root cause or a fixed test.

A second command added `-fae` to collect all reachable modules after failures, with log `out/m38/sdk-clean-verify-fae.log`. Maven again exited **1**: **209 tests, 1 failure, 0 errors, 0 skipped tests** in the reached modules. Operations was skipped as a dependent Maven project, not counted as a passing project or skipped JUnit test. The second failure was a different method in the same suite: `TopicProvisioningIntegrationTest#provisionsIncomingAndCustomDltBeforeUnchangedReliabilityPreflight` (assertion line 68), again startup at ApexTopicProvisioner.java:91 reporting unavailable/incompatible metadata, this time for the `.quarantine` topic. Reports are preserved under `out/m38/second-messaging/`. Two failures in two clean runs establish a recurring qualification problem; exact timing/root cause remains to be proven. Neither command runs standalone Cells, Identity, Gateway, Console, the replay tool or the full Financial acceptance chain.

Because fail-at-end skipped Operations, a separate `mvn -o -B -pl apex-sdk-operations clean verify` was run without `-am`: **exit 0, 37 tests, 0 failures/errors/skips**, `out/m38/operations-clean-verify.log`. This used previously installed local dependency artifacts from the baseline, not newly installed artifacts from the failing reactor. It qualifies current Operations source in that dependency context, not a coherent clean full-reactor PASS.

| Module | Latest M38 tests | Failures/errors/skips | Command context |
| --- | ---: | --- | --- |
| apex-sdk-api | 0 | 0/0/0 | clean fail-at-end; no local test sources |
| apex-sdk-spring-boot | 4 | 0/0/0 | clean fail-at-end |
| apex-sdk-messaging | 126 | 1/0/0 | clean fail-at-end; first run also 126 with one different provisioning failure |
| apex-sdk-webmvc | 9 | 0/0/0 | clean fail-at-end |
| apex-sdk-grpc | 30 | 0/0/0 | clean fail-at-end |
| apex-sdk-grpc-client | 0 | 0/0/0 | clean fail-at-end; dependency artifact only |
| apex-sdk-grpc-server | 0 | 0/0/0 | clean fail-at-end; dependency artifact only |
| apex-sdk-discovery | 6 | 0/0/0 | clean fail-at-end |
| apex-sdk-discovery-eureka | 8 | 0/0/0 | clean fail-at-end |
| apex-sdk-discovery-kubernetes | 4 | 0/0/0 | clean fail-at-end |
| apex-sdk-discovery-grpc | 6 | 0/0/0 | clean fail-at-end |
| apex-sdk-observability | 16 | 0/0/0 | clean fail-at-end |
| apex-sdk-operations | 37 | 0/0/0 | separate clean run using installed dependencies |

The latest per-module reports sum to **246 tests with one failure**, but span two commands/dependency contexts. They must not be described as a successful 246-test reactor run. No further retries were used to seek a green result. Source and test files remained unchanged throughout.

## Test-suite inventory and actual boundaries

Test paths in this table are below each module's `src/test/java/`. Counts in the M37 column come from committed `docs/m37-verification.json`, not an M38 assertion that those tests just passed. API and role selector zero counts are disclosed deliberately.

| Module | M37 tests | Existing suites / package and verified behaviors | Coverage limits / needed cases |
| --- | ---: | --- | --- |
| apex-sdk-api | 0 | No local tests; contracts exercised by messaging/gRPC/Operations consumers. | Direct scope misuse, option/result boundaries and compatibility fixtures G02/G12/G13. |
| apex-sdk-spring-boot | 4 | `com/apex/platform/context/autoconfigure/{DefaultSdkBoundaryTest,ApexExecutionContextAutoConfigurationTest}.java`: minimal boot, one provider, custom bean replacement. | No generic executor, authentication or trace context adapter. |
| apex-sdk-webmvc | 9 | `com/apex/platform/webmvc/integration/ApexWebMvcAutoConfigurationTest.java`; `com/apex/starter/webmvc/integration/WebMvcStarterTest.java`; architecture boundary test. Real servlet filter registration/root scope and error cleanup, no Kafka/JDBC classpath. | No Gateway ingress header, outbound HTTP, async redispatch or OAuth flow. |
| apex-sdk-messaging | 126 | `com/apex/platform/messaging/consumer/internal/{AnnotatedEventHandlerDiscoveryTest,ReflectiveEventHandlerAdapterTest}.java`; `integration/{AnnotatedConsumerIntegrationTest,ConsumerReliabilityWiringTest,ConsumerTopicsTest,ObservationTransactionTest,PlatformCellMessagingAutoConfigurationTest,PublisherRoutingTest,ReliableConsumerIntegrationTest,TopicProvisioningIntegrationTest,TopicProvisioningSafetyTest}.java`; `unit/{DefaultCellEventsTest,SimpleTransactionalEventHandlerExecutorTest}.java`; starter integration/architecture tests. Includes real embedded Kafka KRaft and Zonky PostgreSQL. | Crash/process-kill multi-instance lease qualification, explicit send bound, full decoder evolution and production Kafka ACL/multi-broker behavior absent. Provisioning failure observed now. |
| apex-sdk-grpc | 30 | `com/apex/platform/grpc/internal/{GrpcRuntimeTest,ContractValidationTest,CellGrpcChannelProviderTest,MultiResponderGatewayTest,CollectorWiringTest,CollectorDeadlineIntegrationTest,RpcObservationTest}.java`: real socket request/query, fail-closed production config, JSON/contract validation, proxy handling, response checks, ambiguous failures, local worker cleanup and deadlines, collector saturation/cancel/partial/strict slots. | Not a universal durable receipt implementation; detached nested budget, channel churn, actual user OAuth and distributed tracing absent. |
| apex-sdk-grpc-client | 0 | No local Java tests. Role dependency isolation in historical packaged installed consumer `rpc-client`. | Package/role selection evidence does not imply every inherited runtime behavior is qualified. |
| apex-sdk-grpc-server | 0 | No local Java tests. Role dependency isolation in historical packaged installed consumer `rpc-server`. | Same limitation. |
| apex-sdk-discovery | 6 | `com/apex/platform/discovery/DiscoveryTest.java`: forged metadata/authority/port rejection, duplicate endpoints, replica changes, freshness, environment/epoch scope. | Membership enumeration reads provider snapshots more than once; no atomic cluster snapshot guarantee. |
| apex-sdk-discovery-eureka | 8 | `com/apex/discovery/eureka/EurekaMetadataTest.java`: metadata pinning, registration disabled, actual allocated gRPC port, freshness through Spring scoped proxy, startup before initial refresh, zero-port rejection. | Registry client/manager fixtures are not an entire deployment lifecycle; historical external mode adds owned registry. |
| apex-sdk-discovery-kubernetes | 4 | `com/apex/discovery/kubernetes/EndpointSliceApiTest.java`: local HTTP API, namespaced Pod IPs, ready/terminating filters, replacement, stale expiry and forged transport metadata. | No real API server/RBAC/projected credential rotation, live drain or IPv6. G10/G11 qualification BLOCKED on cluster fixture. |
| apex-sdk-discovery-grpc | 6 | `com/apex/discovery/grpc/{DirectoryRoundRobinTest,DynamicCellChannelsTest}.java`: round-robin real sockets, dynamic addition/removal, startup before replicas, TLS wrong-name/peer/client rejection, request metadata/idempotency and one attempt. | No full deployed rolling drain/registry partition qualification or large cache churn. |
| apex-sdk-observability | 16 | `com/apex/observability/{ObservabilityTest,ObservationCostTest,FixtureExportConfiguration}.java`: fixed resource budgets, drop behavior, exporter timeout/circuit, active expiry and diagnostic cost. Fixture configuration is not itself a behavior test. | No exported distributed trace graph; samples are not durable evidence. |
| apex-sdk-operations | 37 | `com/apex/platform/contracts/{ContractDiscoveryProviderTest,LocalContractHttpTest,OperationsOptionalCapabilitiesTest}.java`; `com/apex/operations/owner/{MessagingEvidenceReaderTest,OwnerSearchTest,OwnerRecentSearchTest}.java`: real PostgreSQL reads, commit uncertainty, restricted roles, cursors/epochs/expiry, deadline/rate/admission bounds, scan completeness/index plan assertions and optional classpath backoff. | Application endpoint auth/resource policy remains separate; no one-transport manifest behavior, production query budget or cluster membership parity certification. |
| **Total** | **246** | M37 recorded 0 failures/errors/skips across SDK only. | Not full product regression evidence; M38 first attempt already differs. |

Tests using embedded Kafka/PostgreSQL are real transport/database integration tests, not external production services. Round-robin/TLS socket tests establish local runtime behavior. Configuration runner/filtered-classloader tests establish bean and dependency boundaries. None alone proves independent deployed processes under crashes, partitions, credential rotation and rolling upgrades.

## External-consumer evidence

`tools/m35-6/external-consumer/pom.xml` uses `apex.sdk.version` and installed Maven artifacts. `tools/m35-6/verify-external-consumer.ps1` copies it outside the repository, sanitizes relevant application environment variables, verifies runtime dependency trees and boots packaged JARs. This is stronger evidence than a reactor test compiling against source directories.

The committed M37 ledger records seven successful modes: default, HTTP, capabilities, observability, RPC client, RPC server and Operations, completed `2026-11-12T09:55:55.6428395+00:00`. Local receipt: `out/m35-6/external/20261112095423/receipt.json`; log `out/m37/external-final-boundaries.log`. Capability mode uses owned PostgreSQL/Kafka, real HTTP/2 gRPC and an owned Eureka process; others test packaged boot and dependency isolation. Source fixtures include `src/capability-test/java/external/capabilities/InstalledCapabilitiesTest.java`, `src/isolation-test/java/example/CapabilityIsolationTest.java` and `src/observability-test/java/example/ObservationConsumerTest.java`.

M38 inspected these fixtures/receipts but did **not** install SDK artifacts or rerun these modes. They qualify the unchanged M37 source/artifact set within their stated scope, not future M38 recommendations. The fixture requires a prebuilt registry JAR at `out/m34-artifacts/apex-discovery-server.jar`; the path/owned process must be regenerated and fingerprinted for a release. A clean local Maven cache or actual artifact repository consumer, mixed supported SDK versions and security/tracing/cluster modes remain G12/G14 work.

## Reconciliation of M37 application failures

M37 report text saying "implementation is uncommitted" describes that earlier session; its code is now the supplied committed baseline. Preserve its test findings without treating its working-tree wording as current state.

| Area | Most specific retained M37 evidence | M38 disposition |
| --- | --- | --- |
| SDK | Final ledger: 246 tests, 0 failures/errors/skips; final clean RPC/discovery/Console rebuild passed. | Historical PASS only; first M38 clean attempt failed provisioning. Final repeat below must be read alongside it. |
| Order standalone | `out/m37/order-cell.log`: 42 tests, 7 failures, 7 errors. Focused old-POM `OrderGrpcLocalTest` reproduced two startup errors (`order-baseline.log`). | Unresolved application/profile integration; SDK EventHandlerRegistry is conditional on consumer activation. Baseline reproduction proves that subset, not all Order failures. |
| Order full-chain | `out/m37/platform-retry.log`: discovery-profile Order 42 tests, 6 failures, 8 errors; chain stops here. | Full acceptance remains FAILED; distinct profile/run counts must not be combined with standalone counts. |
| Inventory | `out/m37/inventory-cell.log`: 36 tests, one failure, base listener running when smoke expects paused; same failure with old committed POM in inventory-baseline.log. | Existing application/profile expectation failure, not evidence requiring globally paused SDK listeners. Not rerun/fixed in M38. |
| Payment | `out/m37/payment-cell.log`: 19 tests, three simulation decline/no-policy failures; same three in payment-baseline.log. | Business/runtime-policy discrepancy; not a generic SDK defect; not rerun/fixed. |
| Reporting receipt | Direct standalone run failed clean-build prerequisite; an earlier receipt was invalidated by changed discovery boundary fingerprint. | Final isolated source/artifact-consistent Customer→Reporting chain passed (`customer-reporting-isolated.log` and result JSON). Do not report the earlier invalid receipt as valid. |
| Provisioning transient | M37 `out/m37/topic-provisioning-retry-failure.xml` retained; subsequent isolated chain passed without source/test changes. | Similar class of failure to current G04. Later success does not prove race resolved. |
| Identity/Gateway/Console | M37 Identity prerequisites and standalone Gateway/Console verification passed; topology suites passed. | Not fresh M38 runs. Live Operations deployment and later full-chain Financial/cross-Cell stages were not qualified. |

`docs/m37-verification.json` explicitly warns that standalone JUnit directories may include earlier runs. Its aggregate snapshot Inventory counts (43 with 2 failures/2 errors) do not replace the command-specific 36/one-failure result. Read command logs and profiles before adding totals. Old POM/dependency reproduction suggests certain failures predated consolidation, but does not prove that the current SDK is innocent of every application integration failure.

## Required acceptance coverage beyond existing tests

| Gate | Required evidence | Gap / milestone |
| --- | --- | --- |
| Context | HTTP ingress/egress + async worker + Kafka + RPC with one correlation and safe cleanup | G01/G02; SDK-2 |
| Security | External Resource Server, real issuer/JWKS rotation, machine/user separation, tenant/scope and resource authorization | G03; SDK-3 |
| Reliable processing | Two processes, claim/reclaim/fencing, crash after ACK/before mark, concurrent duplicate Inbox and owner request receipt, bounded send/stop | G05/G07; SDK-4/5 |
| RPC | Detached nested deadlines/cancellation, high-cardinality channel churn, ambiguity with one mutation attempt | G07/G09; SDK-5 |
| Discovery | Owned registry partition and live cluster readiness/drain/token rotation/environment/epoch | G10/G11; SDK-6 |
| Observability | Collector-received trace parent/link graph and exporter outage isolation across all supported boundaries | G08; SDK-7 |
| Contracts/releases | Binary/API/config/protobuf/JSON/SQL version fixtures, repository-published standalone consumers | G06/G12/G13; SDK-1/8 |
| Product | Every standalone application/profile and explicit IT phase, valid Customer/Reporting receipt and complete Financial/packaged chain | G14; SDK-8 |

No broad refactors or new capabilities were implemented as part of this audit. Tests were executed to qualify findings; existing failure evidence is retained and is not bypassed by reducing scope.
