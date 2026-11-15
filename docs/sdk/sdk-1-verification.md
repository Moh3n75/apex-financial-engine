# SDK-1 verification and readiness

Repository `Moh3n75/apex-financial-engine`, branch `credit-engine-event-sourcing`; committed input checkpoint **`8a2f822`**, Java 21. The working tree was clean at the start. The four M38 audits and SDK-0/0.1 report were read before implementation. Historical `out/m38`, `out/sdk-0` and `out/sdk-01` evidence remains retained. No commit or push is authorized/performed by this work.

## Before and implementation

SDK-0.1's checkpoint records three clean 273-test runs and four installed checks. There was no machine API/config/protocol baseline, no generated metadata processor, SDK versions were repeated and provider Cloud BOMs duplicated. SDK-1 adds these gates and fixture coverage without changing existing public signatures, event packages/models, authentication policy or business Cell boundaries. The checkpoint/final declaration comparison added one package-private discovery preflight class only. Configuration changes reject invalid selected settings earlier; valid keys/defaults and identity precedence remain supported.

Deliverable details: [API baseline](sdk-1-api-baseline.md), [configuration contract](sdk-1-configuration-contract.md), [compatibility matrix](sdk-1-compatibility-matrix.md). The existing parent/importable POM is reused; no SDK module or BOM artifact was added.

## Retained development failures and controls

- `out/sdk-1/compatibility-focused.log`: new SQL fixture initially expected JDBC `id:int8`; actual driver metadata reports `id:bigserial`. Failed XML is preserved under `first-focused-failure/`. Fixture corrected to actual existing schema; no SQL changed.
- `compatibility-focused-2.log`: new metadata test initially had an ambiguous generic AssertJ overload at compilation. The test now explicitly uses JsonNode.
- `compatibility-focused-3.log` / `focused-3-failure/`: new metadata test compared a ShortNode with an IntNode despite equal numeric default -1. It now compares numeric value, while metadata type is independently frozen. No runtime/default change.
- `compatibility-focused-4.log` and `compatibility-focused-final.log`: subsequent focused compatibility/configuration/API checks passed. Public API model shape capture was an explicit one-time command and refuses overwriting an existing fixture; ordinary verification never captures fixtures.
- First external matrix `out/sdk-1/external/20261114142306/`: default, HTTP, combined capabilities, observability, RPC client/server and Operations built successfully; the new Messaging-only test timed out before reaching provider modes. Its log reports `auto.offset.reset=latest` and reset to **offset 1** after the one fixture event was written at offset 0. The fixture now explicitly selects **earliest** for the new group. Production Kafka defaults and SDK-0 provisioner are unchanged. Failed log/XML are retained. `messaging-isolated-corrected.log` and its report directory record the corrected fixture passing with real Outbox/Inbox/handler effects.
- `api-self-test.log`: a body-only change accepted; eight compiled API/contract mutations rejected.
- `negative-controls.log`: unchanged config/protocol/SQL fixture tree accepted; protobuf field-type mutation, applied SQL constraint mutation and configuration-default mutation rejected. Temporary mutated fixture trees/logs remain under `out/sdk-1/negative-controls-*`; repository production files were not mutated by these controls.
- The literal property audit initially flagged registry metadata/metric names as configuration keys; explicit non-property exclusions now distinguish them. Initial protocol-guard array construction was corrected before final verification. Draft/capture logs remain in `out/sdk-1/`.
- First full candidate sequence `out/sdk-1/final/repeated-20261114143242/`: all **three** attempts ran 289 tests with **one failure, zero errors/skips each**. The failure was the new `PublicContractShapeTest` assuming the API code source was a classes directory; Maven `verify` had already packaged the prerequisite into a JAR. The fixture now enumerates either classes or JAR entries. `jar-shape-corrected.log` records the corrected fixture passing under `verify`. These failed attempts remain retained and are not reported as final passes. Their post-build script guards were not run because Maven failed (the old ledger's compatibilityExit=0 is not evidence those guards ran; the wrapper now records -1 for not-run).
- Compiler processor paths were scoped to SDK POMs to preserve non-SDK Lombok processor discovery. `parent-processor-isolation.log`: focused root/domain compile exit 0, tests not run. This avoids imposing SDK metadata processors on business application compilation.
- The literal-default audit identified two internal Operations absent-value sentinels (`static`/empty) differing from catalogue `none`/`unknown`. These were aligned without changing supported providers or local HTTP allowed environments; no security or public signature change.

## Final source fingerprint and clean builds

**PASS: three consecutive clean SDK attempts at one unchanged source/configuration fingerprint.** Evidence: `out/sdk-1/final-corrected/repeated-20261114144800/`.

| Run | UTC start → finish | Maven exit | Compatibility exit | Tests | Failures / errors / skips |
| --- | --- | ---: | ---: | ---: | --- |
| 1 | 14:48:00 → 14:51:14 | 0 | 0 | 289 | 0 / 0 / 0 |
| 2 | 14:51:14 → 14:54:26 | 0 | 0 | 289 | 0 / 0 / 0 |
| 3 | 14:54:26 → 14:57:37 | 0 | 0 | 289 | 0 / 0 / 0 |

The wrapper exited **0**. The increase from checkpoint 273 to 289 comprises 16 new API/configuration/contract fixture tests. Each run includes all existing topic-provisioning and 1,000-iteration collector deadline cases; no iterations, runtime bounds or old failure evidence were removed. Messaging ran **152 tests** per run (the previous 150 plus two early-validation tests). Public API comparison, all eight negative API mutations, all 73 generated module/property catalogue entries and frozen protocol/SQL guards passed after each build.

The `-Sdk1` verifier captures root/SDK POMs, all SDK source/resources/tests, API/config/contract baselines and verification tools, standalone consumer sources/POM and owned registry source/POM. It checks unchanged hashes before each attempt and retains all logs/XML/diagnostics. Final recheck: **286 fingerprint entries, zero mismatches**, including after installation. Documentation is excluded from the executable source fingerprint so the report can be completed with actual results.

`ledger.json`, `configuration.json`, `source-fingerprint.json`, `run-N/build.log`, `api.log`, `api-negative-controls.log`, `configuration.log`, `protocol-sql.log`, all XML and supplemental diagnostic/cost outputs provide the exact ledger. In addition to the earlier incremental compile, `parent-processor-isolation-clean.log` records **37 non-SDK domain sources actually recompiled**, exit 0, with existing Lombok discovery; this is compile-only evidence.

```powershell
$env:JAVA_HOME='C:/Program Files/Java/jdk-21'
powershell -NoProfile -ExecutionPolicy Bypass -File tools/sdk-0/verify-sdk.ps1 -Sdk1 -EvidenceRoot out/sdk-1/final -Runs 3 -MavenCommand mvn.cmd
```

The Maven command selects all 13 SDK modules and prerequisites, `-o -B -ntp -fae -pl <SDK modules> -am clean verify`. Post-build gates run Java API comparison, compiled negative API controls, generated metadata comparison and frozen protocol/SQL checks. A Maven pass with a compatibility-gate failure is a failed attempt (`compatibilityExit`). API/role modules with no runtime suites are not interpreted as independent behavioral qualification. No tests are disabled or runtime bounds relaxed to obtain a pass.

## Installed consumer

The external fixture has its own Boot parent, imports the installed existing Apex parent POM only for version management, and uses project version `0.0.1-EXTERNAL` with SDK `1.0-SNAPSHOT`. Ten capability modes, runtime dependency trees, test reports and packaged boot checks are required. Combined and Messaging-only modes exercise real services inside JUnit; their packaged JARs are built but not separately launched. Other modes additionally execute the packaged JAR. The frozen checkpoint consumer class is executed without recompilation against current installed API.

The owned registry is rebuilt from unchanged `apex-discovery-server` source (`out/sdk-1/registry-package.log`) and copied/fingerprinted at `out/sdk-1/registry/apex-discovery-server.jar`. The previous prebuilt registry JAR is not reused as current source evidence.

```powershell
# After successful final verification, install the same SDK reactor locally with -DskipTests install.
powershell -NoProfile -ExecutionPolicy Bypass -File tools/m35-6/verify-external-consumer.ps1 -Offline -Sdk1 -MavenCommand mvn.cmd -RegistryJar '<repo>/out/sdk-1/registry/apex-discovery-server.jar'
```

**PASS: all ten modes, 23 JUnit tests, zero failures/errors/skips**, `out/sdk-1/external/20261114150034/`, completed UTC **15:03:02**. `final-install.log` records same-source local reactor installation exit **0**, tests skipped because the preceding clean verification is the test evidence.

| Mode | Tests | Build / selected behavior | Packaged JAR launch |
| --- | ---: | --- | --- |
| default | 4 | PASS; transport-free context and bean backoff | PASS |
| http | 4 | PASS; servlet boundary without other transports | PASS |
| capabilities | 7 | PASS; installed Messaging, real RPC/collector, owner SQL deadline and owned Eureka | Application contexts exercised inside JUnit |
| observability | 2 | PASS; optional diagnostics/export fixture | PASS |
| rpc-client | 1 | PASS; client role without server/DB/provider leakage | PASS |
| rpc-server | 1 | PASS; server role without client/DB/provider leakage | PASS |
| operations | 1 | PASS; optional capability/declaration backoff | PASS |
| messaging | 1 | PASS; real provisioned producer/consumer, Outbox/Inbox and one committed effect; no RPC/provider/MVC | Application context exercised inside JUnit |
| discovery-eureka | 1 | PASS; isolated provider installation and none/backoff | PASS |
| discovery-kubernetes | 1 | PASS; isolated provider installation and none/backoff | PASS |

The receipt's free-text JUnit boot label is shared by combined/Messaging modes; Messaging-only has **no gRPC runtime**, as its test and tree assert. Scope is the table above, not that generic label. Both combined/Messaging packaged JARs were built; neither was separately launched. Eight other packaged modes were launched. The combined fixture reads the real rebuilt owned registry without registering the consumer; provider-isolation modes deliberately select none and disable native Eureka to test backoff, not deployment behavior.

`default-tree.txt` proves `example.external:sdk-consumer:0.0.1-EXTERNAL` independently resolves SDK artifacts at `1.0-SNAPSHOT`. All runtime trees passed forbidden executable/business/role/provider checks; **zero metadata-processor runtime references** were found. The frozen checkpoint bytecode printed `FROZEN_CONSUMER_V1_ACCEPTED` without recompilation. `installed-artifacts.json` confirms all **13 SDK JAR SHA256s match final reactor artifacts**; `registry-artifact.json` and per-mode packaged JAR hashes are retained. Receipt, individual build/dependency/boot logs and copied XML are the authoritative evidence.

The wrapper preserves failed XML before throwing, restores sanitized environment variables, audits forbidden runtime dependencies and compares installed SDK JARs with the final reactor. This is local installed-artifact qualification, not remote publication or an empty-cache release test.

## Remaining risks and SDK-2 decision

**GO for SDK-2 on this tested local baseline.** The required executable API/configuration/contract guards, three unchanged-fingerprint zero-failure SDK builds, affected optional-capability combinations, dependency isolation and frozen binary consumer passed. Begin HTTP/async context work while retaining these gates and reviewing intentional API additions against the versioned baseline policy.

The checkpoint's unresolved historical native-timeout attribution, bounded cancellation backpressure and synchronous user-callback caveat remain recorded in [SDK-0/0.1](sdk-0-before-after.md). SDK-1 does not claim distributed crash/rebalance, security/SSO, tracing, live Kubernetes/token rotation, arbitrary mixed SDK versions, schema-owner migrations or immutable release compatibility. The strict snapshot guards deliberately require review for some compatible additions. Raw `mvn verify` does not by itself invoke the external script-level API/config/source checks; use the SDK-1 verifier as the qualification entrypoint. Non-SDK application integration is not accepted by the domain compile check or SDK suites.

Full product acceptance remains outside SDK-only evidence: M38's Order/Inventory/Payment and later Financial/packaged chain findings are not fixed or silently cleared. A GO for SDK-2, if recorded below, authorizes starting HTTP/async context work on this SDK baseline, not an enterprise/product release.

## Exact changed files

The following 69 source/baseline/document paths changed from checkpoint `8a2f822`. Generated logs, temporary consumers and mutation trees are ignored evidence artifacts.

```text
apex-sdk-api/src/test/java/com/apex/platform/core/context/ApiContractBaselineTest.java
apex-sdk-discovery/pom.xml
apex-sdk-discovery/src/main/java/com/apex/platform/discovery/DiscoveryPolicyAutoConfiguration.java
apex-sdk-discovery/src/main/resources/META-INF/additional-spring-configuration-metadata.json
apex-sdk-discovery/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
apex-sdk-discovery/src/test/java/com/apex/platform/discovery/DiscoveryConfigurationContractTest.java
apex-sdk-discovery-eureka/pom.xml
apex-sdk-discovery-eureka/src/main/resources/META-INF/additional-spring-configuration-metadata.json
apex-sdk-discovery-grpc/pom.xml
apex-sdk-discovery-grpc/src/main/resources/META-INF/additional-spring-configuration-metadata.json
apex-sdk-discovery-kubernetes/pom.xml
apex-sdk-discovery-kubernetes/src/main/resources/META-INF/additional-spring-configuration-metadata.json
apex-sdk-grpc/pom.xml
apex-sdk-grpc/src/main/resources/META-INF/additional-spring-configuration-metadata.json
apex-sdk-grpc/src/test/java/com/apex/platform/grpc/internal/GrpcConfigurationContractTest.java
apex-sdk-grpc-client/pom.xml
apex-sdk-grpc-server/pom.xml
apex-sdk-messaging/pom.xml
apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/PlatformCellMessagingAutoConfiguration.java
apex-sdk-messaging/src/main/resources/META-INF/additional-spring-configuration-metadata.json
apex-sdk-messaging/src/test/java/com/apex/platform/messaging/integration/MessagingConfigurationContractTest.java
apex-sdk-observability/pom.xml
apex-sdk-observability/src/main/resources/META-INF/additional-spring-configuration-metadata.json
apex-sdk-operations/pom.xml
apex-sdk-operations/src/main/java/com/apex/platform/contracts/ContractDiscoveryProvider.java
apex-sdk-operations/src/main/java/com/apex/platform/contracts/LocalContractRead.java
apex-sdk-operations/src/main/resources/META-INF/additional-spring-configuration-metadata.json
apex-sdk-operations/src/test/java/com/apex/compat/ConfigurationMetadataContractTest.java
apex-sdk-operations/src/test/java/com/apex/compat/PublicContractShapeTest.java
apex-sdk-operations/src/test/java/com/apex/compat/SdkContractCompatibilityTest.java
apex-sdk-operations/src/test/java/com/apex/compat/SqlMigrationCompatibilityTest.java
apex-sdk-operations/src/test/resources/sdk-1/v1/cell_gateway.proto
apex-sdk-operations/src/test/resources/sdk-1/v1/event-envelope.json
apex-sdk-operations/src/test/resources/sdk-1/v1/event-envelope-v2.json
apex-sdk-operations/src/test/resources/sdk-1/v1/operations-manifest-v1.json
apex-sdk-operations/src/test/resources/sdk-1/v1/operations-query.json
apex-sdk-operations/src/test/resources/sdk-1/v1/public-contract-shapes.txt
apex-sdk-operations/src/test/resources/sdk-1/v1/rpc-request.json
apex-sdk-operations/src/test/resources/sdk-1/v1/rpc-response.json
apex-sdk-operations/src/test/resources/sdk-1/v1/rpc-response-v2.json
apex-sdk-operations/src/test/resources/sdk-1/v1/sql/V1001__platform_outbox_event.sql
apex-sdk-operations/src/test/resources/sdk-1/v1/sql/V1002__platform_processed_event.sql
apex-sdk-spring-boot/pom.xml
apex-sdk-spring-boot/src/main/resources/META-INF/additional-spring-configuration-metadata.json
apex-sdk-webmvc/pom.xml
apex-sdk-webmvc/src/main/resources/META-INF/additional-spring-configuration-metadata.json
docs/sdk/sdk-1-api-baseline.md
docs/sdk/sdk-1-compatibility-matrix.md
docs/sdk/sdk-1-configuration-contract.md
docs/sdk/sdk-1-verification.md
pom.xml
tools/m35-6/external-consumer/pom.xml
tools/m35-6/external-consumer/src/discovery-test/java/example/DiscoveryIsolationTest.java
tools/m35-6/external-consumer/src/messaging-test/java/external/messaging/InstalledMessagingOnlyTest.java
tools/m35-6/verify-external-consumer.ps1
tools/sdk-0/verify-sdk.ps1
tools/sdk-1/baseline/api-inventory-v1.tsv
tools/sdk-1/baseline/checkpoint-api.txt
tools/sdk-1/baseline/checkpoint-inventory.tsv
tools/sdk-1/baseline/configuration-v1.json
tools/sdk-1/baseline/frozen-consumer-compiler-artifact.json
tools/sdk-1/baseline/frozen-consumer-v1.class.b64
tools/sdk-1/baseline/FrozenConsumerV1.java
tools/sdk-1/baseline/public-api-v1.txt
tools/sdk-1/SdkApiBaseline.java
tools/sdk-1/verify-configuration.ps1
tools/sdk-1/verify-contract-files.ps1
tools/sdk-1/verify-negative-controls.ps1
tools/sdk-1/WriteConfigurationMetadata.ps1
```
