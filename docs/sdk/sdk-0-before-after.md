# SDK-0: Kafka topic metadata readiness

Repository `Moh3n75/apex-financial-engine`, branch `credit-engine-event-sourcing`, source baseline `a9202ee69a368cb8b9d8691be20db5ac831a11d0`. Read all four M38 audit documents before implementation. No commits or pushes. This report concerns SDK Messaging qualification; full product acceptance remains blocked by the application findings documented in M38.

The **SDK-0.1 follow-up** at the end of this report records subsequent investigation and verification. The SDK-0 results below remain historical evidence and have not been replaced by later passes.

## Before: retained failures and demonstrated cause

Both original M38 failures remain untouched:

| Command / retained evidence | Actual result |
| --- | --- |
| `out/m38/sdk-clean-verify.log`, `out/m38/first-messaging/` | Exit 1; Messaging 126 tests, one failure in `provisionsLegacyOutgoingAndValidatedContractRoutesOnly` |
| `out/m38/sdk-clean-verify-fae.log`, `out/m38/second-messaging/` | Exit 1; 209 reached tests, one failure in `provisionsIncomingAndCustomDltBeforeUnchangedReliabilityPreflight`; Operations reactor project skipped |

Copies of both Messaging report sets are retained at `out/sdk-0/m38-first-messaging/` and `out/sdk-0/m38-second-messaging/`. `out/sdk-0/preserved-failures-sha256.json` fingerprints the original logs, failing suite XMLs and the new deterministic before test report. Clean builds never delete these directories. M37's separate retained provisioning failure is unchanged.

The old `ensure()` made one describe after creation acknowledgement. It immediately rejected an absent description, an unassigned leader or missing replicas. It also granted every describe/create a fresh full timeout, and used Admin's default close timeout. A successful create therefore did not establish the predicate required by the next line of SDK startup code.

`TopicMetadataRegressionTest` scripts two M38-style plans: outgoing legacy plus explicit contract route with a post-create leader ID of -1, and incoming source plus `.quarantine` with a post-create unknown-topic response. Both have valid, ready metadata on the next read. Against unchanged production source, the focused command failed **2 tests out of 2**, with no errors/skips: `out/sdk-0/before-deterministic.log` and `out/sdk-0/before-deterministic/`. The only subsequent fixture adaptation was to mock the explicit Admin options overloads used by the fix.

Debugger observation before implementation: `TopicMetadataRegressionTest` paused at original `ApexTopicProvisioner.ensure:107`, called by `afterSingletonsInstantiated:73` → regression fixture `reproduce:62` → `outgoingM38PlanWaitsForLeaderAfterSuccessfulCreate:20`. Local values: `name="legacy"`, requested partitions `1`, description `internal=false`, partition `0`, leader ID `-1`, replicas and ISR containing broker `0` at localhost:9092. Non-suspending probes could not be drained before the short test sessions disappeared; a suspending breakpoint was used to retain the state. The IDE session ended before subsequent export; the values and stack above transcribe the tool response, not a saved debugger trace. Agent breakpoints were cleaned up; user breakpoints were left alone.

This proves the implementation's readiness failure mechanism. The historical M38 exception discarded the raw metadata, so it does not prove which exact broker state occurred in each historical run. Kafka explicitly documents delayed visibility after successful creation, and leader election as a transient state: [Admin 4.2 API](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/admin/Admin.html), [LeaderNotAvailableException](https://kafka.apache.org/42/javadoc/org/apache/kafka/common/errors/LeaderNotAvailableException.html). Resolved test dependency: kafka-clients 4.2.1.

## After: implementation and invariants

One `System.nanoTime()` deadline begins before Admin construction. All topics, creation calls, metadata calls, future waits, polling delays and Admin cleanup share its remaining budget. Kafka request options receive remaining milliseconds; future waits receive remaining nanoseconds. Time arithmetic handles nanoTime wraparound. The timeout default remains 10 seconds; the existing 1ms–1m validation remains unchanged.

The provisioner polls every **at most 50ms**, clipped to the remaining budget. It waits for nonempty partition metadata, assigned leaders and nonempty replica assignments. It retries only `UnknownTopicOrPartitionException`, `LeaderNotAvailableException`, `NotLeaderOrFollowerException`, and incomplete readiness metadata. `TopicExistsException` is accepted only from creation as a concurrent creator outcome, followed by actual metadata validation. Kafka's own Admin runtime retains responsibility for its transport/controller retries. The SDK does not retry generic `RetriableException`, authentication/ACL/configuration errors or exhausted request/future timeouts.

Invalid topic plans fail validation before Admin creation. Impossible replication and ACL/configuration errors fail immediately. Internal topics and known undersized DLTs fail before waiting for leaders. Existing source partition counts are authoritative; new DLTs cover their sources. Existing topic partition counts, replication assignments and configs are never altered. No automatic partition expansion or fallback replication policy was added.

`SmartInitializingSingleton` remains the startup barrier: provisioning completes before Spring starts any `SmartLifecycle`, including reliability preflight at `Integer.MIN_VALUE` and Kafka listener containers. Failures still abort refresh. Tests assert both delayed success and failure prevent early lifecycle startup; the installed consumer also starts real listeners with provisioning enabled and broker auto-creation disabled. Applications manually starting containers before context refresh are outside this lifecycle guarantee.

Kafka exception messages and causes are discarded from SDK startup errors; only validated topic names, fixed compatibility text and exception class names remain. Interrupted waits restore the interrupt flag. Admin cleanup receives only the remaining budget, including zero on exhaustion.

No business Cells, public event contracts, authentication behavior, Financial application YAML, default listener policy or reliability preflight policy changed.

## Verification ledger

The first post-change focused run passed **37 tests, zero failures/errors/skips** (`out/sdk-0/focused.log`): 11 existing embedded Kafka provisioning tests, two existing safety tests, two M38 regression cases, and 22 deterministic deadline/safety invocations. Deterministic coverage includes delayed visibility/leader assignment, create races, both describe/create permanent failures, no broad timeout retry, metadata exhaustion, creation/future budget, multiple topics sharing a budget, internal topics, interrupt restoration and Spring lifecycle ordering. Existing broker tests retain partition/config preservation, source/DLT sizing and impossible replication coverage.

An initial full-build wrapper attempt is retained at `out/sdk-0/repeated-20261114121622/`. Windows PowerShell's `ErrorActionPreference=Stop` treated Mockito stderr warnings as terminating errors; the wrapper aborted without recording Maven's final exit. XMLs were copied before another clean. This attempt is **not counted as a successful clean build**. Diagnostic thread snapshots also remain under `out/sdk-0/`; the build was progressing while redirected output was buffered. The wrapper now captures the native exit code with stderr warnings permitted, then restores strict PowerShell error handling.

The first fully recorded sequence, `out/sdk-0/repeated-20261114122055/`, retained all three attempts at one source/configuration fingerprint:

| Run | Exit | Tests | Failures / errors / skips |
| --- | ---: | ---: | --- |
| 1 | 1 | 270 | 0 / 1 / 0 |
| 2 | 0 | 270 | 0 / 0 / 0 |
| 3 | 0 | 270 | 0 / 0 / 0 |

Run 1's error occurred **before the provisioner ran**: `reusesExistingSourceAndDltWithoutChangingMetadataOrConfigs:95 -> describe:61`, `UnknownTopicOrPartitionException`, during the immediate post-create before-snapshot in test setup. This independently exhibited the documented visibility behavior. The integration helper now waits up to 10 seconds for assigned metadata, polling every 50ms, ignoring only the three documented metadata errors; individual helper reads have a 1-second wait. Context startup failures are still asserted directly, outside the helper. No startup exception was suppressed. This necessary fixture correction changes the source fingerprint; final verification therefore starts a new three-run sequence.

The final source/configuration fingerprint and all three clean builds are retained at **`out/sdk-0/repeated-20261114123222/`**:

| Run | UTC start → finish | Exit | Tests | Failures / errors / skips | Result |
| --- | --- | ---: | ---: | --- | --- |
| 1 | 12:32:23 → 12:36:12 | 0 | 270 | 0 / 0 / 0 | PASS |
| 2 | 12:36:12 → 12:38:15 | 1 | 270 | 0 / 1 / 0 | gRPC deadline timeout |
| 3 | 12:38:15 → 12:42:59 | 1 | 270 | 0 / 2 / 0 | Operations deadline errors |

**Messaging passed all 150 tests in each final run**, including all provisioning cases. All 13 SDK modules and prerequisites were selected with identical arguments and source hashes; no exclusions or longer timeouts were introduced to obtain green results. `ledger.json`, `configuration.json`, `source-fingerprint.json`, each `run-N/build.log` and module XML reports provide the exact evidence. The repeated-build command exited **1**, correctly reflecting the unsuccessful overall gate.

Outstanding failures outside the changed Messaging runtime:

- Run 2: `CollectorDeadlineIntegrationTest.repeatedNativeDeadlinesNeverBlockTheSharedDeadlineScheduler:49`, `java.util.concurrent.TimeoutException` while observing a collection future with a 1-second wait and configured collection deadline 25ms. gRPC code/tests remain unchanged. Its existing supplemental `target/m32-deadline-threads.log` was removed by the next clean before it could be copied; the failure stack and suite output are retained in the XML/log. Root cause is not established by this work.
- Run 3: `OwnerRecentSearchTest.indexedAndNonindexedPlansCostsAndBloatAreRecordedBeforeAnyProjectionGate:190 -> measure:201`, and `OwnerSearchTest.oldCandidatesConsumeFinitePagesWithoutPretendingRecentAbsence:74`, both `GatewayException: DEADLINE_EXCEEDED`. Operations code/tests remain unchanged. These are actual qualification failures, not classified as environment-only defects.

Consequently **the zero-failure repeated SDK baseline is NOT established**. Messaging's readiness fix has repeated passing evidence; SDK-wide acceptance remains incomplete because of the retained gRPC/Operations failures. Later successes do not erase them. No full product acceptance is claimed.

Local artifact installation: `out/sdk-0/install.log`, exit **0**, using the same 13-module selection and prerequisites with `-o -B -ntp -DskipTests install`. This produces installed artifacts for the required consumer check; it is not an additional successful test run or release approval.

Installed consumer: **PASS, 2 tests, zero failures/errors/skips**, `out/sdk-0/installed-20261114124358/`. `verify-installed-messaging.ps1` copied the standalone consumer outside the repository, sanitized application environment variables, then ran offline `-Pcapabilities` with exactly these affected Messaging methods:

- `installedMessagingPublishesTransactionalOutboxToOwnedKafka`: broker auto-creation disabled, no precreated topic, opt-in SDK provisioning, real PostgreSQL migrations/transactional Outbox, Kafka delivery and envelope/identity assertions.
- `installedProvisioningReadiesIncomingAndCustomDltBeforeListenersStart`: broker auto-creation disabled, source and custom `.quarantine` with three partitions, reliability preflight active, actual Kafka listener containers running and all required partition leaders/replicas present.

Tests boot the standalone consumer's Spring application inside JUnit against installed JARs, not reactor source directories. They build/package the external JAR; they do not separately execute that packaged JAR. These two focused Messaging methods do not claim the full seven-mode installed capability matrix or live production ACL/multi-broker qualification. `receipt.json` records the outside repository fixture path and matching reactor/installed Messaging SHA256. `dependencies.txt`, `consumer-artifact.json`, `reactor-artifacts.json`, `installed-artifacts.json`, build/dependency logs and copied XML reports retain artifact and execution evidence.

## Reproduction

Use Java 21 and Maven 3.9.11 with the dependencies available locally. From the repository root:

```powershell
$env:JAVA_HOME='C:/Program Files/Java/jdk-21'
# Supply the absolute path to Maven 3.9.11 in place of mvn.cmd if needed.
powershell -NoProfile -ExecutionPolicy Bypass -File tools/sdk-0/verify-sdk.ps1 -MavenCommand mvn.cmd -Runs 3
# Install the same SDK reactor for local consumer verification with -o -B -pl <all 13 SDK modules> -am -DskipTests install.
# Retain and report any failed verification gate; installation is not release acceptance.
powershell -NoProfile -ExecutionPolicy Bypass -File tools/sdk-0/verify-installed-messaging.ps1 -MavenCommand mvn.cmd
```

The repeated-build script retains every attempt's log and XML, including failures, rejects changed source between runs, and records source SHA256s, arguments, Java/Maven paths and per-run test totals. It selects all 13 `apex-sdk-*` modules and prerequisites with `clean verify -fae`, not the full product reactor. ExecutionPolicy bypass is process-local; no machine policy is changed.

## Remaining risks and exact changed files

Local embedded Kafka is a single broker. Mocked ACL failures do not qualify a production secured/multi-broker cluster. Metadata can regress after startup through broker failures or administrative deletion. Admin's synchronous construction (including caller-provided plugins/DNS) and JVM/OS scheduling cannot be forcibly preempted by the provisioning deadline; all SDK-owned waits and Kafka request deadlines are bounded, and elapsed construction time consumes the budget. This work does not qualify Kubernetes, tracing, security, Outbox crashes, application profile failures, or full product acceptance. Installed checks use the local Maven repository and same-version snapshots, not an immutable remote release or an empty cache.

Exact implementation/tool/report changes:

1. `apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/ApexTopicProvisioner.java`
2. `apex-sdk-messaging/src/main/java/com/apex/platform/messaging/jdbc/TopicProvisioningProperties.java`
3. `apex-sdk-messaging/src/test/java/com/apex/platform/messaging/integration/TopicProvisioningSafetyTest.java`
4. `apex-sdk-messaging/src/test/java/com/apex/platform/messaging/integration/TopicMetadataRegressionTest.java` (new)
5. `apex-sdk-messaging/src/test/java/com/apex/platform/messaging/jdbc/TopicProvisioningDeadlineTest.java` (new)
6. `tools/m35-6/external-consumer/src/capability-test/java/external/capabilities/InstalledCapabilitiesTest.java`
7. `tools/sdk-0/verify-sdk.ps1` (new)
8. `tools/sdk-0/verify-installed-messaging.ps1` (new)
9. `docs/sdk/sdk-0-before-after.md` (new)
10. `apex-sdk-messaging/src/test/java/com/apex/platform/messaging/integration/TopicProvisioningIntegrationTest.java`

The four M38 audit documents were already untracked at session start and remain unchanged. Generated evidence under ignored `out/sdk-0/` and external temporary fixture directories is additional local output, not committed content.

# SDK-0.1 follow-up: SDK verification stability

Continues the uncommitted SDK-0 implementation at the same Git HEAD. All inherited Messaging source hashes match SDK-0's final fingerprint. No Kafka code/tests, business Cells, event contracts, authentication behavior or SDK module boundaries changed in SDK-0.1.

## Before corrections: investigation and preserved evidence

Each originally failing test was executed individually before implementation corrections:

| Test | Result | Evidence under `out/sdk-01/` |
| --- | --- | --- |
| `CollectorDeadlineIntegrationTest` (all 1,000 iterations) | PASS | `before-grpc-individual.log`, `before-grpc-individual/` |
| `OwnerRecentSearchTest#indexedAndNonindexedPlansCostsAndBloatAreRecordedBeforeAnyProjectionGate` | PASS | `before-OwnerRecentSearchTest.log`, matching report/measurement directory |
| `OwnerSearchTest#oldCandidatesConsumeFinitePagesWithoutPretendingRecentAbsence` | PASS | `before-OwnerSearchTest.log`, matching report directory |

Two additional clean full SDK builds before runtime/test-fixture corrections used the same fingerprint: `out/sdk-01/before/repeated-20261114130814/`. Run 1: exit **1**, 270 tests, one failure, zero errors/skips. Run 2: exit **0**, 270 tests, zero failures/errors/skips. The failure was an independently discovered random cursor-tampering fixture error, documented below. None of the three original timing failures recurred in these two builds. Their SDK-0 XMLs/logs remain retained separately; an isolated pass is not treated as proof those failures never happened.

Six further individually executed native gRPC stress attempts, with diagnostic heartbeat/timing collection added but unchanged production code, also passed all **1,000 iterations each**. Evidence: `out/sdk-01/grpc-diagnostic-1/` through `grpc-diagnostic-6/`, each containing the Maven log, XML, exit and `sdk01-collector-timings.csv`. No iterations were removed and the observation timeout remained one second. Diagnostics add a 10ms heartbeat on the collector's own clock and capture clock age, queue depth, active workers, per-iteration latency and all-thread state on timeout. Their overhead is part of final verification as well.

The verifier now copies module-root diagnostic `.log`, `.csv`, `.txt`, `.jfr` and Surefire dump files **before the next clean**. This corrects SDK-0's loss of the gRPC supplemental dump. Failed-run logs and XML remain preserved as before. `out/sdk-01/inherited-sdk0.patch` records inherited tracked changes; SDK-0's original failure reports and hashes are untouched.

## gRPC finding and correction

Review found that `Job.publish()` publishes terminal slots, then cancels outstanding child futures on its calling thread. Expiry runs on the dedicated, single-thread `grpcCollectorDeadlines` executor. Child cancellation may execute native or custom cancellation callbacks synchronously. A blocking cancellation therefore pins the only clock even after the first collection's result is complete; subsequent collection futures can remain unresolved. Job monitors were already released before publication/cancellation, and collection/query-local clocks were already separate. Increasing worker counts, changing native deadlines, increasing the observation timeout or suppressing the test would not address this coupling.

`MultiResponderGatewayTest.blockedCancellationDoesNotStrandTheNextDeadline` reproduces it deterministically: a mock scheduler exposes deadline tasks; a single test clock runs them; the first child cancellation enters a latch; a second expiry is queued behind it. Against unchanged production code the test failed with **TimeoutException** while the first collection was complete and the second was incomplete. Evidence: `out/sdk-01/before-blocked-cancellation.log` and `before-blocked-cancellation/`, including `sdk01-blocked-cancellation-threads.log`. The thread dump places `test-collection-clock` waiting in `CountDownLatch.await` → child `cancel` → `DefaultMultiResponderGateway.Job.lambda$publish`. Debugger state captured after timeout confirmed `entered=0`, `release=1`, first completed normally, second not completed (`grpc-debugger-evidence.json`). This is a scheduler-blocking/incomplete-future defect, not a mere observation-duration assumption.

**Historical limitation:** the SDK-0 native timeout's supplemental thread dump was lost in the next clean. This session's seven isolated native executions and two pre-correction full builds did not reproduce that exact native timeout. The deterministic failure demonstrates a real matching mechanism; it does not prove that the historical native timeout was necessarily caused by that mechanism. General OS scheduling, native cancellation internals and synchronous user completion callbacks are not universally qualified by these tests.

Correction: move cancellation/cleanup onto `grpcCollectorCancellations`, a dedicated four-thread executor with a queue sized to the configured maximum collections. One cleanup is submitted per terminal collection, and its collection/call admission permits remain held until cleanup finishes. Thus cleanup backlog is bounded by admitted work; slow cleanup causes truthful admission exhaustion rather than delaying the shared clock. Terminal result publication and partial/strict/cancelled snapshots remain unchanged. No native retry or mutation resend was added. The executor uses daemon threads and Spring's explicit `shutdown` lifecycle, like the existing worker/clock beans; rejected work during shutdown is cleaned up on the calling thread. The normal admitted-work bound prevents capacity rejection while active.

The deterministic regression now passes while first cancellation is still blocked. Existing cancellation tests retain their assertions, with completion barriers/awaits for child cleanup because it now intentionally occurs after result publication on another executor. A first focused attempt exposed one stale immediate child-cancel assertion; its failure XML/log is retained (`after-focused.log`, `after-focused-failure/`). The corrected focused run and subsequent unit run passed (`after-focused-2.log`, `after-collector-unit.log`).

## Operations findings and correction

Both SDK-0 Operations errors originated at `BoundedRecentSearch.read:81` / `BoundedOwnerSearch.read:81`: the explicit total-read guard. They did **not** originate at the SQLException/SQLSTATE cancellation mapping. Source SQL uses bounded candidate LIMITs, so the observed path indicates the real 750ms overall budget was exhausted. Production correctly returned `DEADLINE_EXCEEDED`; it must continue doing so.

The fixture's injected monotonic supplier advances two seconds per admission to permit repeated functional/cost reads without one-second rate-limit waits. It controls **rate admission only**. The total-read budget independently uses `System.nanoTime()`, and PostgreSQL enforces its own statement and lock limits. The synthetic supplier therefore does not exempt a real slow JDBC connection or SQL operation from its deadline. No clock or deadline implementation change was justified.

The two fixtures used `PGSimpleDataSource` for every read, repeatedly creating PostgreSQL backends and negotiating the connection. A live all-thread dump caught the performance test inside `ConnectionFactoryImpl.enableSSL` → connection establishment → `BoundedRecentSearch.read:63` → `OwnerRecentSearchTest.measure` (`before-full-live-26020.log`). Debugger logpoints before correction observed the actual datasource class `PGSimpleDataSource`, candidate count 16 and real elapsed values **189,660,601ns** and **33,579,699ns**, while synthetic admission readings were 2s and 4s (`ops-debugger-events.json`). This identifies distinct clocks and non-query setup cost; it does not attribute every historical pause to SSL or claim the OS was the sole cause.

Correction: a suite owns a bounded Hikari pool (maximum four connections, minimum two), explicitly establishes two connections before timed reads, and closes the pool before its own embedded PostgreSQL. This measures indexed read work on warm connections and supports the existing concurrent connection tests. Restricted-role and disposable restart fixtures keep their explicitly selected datasources. No shared database, global pool, sleep/retry of a failed production read, or shared fixture across classes was added.

The retained isolated pre-correction measurement had medians **21.97–23.36ms**, p95 **28.36–137.42ms**, p99 **140.42–152.51ms**. The focused pooled run had medians **0.474–0.864ms**, p95 **0.676–1.503ms**, p99 **0.859–1.830ms** (`before-OwnerRecentSearchTest/ops03d-native-cost.csv`, `after-focused-measurements/ops03d-native-cost.csv`). These are measurements of different fixture connection lifecycles, not a claim of production performance improvement.

All original indexed/nonindexed controls, retained row counts (10k/100k/250k), 20 warmups, 100 samples, candidate/page bounds, index definition/readiness checks, EXPLAIN index/no-sequential-scan assertions, and **p95 <150ms / p99 <250ms** gates remain. The production **250ms statement**, **100ms lock**, **750ms overall read**, and rate limits are unchanged. Existing SQL `pg_sleep(2)` cancellation tests remain active. New tests in both search suites intentionally delay connection acquisition by 800ms and assert the production reader still returns `DEADLINE_EXCEEDED`. The delay is a negative test of the unchanged limit, not an increased observation timeout.

Additional fixture failure: `cursorRejectsTamperingFiltersLimitsOwnerScopeExpiryAndNewBoot:61` replaced token character 10 with `A`, which can already be `A`; the token was then unmodified and correctly accepted. The fixture now chooses `A` or `B` to guarantee a different character and explicitly asserts the token changed before testing rejection. No cursor implementation or validation was weakened. Evidence: pre-correction full run 1 XML/log.

## Final repeated verification, installed checks and decision

**Three consecutive clean full SDK attempts passed at one source/configuration fingerprint.** Evidence: `out/sdk-01/after/repeated-20261114133020/`.

| Run | UTC start → finish | Exit | Tests | Failures / errors / skips |
| --- | --- | ---: | ---: | --- |
| 1 | 13:30:21 → 13:33:45 | 0 | 273 | 0 / 0 / 0 |
| 2 | 13:33:45 → 13:36:53 | 0 | 273 | 0 / 0 / 0 |
| 3 | 13:36:53 → 13:40:25 | 0 | 273 | 0 / 0 / 0 |

The verification command exited **0**. The increase from 270 to 273 is one deterministic collector regression and two slow-acquisition budget checks. Messaging remains **150/150 in every run**. Its 71 fingerprinted files match SDK-0 exactly. Each collector integration run retains all 1,000 iterations; run 1 measured maximum observed completion 93,860µs and clock age 17,698µs. These are observed local timings, not guaranteed worst-case execution bounds.

`ledger.json`, `configuration.json`, `source-fingerprint.json`, every Maven log, all module XMLs and supplementary per-module diagnostic/cost files are retained. Unlike SDK-0 (PASS, one gRPC error, two Operations errors across its three final runs), this final sequence has zero test failures/errors/skips in all three attempts. Earlier SDK-0 and SDK-0.1 failures remain separately retained and are not included in successful-run totals.

Local installation of all 13 SDK modules and prerequisites passed with `-DskipTests install` (`out/sdk-01/install.log`); this is artifact preparation, not an extra successful test run.

Installed consumer: **PASS, 4 tests, zero failures/errors/skips**, `out/sdk-01/installed-20261114134141/`, completed UTC **13:42:32**. The standalone fixture ran outside the repository against installed JARs. The selected methods verify:

- The two SDK-0 Messaging checks: real transactional Outbox delivery to a provisioned topic, and source/custom-DLT readiness with actual listener startup.
- `installedCollectorExpiresRepeatedNativeCalls`: two loopback gRPC application contexts, 100 successive slow native reads, 25ms collection deadlines, unchanged one-second observations, all slots `TIMED_OUT`.
- `installedOperationsReadsAndEnforcesSqlDeadline`: real PostgreSQL schema/index, successful public owner/recent reads using a pool, and a `pg_sleep(2)` source producing the expected `DEADLINE_EXCEEDED` through the production SQL limit.

Build, dependency audit, copied XML, source fixture path, external packaged JAR hash and installed/reactor artifact manifests are retained. The script compares Messaging, gRPC and Operations installed JAR SHA256s with the verified reactor. All match. Tests boot their application contexts inside JUnit; the resulting packaged JAR was not separately launched. This is focused installed-capability evidence, not the complete seven-mode matrix or a remote immutable-release consumer.

Final validation checked **234 fingerprinted source/configuration files: zero mismatches**, and all **71 Messaging files against SDK-0: zero mismatches**. `git diff --check` passed. All SDK-0 failure hashes remain valid. Git HEAD and branch remain the supplied baseline/branch; everything is uncommitted.

**Readiness decision: GO to begin SDK-1 on this local SDK verification baseline.** The requested three consecutive zero-failure clean SDK attempts and affected installed-consumer checks passed. Keep the explicitly unresolved historical native-timeout attribution and production/distributed qualification gaps tracked; neither this decision nor SDK-only tests grant full product, enterprise or release acceptance. M38's application regressions and later capability/release gates remain separate work.

The final fingerprint includes all SDK source/POMs, the root POM, verification scripts and external consumer source/POM. It is checked before every clean build. No SDK-0 Messaging source or provisioning test was changed. Java 21, Maven 3.9.11, offline dependencies and all 13 SDK modules plus prerequisites remain the verification environment.

Reproduction commands (supply the local Maven 3.9.11 absolute path as needed):

```powershell
$env:JAVA_HOME='C:/Program Files/Java/jdk-21'
powershell -NoProfile -ExecutionPolicy Bypass -File tools/sdk-0/verify-sdk.ps1 -EvidenceRoot out/sdk-01/after -Runs 3 -MavenCommand mvn.cmd
# Install the same SDK reactor locally after verification; this is not publication.
# mvn.cmd -o -B -ntp -pl <all 13 SDK modules> -am -DskipTests install
powershell -NoProfile -ExecutionPolicy Bypass -File tools/sdk-0/verify-installed-messaging.ps1 -Sdk01 -MavenCommand mvn.cmd
```

Exact SDK-0.1 incremental changed files (the SDK-0 list above remains historical):

1. `apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/DefaultMultiResponderGateway.java`
2. `apex-sdk-grpc/src/main/java/com/apex/platform/grpc/internal/GrpcCollectorAutoConfiguration.java`
3. `apex-sdk-grpc/src/test/java/com/apex/platform/grpc/internal/CollectorDeadlineIntegrationTest.java`
4. `apex-sdk-grpc/src/test/java/com/apex/platform/grpc/internal/MultiResponderGatewayTest.java`
5. `apex-sdk-operations/src/test/java/com/apex/operations/owner/OwnerRecentSearchTest.java`
6. `apex-sdk-operations/src/test/java/com/apex/operations/owner/OwnerSearchTest.java`
7. `apex-sdk-operations/src/test/java/com/apex/operations/owner/OperationsTestDatabase.java` (new)
8. `tools/sdk-0/verify-sdk.ps1`
9. `tools/sdk-0/verify-installed-messaging.ps1`
10. `tools/m35-6/external-consumer/pom.xml`
11. `tools/m35-6/external-consumer/src/capability-test/java/external/capabilities/InstalledCapabilitiesTest.java`
12. `docs/sdk/sdk-0-before-after.md`

Remaining qualification gaps: single-host embedded PostgreSQL/Kafka and loopback gRPC are not distributed production qualification. Uncooperative cancellation can retain the bounded admission capacity; user-supplied synchronous completion callbacks can still block their notifying thread. The exact historical native timeout remains unproven despite the deterministic defect correction. The performance measurements are fixture-specific, and original M38 application/security/tracing/discovery/release gaps remain. No full product acceptance, commit or push is implied.
