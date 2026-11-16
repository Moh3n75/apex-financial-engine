# AOR-01 verification — AOR-01R closure, 2026-11-16

## Decision

**GO for closing the staged AOR-01 scope.** Three consecutive full clean SDK-0/SDK-1 runs passed against one unchanged executable-source fingerprint, followed by successful installed-consumer verification. No production or test code change was justified by the investigation. Existing deadlines, assertions, expected status codes and delivery semantics remain unchanged; no skips, hidden reruns or status remapping were introduced.

This closes the regression-gate blockers. The documented staged artifact boundary remains: the driver-free core composition still transitively contains the legacy JAR's dormant PostgreSQL classes/resources. Physical relocation remains a separately reviewed compatibility task. No Oracle implementation or qualification is included, and SNAPSHOT coordinates remain mutable development artifacts.

## Investigation and causal findings

All three previous complete Maven logs and the relevant JUnit XML/stdout/stderr were analyzed. Their hashes/line counts and case durations are indexed in `out/aor-01r/retained-complete-log-index.json`; full extracted failing-case output and analysis are retained alongside it. The original FAIL/PASS/FAIL logs remain untouched under `out/aor-01/final-corrected/repeated-20261116125416/`. Original AOR-01 reports are preserved under `out/aor-01r/initial-reports/`.

| Failure | Determination | Direct evidence |
|---|---|---|
| ReliableConsumerIntegrationTest.observedRetryCommitDuplicateAndQuarantineRemainDistinct | Execution/scheduling stall affecting the test process and embedded broker; no reproducible AOR-01 storage or delivery regression | Failed case took 88.846s, versus 3.220s/3.205s in the other retained runs. Hikari reported an 88.286s scheduling interval; Kafka controller events took about 52s, the broker was fenced, consumer heartbeats expired, and source offset remained 2 while the test waited for 3. Case log gaps were 30.915s and 47.620s. Independent broker/timer activity was delayed along with the business path. |
| DynamicCellChannelsTest.wrongServerNameWrongPeerAndUntrustedClientRemainRejected | Pre-existing sensitivity of a bounded cold TLS call to execution/handshake delay; the AOR-01 messaging change is excluded from this runtime path | Failed rogue-client assertion received DEADLINE_EXCEEDED after a 28.732s case, with a 26.352s interval between client startup and server shutdown; passing retained cases took 4.116s/4.104s. The entire normalized 104-entry discovery test classpath and 172 production class files are identical to unchanged HEAD, including every third-party JAR hash. No messaging dependency is in that graph. |

The messaging causal chain is stalled scheduling → broker/controller and heartbeat delays → unavailable quarantine progress → no source-offset commit → the existing 20s offset assertion expires. The offset was **not** advanced unsafely. Retained Hikari/Kafka observations establish a broad runtime delay; they do not identify whether the original underlying cause was host descheduling, VM suspension, GC or another resource stall. No historical JFR/thread dump exists to distinguish those mechanisms. The retained dumpstreams are manifest/classpath notices, not fault thread dumps.

For gRPC, QueryOptions.defaults remains 5s and RpcErrors.await retains its 5s+250ms local wait. Native deadline expiry and a late TLS rejection can race under execution delay. RpcErrors preserves the native status; converting DEADLINE_EXCEEDED to UNAVAILABLE would change the contract without evidence of a mapper defect. A non-suspending debugger probe observed the healthy rogue request returning native `UNAVAILABLE: ssl exception` about 131ms after the request marker, mapped unchanged to GatewayException.UNAVAILABLE at the exact assertion/caller (DynamicCellChannelsTest:132/174). The denied peer returned native PERMISSION_DENIED. No local await-timeout probe was hit before that assertion. A subsequent pause was at the completed-call assertion, after the RPC result, to inspect the value/stack; it was diagnostic only. Observations: `debugger-observations.json`. Agent breakpoints were removed, all 13 original user breakpoints preserved, and no debugger ran during qualification.

The exact producer of the *historical* gRPC timeout (native deadline versus local await timeout) and the precise cause of its delay cannot be recovered from its sanitized exception/log. Classification as existing deadline sensitivity, rather than an AOR-01 functional regression, rests on identical runtime binaries/dependencies, unchanged tests/defaults, baseline comparisons and successful subsequent qualification. It is not a claim that the original failure was reproduced on demand or that arbitrary host stalls are now handled within those deadlines.

Additional exclusion evidence: ReliableKafkaConsumerRuntime, ConfigurableKafkaEventConsumer, ConfigurableOutboxPublisher and SimpleTransactionalEventHandlerExecutor class files are byte-identical to HEAD. The PostgreSQL driver's getDatabaseProductName bytecode returns the constant PostgreSQL; the added guard does not add a metadata SQL round trip. Production SQL, transaction boundaries, retries, claims and fences were not altered in AOR-01R.

## Unchanged baseline comparison

Tracked HEAD `02f591f0e9cb3e1a9a663cc79b0c569f36f3a465` was extracted with git archive into `out/aor-01r/unchanged-head/`; the current dirty worktree was never reset or overwritten. No baseline artifacts were installed. Original/current source comparison normalizes Git archive LF versus Windows checkout CRLF; raw and normalized hashes are both retained. Actual gRPC production class files were also compared byte for byte.

Two baseline and two current runs alternated, each executing the **complete** ReliableConsumerIntegrationTest and DynamicCellChannelsTest classes. All four runs passed 21 tests, zero failures/errors/skips: **84 executions**. This tests 17 reliability scenarios and four real mTLS/discovery scenarios per run, rather than replacing an integration case with a mock. The failures did not reproduce in this comparison.

Commands, complete Maven logs, copied JUnit XML, GC/safepoint logs, eight JFR recordings and host samples are in `comparison-1-baseline` through `comparison-4-current`. Diagnostic JVM flags were applied equally to both variants; they changed no deadlines or assertions. The largest recorded GC collection/pause was 0.031159s. Those observations describe the comparison runs and cannot rule out GC during the old unrecorded failures.

The exact comparison command is recorded per run in command.json and used these arguments:

```powershell
$env:JAVA_HOME='C:/Program Files/Java/jdk-21'
$M='C:/Users/dev3.FARDUP/.m2/wrapper/dists/apache-maven-3.9.9-bin/4nf9hui3q3djbarqar9g711ggc/apache-maven-3.9.9/bin/mvn.cmd'
& $M -o -B -ntp -pl apex-sdk-messaging,apex-sdk-discovery-grpc -am test '-Dtest=ReliableConsumerIntegrationTest,DynamicCellChannelsTest' '-Dsurefire.failIfNoSpecifiedTests=false' '-DargLine=-Xlog:gc*,safepoint:file=target/aor01r-gc-%p.log:time,uptime,level,tags -XX:StartFlightRecording=filename=target/aor01r.jfr,settings=profile,dumponexit=true'
```

`surefire.failIfNoSpecifiedTests=false` permits prerequisite modules that do not contain those two named classes; both selected classes actually ran, and no selected test was skipped. No diagnostic argLine or debugger was used in the full clean gate below.

## Three consecutive clean SDK gates

Environment: Windows 10 amd64, JDK 21, Maven 3.9.9, Boot 4.1.1; disposable PostgreSQL 14.15 and Kafka 4.2.1. All Maven resolution was offline. No production/organization database connection was made.

Executed command:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/sdk-0/verify-sdk.ps1 -Sdk1 -Runs 3 -EvidenceRoot out/aor-01r/final -MavenCommand $M
```

The existing verifier executes `-o -B -ntp -fae -pl <all 14 SDK modules> -am clean verify` and, after each successful build, the exact v1 API guard, its eight compiled negative controls, configuration guard and frozen SQL/protobuf guard. There were no changes to that verifier, the fixtures or module selection. Exact native arguments, environment and source hashes are retained in configuration.json/source-fingerprint.json.

Evidence root: `out/aor-01r/final/repeated-20261116134355`. Wrapper exit **0**. Every recorded source hash was rechecked after qualification and installed consumers.

| Run | UTC start → completion | Tests | Failures / errors / skips | Maven exit | Compatibility exit |
|---|---|---:|---|---:|---:|
| 1 | 2026-11-16T13:43:56.2553243+00:00 → 2026-11-16T13:47:17.4353565+00:00 | 307 | 0 / 0 / 0 | 0 | 0 |
| 2 | 2026-11-16T13:47:17.6213659+00:00 → 2026-11-16T13:50:40.2402438+00:00 | 307 | 0 / 0 / 0 | 0 | 0 |
| 3 | 2026-11-16T13:50:40.4212426+00:00 → 2026-11-16T13:54:02.9147140+00:00 | 307 | 0 / 0 / 0 | 0 | 0 |

Each run includes 164 messaging tests, six neutral-composition tests and all remaining SDK suites. The two investigated tests passed in all three full runs. All 73 v1 configuration entries remain exact, with the separately checked AOR-01 addition giving 74 current entries. The public API/artifact v1 baseline, event/protobuf protocols and raw V1001/V1002 migration bytes remain unchanged. Positive guards and all required compiled negative controls ran after **each** clean build, rather than inferring compatibility from compilation.

An external one-second sampler observed a maximum sampling gap of 1.0185s, maximum system CPU 49.92% and minimum available memory 10.53 GiB during the complete sequence. These are observation data, not a resource reservation or a reproduction of the old stall. No host power, priority, heap, timeout or GC policy was changed. Full samples are `qualification/host.csv`.

## Installed consumers and controls after the clean gate

All SDK artifacts were installed locally with `-DskipTests install` only **after** the successful clean gate; installation is not counted as test execution. Every installed SDK JAR matches its corresponding reactor JAR by SHA256 (14 artifacts), recorded in installed-artifact-hashes.json.

| Executed stage | Result | Retained result/evidence |
|---|---|---|
| tools/sdk-1/verify-negative-controls.ps1 | PASS: unchanged fixtures accepted; intentional protobuf, applied SQL and configuration mutations rejected | out/aor-01r/negative-controls/maven.log and the fixture path it records |
| tools/aor-01/verify-neutral-consumer.ps1 -MavenCommand $M | PASS, 7 tests, 0 failures/errors/skips; packaged launch; unsupported provider and missing-owner negative launches | out/aor-01/neutral-20261116135426/ |
| tools/sdk-0/verify-installed-messaging.ps1 -MavenCommand $M | PASS, 2 real PostgreSQL/Kafka durable/provisioning tests | out/sdk-0/installed-20261116135439/ |
| Owned discovery registry clean package | PASS; compile/package only with tests explicitly skipped, source unchanged | out/aor-01r/registry/; fresh JAR out/aor-01r/apex-discovery-server.jar |
| tools/m35-6/verify-external-consumer.ps1 -Offline -Sdk1 -RegistryJar out/aor-01r/apex-discovery-server.jar -MavenCommand $M | PASS, 10 modes, 23 tests, 0 failures/errors/skips, frozen compiled v1 consumer accepted without recompilation | out/sdk-1/external/20261116135517/ |

Exact commands (including the full 14-module install selection and executable paths) and environment are in each `out/aor-01r/<stage>/command.json`; installed-ledger.json records all exits as zero. Every PowerShell script was launched with `-NoProfile -ExecutionPolicy Bypass -File`. The existing ten-mode matrix also runs eight packaged smoke processes; combined/messaging modes launch real Boot contexts inside JUnit and build their JARs. Neutral/legacy fixtures live outside the repository and consume installed artifacts, not reactor source. No previous consumer result was reused as a new execution.

## Changes, retained history and limits

AOR-01R changes only **docs/sdk/aor-01-verification.md** relative to the starting AOR-01 worktree. The other 25 previously changed/new files retain their starting hashes. Diagnostics, unchanged HEAD extraction, failed/successful reports and receipts are under ignored out/aor-01r/. The earlier AOR-01 source changes remain uncommitted and intact. HEAD and branch remain unchanged; no Fardup, legacy, Identity or Gateway source was edited. No commit/push occurred.

The earlier three runs remain **FAIL / PASS / FAIL**, each with 307 tests: run 1 had one reliability error; run 2 passed; run 3 had one discovery status assertion failure. Their evidence and previous assessment were preserved, not relabeled or overwritten. The new PASS / PASS / PASS sequence is separate evidence with its own dates, command/configuration and source fingerprint. The investigated methods' source/defaults and all original assertions are unchanged.

No code fix was supported by the observations: the recorded scheduling stall cannot safely be corrected by changing persistence behavior, and the TLS runtime is byte-identical to baseline with correct observed status mapping. The evidence supports closing the AOR-01 regression gate on the tested staged scope; it does not prove a particular historical OS/JVM stall mechanism, eliminate all environmental timing sensitivity, qualify a remote immutable release, complete physical JAR relocation or provide Oracle support.
