# APEX M30 — Financial build reproducibility and three-Cell measurements

Date: 2026-10-27. Branch: credit-engine-event-sourcing.
Baseline HEAD: a263d436e5e556848802c51f1443e2b8fd1fedfb.

**PASS for the requested isolated build, regression and measured-load gates.**
Financial test-compile is reproducible without a shared database. Final measured run:
1554 offered arrivals, 1551 accepted and completed Sagas, three bounded-generator
non-admissions, no HTTP failures or residual backlog. **291 distinct tests passed;
zero failures, errors or skips among executed tests.** Financial service integration
and production readiness remain explicitly NOT VERIFIED.

## Acceptance gates

| Gate | Status | Executed evidence |
|---|---|---|
| Safe reproducible Financial test-compile | PASS | Fresh native PostgreSQL, authoritative V1..V16, 29 real jOOQ Java files, full reactor Financial test-compile exit 0 |
| Focused Financial tests | PASS | 7 Domain + 3 ExecutionContext tests; no external DB/Kafka |
| Financial DB/Kafka integration | NOT VERIFIED | Existing integration suites require additional supported infrastructure; not implied by compilation |
| Previous M29 and earlier regression | PASS | Full unchanged runner exit 0 at 16:44:33 +03:30; 279 distinct tests, zero failures/errors/skips |
| Real smoke/moderate/higher/spike + drain | PASS | Actual three packaged JVMs; final 30s moderate/high, short spike/recovery, all accepted Sagas finish; three non-admitted arrivals reported |
| Owning DB / offset / idempotency invariants | PASS | Stock conservation, trusted outcomes, all-event correlation/local Inbox causation, explicit duplicates, final group commits and zero DLT/dead/backlog |
| Reusable commands, cleanup, preservation | PASS | Named fixture commands and performance wrapper executed; owned processes/listeners closed; original changes/local YAML preserved |
| Real broker process outage / failover | NOT VERIFIED | Controlled Inventory consumer and Payment process outages executed instead; existing reliable-DLT regression retained |
| Pool utilization/wait, continuous heap/GC pauses | NOT VERIFIED | DB connections, process CPU and final jstat snapshots measured; no fabricated pool/pause metrics |

## Change scope and reproducibility

At entry Git contained 89 modified/untracked paths; staged binary diff was empty.
HEAD, index and SHA-256 of every existing dirty/untracked file were captured. No
reset, clean of Git, staging, commit, push, offset reset or shared-service operation
was performed. Maven clean applies only to build target output.

| Exact repository-relative file | Purpose |
|---|---|
| apex-infrastructure/pom.xml | Remove implicit database credentials/endpoint and unsupported addCompileSourceRoot; explicit codegen skip and fixture-supplied JDBC parameters |
| inventory-cell/pom.xml | Opt-in m30 test executions with separate Financial-build/performance JUnit report directories |
| inventory-cell/src/test/java/com/apex/inventory/FinancialBuildIT.java | Owned PostgreSQL/Flyway schema provenance, real jOOQ generation, bounded reactor test-compile and focused tests |
| inventory-cell/src/test/java/com/apex/inventory/SagaPerformanceIT.java | Existing packaged-launcher based bounded measurement, restart and SQL/Kafka invariant fixture |
| tools/m30/verify-m30.ps1 | Java 21 checks; Financial / full regression / performance orchestration |
| tools/m30/README.md | Safe commands, prerequisites, load contract and metric limitations |
| APEX_M30_FINANCIAL_BUILD_PERFORMANCE_REPORT.md | This acceptance evidence |

No domain/application logic, event contract, routing algorithm, operational endpoint,
base/local YAML, migration, replay guarantee, Outbox/Inbox schema or financial business
source was changed. The Financial fixture lives alongside the existing native test
tools in Inventory's test sources; it adds no runtime dependency between Cells and
Financial. All previous tests remain present with their assertions.

## Financial root cause and verified solution

The original jOOQ plugin connected implicitly to a local database. Its configuration
also contained addCompileSourceRoot, which is not an actual option of the locally
resolved jooq-codegen-maven 3.21.7 Mojo. javap inspection confirms that the Mojo itself
adds the generated source root after successful generation, but returns immediately
when skipped. Disabling generation therefore neither creates missing types nor
registers cached output in a fresh compiler execution.

Infrastructure's V1-only SQL does not describe the current Financial schema. The
authoritative input used is apex-bootstrap/src/main/resources/db/migration, all sixteen
applied in Flyway version order to a fresh owned PostgreSQL instance. Schema version
16 supplies the twelve explicitly included Financial tables, including leases, failure
audit and Saga context/claims. Cell Platform V1001/V1002 are intentionally excluded:
their UUID Outbox is a different adapter/schema from Financial's BIGINT aggregate ID.
No migration history/checksum was edited and no synthetic schema or stub type was used.

The fixture generated 29 Java sources under ignored
apex-infrastructure/target/generated-sources/jooq, compiled all Financial production
and test sources through apex-bootstrap, then ran four named pure test classes.
Schema input SHA-256 values and version/count are recorded in
out/m30-financial-schema-evidence.json. V1 SHA-256:
05714477e61f1b47e0d1a3a4fd2d8164f2b6026bc9c59d000b8f02ac3ff7e36c;
V16 SHA-256: 7ef7251c6be5feb442e8492b1f038ded4ae8f52c866382506adef536ca56231c.

Code generation is now skipped by default with no embedded endpoint/credentials.
A fresh Financial compile uses the runner, which explicitly enables generation and
supplies only its own allocated PostgreSQL connection. Skipping generation alone is
still not a replacement for generation. No generated source is checked into Git.
Ordinary fresh aggregate builds must use this documented prerequisite rather than
an implicit database connection. Financial runtime/database compatibility remains
NOT VERIFIED beyond compilation and the ten focused tests.

## Executed commands and results

Environment: Java 21, Maven 3.9.16, Boot/Spring Kafka 4.1.1, jOOQ codegen 3.21.7,
native PostgreSQL 14.15, Flyway 12.4.0, disposable Kafka/KRaft 4.2.1. Commands ran
from repository root; mvn.cmd denotes the locally cached Maven launcher.

```powershell
mvn.cmd -B -ntp -o -f inventory-cell/pom.xml -Pm30 test-compile surefire:test@financial-isolated-build -Dm30.maven=<cached-mvn.cmd>
powershell.exe -NoProfile -ExecutionPolicy Bypass -File inventory-cell/tools/verify-operational-recovery.ps1 -Offline -MavenCommand <cached-mvn.cmd>
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/m30/verify-m30.ps1 -Phase Performance -Offline -MavenCommand <cached-mvn.cmd>
```

Financial fixture exit 0. Its actual nested commands (fixture-owned JDBC properties
omitted here, not inherited development values):

```text
mvn -B -ntp -o -Djooq.codegen.skip=false -Dapex.jooq.url=<owned ephemeral URL> -Dapex.jooq.user=<fixture user> -Dapex.jooq.password=<fixture value> -pl apex-bootstrap -am clean test-compile
mvn -B -ntp -o -Djooq.codegen.skip=false -Dapex.jooq.url=<owned ephemeral URL> -Dapex.jooq.user=<fixture user> -Dapex.jooq.password=<fixture value> -pl apex-bootstrap -am test -Dtest=CreditAccountTest,CreditReservationTest,FinancialTransactionTest,ThreadLocalExecutionContextProviderTest -Dsurefire.failIfNoSpecifiedTests=false
```

Both exited 0. The selected-test option avoids accidentally running existing external
Financial integration classes; all of those classes were nevertheless test-compiled.
It is not a skip flag for the existing isolated regression, which runs in full.
Generation runs before compilation and again during the focused Maven lifecycle.
Each nested build has a five-minute timeout and forcibly terminates its owned child
on failure; the native database closes in try-with-resources.

Logs: out/m30-financial-fixture.log, out/m30-financial-compile.log,
out/m30-financial-focused.log, out/m30-full-regression.log and out/m30-performance.log.
Final distinct JUnit totals: **291 passed, 0 failed, 0 errors, 0 skipped**.

| Suite group | Distinct passed |
|---|---:|
| Context / WebMVC / Messaging / WebMVC Starter / Messaging Starter | 2 / 5 / 98 / 4 / 9 |
| Customer / Reporting / Order | 30 / 20 / 38 |
| Inventory local | 23 |
| Existing packaged Saga + bidirectional replay / M27-M28 / M29 / M30 load | 2 / 1 / 1 / 1 |
| Payment | 6 |
| Replay tests / packaged CLI | 39 / 1 |
| Financial Domain / Bootstrap Context / isolated build fixture | 7 / 3 / 1 |
| **Total** | **291** |

The historical 279 were all rerun. M30 adds two acceptance methods plus ten actually
executed Financial pure tests to this accounting. Repeated suite/load executions are
counted once. JUnit evidence is in each module's target/surefire-reports; Inventory's
saga-e2e-reports, three-cell-reports, operational-recovery-reports and performance-reports;
Replay's cli-it-reports; and out/m30-financial-fixture-reports (archived before the
existing runner's Inventory clean). External Financial integration classes were
compiled, not selected for execution; zero skips refers to executed report sets.

Financial generation/test-compile
completed at 16:25:46 +03:30; the fixture plus ten focused tests completed successfully
at 16:25:53. No shared database was contacted. Bootstrap's pre-existing explicit
Flyway starter 4.2.0-M2 dependency was not changed; compile success does not qualify
that Financial runtime dependency combination.

## Performance conditions and observations

The harness runs actual packaged Order, Inventory and Payment java -jar processes
with three independent native PostgreSQL servers and a disposable KRaft broker.
No replay audit DB is needed for the load fixture; separate M29 regression exercises
its own fourth audit DB and packaged CLI. All source and -dlt topics are explicitly
provisioned; broker auto-creation is false. Existing topic/group semantics are preserved
with unique fixture groups. No Cell database is shared or queried by production peers.

The HTTP generator is paced open-loop with bounded in-flight requests and no growing
queue. It records not-admitted arrivals separately, and distinguishes HTTP 201 from
terminal business completion. HTTP latency ends at response; completion latency starts
there and ends at a separate owning-Order DB observation, polling every 50ms plus query
time. Nearest-rank percentiles include no guessed completion for pending orders.
Drain is bounded (60s terminal observation plus up to 30s Outbox/offset convergence).
Successful HTTP and fixture-seeding failure counts remain separate from business states.

Payment is an explicit per-order seeded simulation only; the fixture writes its policy
after receiving the generated orderId, outside HTTP timing. A missing policy fails
closed/retries; there is no implicit approval. One unit is requested ordinarily;
each tenth candidate is declined, each twentieth requests 100,000 units for intentional
stock rejection. All SKUs begin with 10,000 units. Separate phase stocks isolate totals;
hotspot phases use one SKU. One Kafka partition/listener per relevant stream and
100ms Outbox scheduler cadence are retained. A hotspot with one consumer is not proof
of parallel stock-update scalability; existing concurrency regression remains relevant.

**Final run:** 1554 offered arrivals, **1551 HTTP 201 / completed Sagas**:
1398 CONFIRMED, 78 compensated CANCELLED and
75 out-of-stock CANCELLED. Three higher-load arrivals were not
admitted by the bounded generator (not HTTP failures); none were silently retried.
There were **0 non-201s, 0 HTTP/fixture exceptions, 0 terminal timeouts/incomplete,
0 DLT records, 0 dead Outbox**, and no pending orders/Outbox/lag after drain.
Warmup is included in these exact totals and shown separately below.

All latency columns are milliseconds, p50 / p95 / p99. Rate is target / observed
accepted RPS; duration excludes the bounded drain.

| Scenario | Seconds / VUs / SKUs | Target / actual RPS | Offered / accepted / not admitted | HTTP ms | Saga ms | Final confirmed / compensated / stock-rejected | Incomplete |
|---|---|---|---|---|---|---|---|
| warmup | 2 / 2 / 4 | 2 / 2.00 | 4 / 4 / 0 | 5.34 / 115.19 / 115.19 | 315.66 / 681.96 / 681.96 | 4 / 0 / 0 | 0 |
| smoke | 4 / 2 / 8 | 5 / 5.00 | 20 / 20 / 0 | 4.69 / 5.91 / 16.23 | 377.98 / 552.21 / 552.68 | 18 / 1 / 1 | 0 |
| moderate | 30 / 4 / 16 | 10 / 10.00 | 300 / 300 / 0 | 3.45 / 5.14 / 7.80 | 464.86 / 657.86 / 745.39 | 270 / 15 / 15 | 0 |
| higher-hotspot | 30 / 8 / 1 | 30 / 29.84 | 900 / 897 / 3 | 2.21 / 3.40 / 8.93 | 2655.39 / 3855.27 / 5073.07 | 808 / 45 / 44 | 0 |
| spike | 4 / 12 / 1 | 60 / 60.00 | 240 / 240 / 0 | 1.99 / 2.49 / 4.01 | 4185.35 / 5136.95 / 6040.51 | 216 / 12 / 12 | 0 |
| post-spike-recovery | 6 / 4 / 8 | 10 / 10.00 | 60 / 60 / 0 | 1.91 / 2.47 / 2.93 | 441.32 / 606.51 / 778.65 | 54 / 3 / 3 | 0 |
| payment-stop-backlog | 3 / 3 / 1 | 5 / 5.00 | 15 / 15 / 0 | 1.96 / 2.94 / 2.94 | 4949.16 / 6203.41 / 6203.41 | 14 / 1 / 0 | 0 |
| inventory-consumer-stop-backlog | 3 / 3 / 1 | 5 / 5.00 | 15 / 15 / 0 | 2.29 / 5.49 / 5.49 | 6375.25 / 7463.21 / 7463.21 | 14 / 1 / 0 | 0 |

Cutoff is the end of submission, not a claim of completed processing. State columns
below are CANCELLING / PENDING_PAYMENT / PENDING_INVENTORY / other pending.
Snapshot queries are independent observations, not one atomic cross-system snapshot.

| Scenario | Pending states at cutoff | Lag cutoff → drain | Pending Outbox cutoff → drain | Dead / DLT after drain |
|---|---|---|---|---|
| warmup | 0 / 1 / 0 / 0 | 0 → 0 | 0 → 0 | 0 / 0 |
| smoke | 0 / 1 / 1 / 0 | 0 → 0 | 0 → 0 | 0 / 0 |
| moderate | 0 / 3 / 2 / 0 | 0 → 0 | 5 → 0 | 0 / 0 |
| higher-hotspot | 3 / 45 / 50 / 0 | 0 → 0 | 98 → 0 | 0 / 0 |
| spike | 2 / 58 / 106 / 0 | 1 → 0 | 162 → 0 | 0 / 0 |
| post-spike-recovery | 0 / 1 / 3 / 0 | 1 → 0 | 4 → 0 | 0 / 0 |
| payment-stop-backlog | 0 / 14 / 1 / 0 | 14 → 0 | 0 → 0 | 0 / 0 |
| inventory-consumer-stop-backlog | 0 / 0 / 15 / 0 | 15 → 0 | 0 → 0 | 0 / 0 |

Three production-like PostgreSQL durability flags (fsync, synchronous_commit,
full_page_writes) were asserted ON for every measurement DB. max_connections=300;
shared_buffers=16384 8KiB pages (128MiB). The initial exploratory run used the library's
relaxed durability defaults; it passed logical checks but is **not** the final baseline.
Its artifact is preserved as out/m30-performance-relaxed-durability-evidence.json.
After correcting the fixture, a durable run passed, then the final run additionally
verified correlation and local Inbox causation for **every** emitted Outbox event.
Only final-run numbers above count; repeated executions do not multiply tests/orders.

The final run completed at 16:58:04 +03:30, packaged acceptance duration 141.0s
(Maven 2m25s). There were no failing test cases or weakened assertions. Environment
correction and stronger final metadata verification account for the repeated runs.

Actual final child JVMs: Order **23716**, Inventory **21000 → 5184**, Payment
**32380 → 19768**. Exactly two controlled restarts occurred, preserving each DB/group;
no unplanned Cell restart was observed. Native PostgreSQL ports were Order **7833**,
Inventory **7839**, Payment **7845**; all three were distinct and owner-table exclusion
assertions passed. The test harness is a separate JVM, not a substituted Cell process.
At final cleanup inspection none of these five child PIDs or three DB listeners remained.

Host: Windows (JDK reports Windows 10), 20 logical CPUs, 34,088,599,552 physical RAM bytes
observed separately via CIM; harness max heap 8,522,825,728 bytes. Each Cell uses
-Xms64m -Xmx256m. Other workstation applications were not controlled, and there is
no claim of a dedicated performance host.

| Scenario | Observed Cell CPU seconds | Host CPU sampled at phase end |
|---|---:|---:|
| warmup | 1.16 | Initial gauge returned 0; not used as utilization evidence |
| smoke | 1.36 | 14.83% |
| moderate | 7.75 | 15.12% |
| higher-hotspot | 14.70 | 22.71% |
| spike | 2.02 | 24.61% |
| post-spike-recovery | 0.34 | 12.63% |
| payment-stop-backlog | 10.22 | 19.05% |
| inventory-consumer-stop-backlog | 15.59 | 22.37% |

Cell CPU values sum observed process CPU deltas over submission/recovery/drain;
restart phases include the new JVM's startup cost. They are not host CPU percentages
or peak CPU profiles. Final jstat snapshots on the surviving processes:

| Cell / PID | Used heap, approximately MiB | Young GC count / seconds | Full GC | Total GC seconds |
|---|---:|---|---:|---:|
| Order / 23716 | 53.96 | 28 / 0.081 | 0 | 0.101 |
| Inventory / 5184 | 27.90 | 17 / 0.046 | 0 | 0.053 |
| Payment / 19768 | 29.74 | 13 / 0.051 | 0 | 0.057 |

Heap is the sum of reported survivor/eden/old used KiB. Restarted JVM counters reset;
these are final snapshots, not run-wide pause distributions/heap peaks. Each running
Cell DB showed 11 connections at sampled boundaries, including harness observation
connections; a stopped Cell showed 1. This is observed connection
count, not measured pool utilization/wait, which remains **NOT VERIFIED**.

Independent Admin evidence after final fixture duplicate deliveries (partition 0):

| Group / topic | Broker end | Committed next offset | Lag |
|---|---:|---:|---:|
| order-m30-b2a55442-bece-4d7c-9041-0ccdb98653e3 / apex.inventory.events.v1 | 1631 | 1631 | 0 |
| order-m30-b2a55442-bece-4d7c-9041-0ccdb98653e3 / apex.payment.events.v1 | 1476 | 1476 | 0 |
| payment-m30-9e9afae4-08bc-4fcc-a213-2deea0e451f5 / apex.order.payment-requests.v1 | 1478 | 1478 | 0 |
| inventory-m30-4ec1f0d4-ddf2-4aca-aa91-8d2441162355 / apex.order.events.v1 | 1553 | 1553 | 0 |
| inventory-m30-4ec1f0d4-ddf2-4aca-aa91-8d2441162355 / apex.order.inventory-release-requests.v1 | 80 | 80 | 0 |

Full per-group/topic/partition cutoff/drain evidence is retained in
inventory-cell/target/m30-performance-evidence.json. Record counts include explicit
same-event/new-event fixture duplicates and liveness-safe repeated acknowledgements;
they are not interchangeable with unique Order counts. The fixture never resets an
existing group or operator-republishes to source topics.

Current packaged artifact SHA-256:

| JAR under module target | SHA-256 |
|---|---|
| order-cell.jar | 4AAE951D9D236B34EF018A08E9389AECFC29A3B75EF33DCC750D73A87A4D02D9 |
| inventory-cell.jar | AB87A4349FBB71EE6DB4CE3B5F8718D114C0C4DF592B9781E915B6149C2CFDD8 |
| payment-cell.jar | 3A5573C927E79DB3E97134824BEFE0C45CBA4541DAB8D9624BD18EE4975B091D |
| apex-replay-tool.jar | ACE110C7F46904AA68A0E14DC1C8C1C3DF53B400ECAF25C1519E96428AB6C8D6 |

## Correctness, limitations and next milestone

| Invariant | Result / concrete evidence |
|---|---|
| No CONFIRMED without trusted authorization | PASS: every owning Order row checked against Payment AUTHORIZED and Inventory RESERVED; local payment_decision also AUTHORIZED |
| Unknown/outage is not decline | PASS: Payment-down cutoff had 14 PENDING_PAYMENT + 1 PENDING_INVENTORY, zero cancellation; restart then 14 CONFIRMED + one explicitly seeded DECLINED compensation |
| Cancellation requires a terminal fact | PASS: stock rejection requires Inventory REJECTED and no Payment record; compensated cancellation requires Payment DECLINED, Inventory RELEASED and Order release_acknowledged=true |
| Stock conservation / no oversell / duplicate release | PASS after every phase and final same/new-event duplicates: for each SKU reserved equals stored quantities of RESERVED reservations; available=10000-reserved; neither negative |
| Business idempotency | PASS: six explicit fixture duplicate source sends, unique Payment decisions and one Payment outcome per order; no duplicate payment/cancellation command Outbox; conditional terminal states preserved |
| Metadata / Inbox linkage under load | PASS: every Outbox correlation equals its Order's original ORDER_PLACED correlation, every non-root Outbox causation points to a committed input in that same owner's Inbox; three actual original wire envelopes also compared |
| No ACK of an uncommitted business transaction | PASS in retained real M27/M29 deferred-COMMIT fault/offset tests; final load Admin commits converge only with valid owner states; no replacement ACK logic |
| Durable DLT/replay guarantees intact | PASS in rerun M26.8/M26.9/M29 real Kafka tests, including failed DLT sends, unknown contracts, crash-after-ACK seam, UNKNOWN resend refusal and approved recovery |
| Failed/pending is not successful recovery | PASS: performance asserts zero dead/DLT and reports all pending cutoffs; M29 separately observes actual quarantine and verifies owner business recovery after explicit CLI approval |
| No auto-replay/source operator republish/offset reset | PASS: existing targeted CLI checks retained; load fixture only makes explicit transport duplicate sends to its owned broker, no replay tool bypass or reset |

The metadata/atomicity evidence is executable at SagaPerformanceIT.java:321; load
and metric boundaries at :154 and :254; generation at FinancialBuildIT.java:20.
These paths are under inventory-cell/src/test/java/com/apex/inventory. Production
Cells still own their databases; cross-owner reads/seeding above occur only inside
the explicitly isolated harness. No platform behavior was changed to pass these checks.

No real PSP/price/currency/capture/refund/ledger operation is implemented. These are
bounded single-host plaintext single-broker observations, not production capacity or
readiness. TLS/SASL/ACL enforcement, real broker outage/failover, RBAC, production
migration rollout, long-duration soak/GC pause distributions and realistic financial
integration are NOT VERIFIED. Hikari utilization/wait lacks exposed instrumentation;
pg_stat_database connections are a different metric and are not presented as pool wait.
Final jstat heap/GC snapshots do not measure every transient maximum or pause percentile.

Measured limiting observations, ranked without claiming an untested cause or capacity:

1. **Order publication backlog dominates the sampled stress backlog.** At higher
   load, Order had 91 of 98 pending Outbox rows at cutoff; at spike, 150 of 162.
   HTTP p99 stayed 8.93ms / 4.01ms while Saga p99 reached 5073.07ms / 6040.51ms.
   ConfigurableOutboxPublisher.java:62 claims batches of 100 and serially joins each
   broker send; the fixture uses a 100ms fixed-delay schedule. These are plausible
   contributors, not a proven per-stage timing decomposition. No delivery algorithm
   or scheduler value was tuned during this milestone.
2. **The bounded generator hit its own admission limit three times** in the 900-arrival
   higher phase (8 VUs). Observed accepted rate was 29.84 RPS, not a guaranteed 30;
   the cause of those momentary admission limits is not separately profiled.
3. **Process restart adds visible recovery delay**, with p99 6.20s Payment / 7.46s
   Inventory consumer. Both recover the same DB/group and fully drain. Sampled CPU
   stayed roughly 13–25% after warmup and final full-GC counts were zero; these
   samples do not prove CPU/GC can never become limiting. Row-lock wait attribution
   and an actual saturation throughput ceiling are NOT VERIFIED. Rate, VUs and SKU
   concentration changed together, so their effects are not causally isolated.

Recommended next single vertical milestone: instrument Order Outbox claim-to-broker-ACK
and consume-to-commit latency using the existing event IDs/observability, then make
one evidence-driven publisher/scheduling adjustment and rerun this exact durable
three-Cell + replay/rollback matrix against an agreed latency SLO. Keep contracts,
Inbox and delivery guarantees unchanged; do not guess a timeout compensation policy.

## Git preservation and handoff

Final HEAD remains a263d436e5e556848802c51f1443e2b8fd1fedfb; index is still empty.
Expanded status now has 95 paths versus 89 at entry. Of the 89 original dirty/untracked
files, **88 remain byte-identical by SHA-256**; only inventory-cell/pom.xml has the
intentional additional opt-in executions. The other six newly dirty/new paths are
Infrastructure POM plus the five new files listed above. All local YAMLs, previous
reports, Platform/Starter and Cell business sources retain their original bytes.
No stage/commit/push was performed. Generated sources/JARs/logs/metrics remain in
ignored target/out; no .gitignore edit or machine-specific source setting was added.
git diff --check and no-index checks on all five new files passed.

Review the existing milestone changes separately. Suggested M30-only grouping:
`build(financial): generate jOOQ from isolated versioned schema` and
`test(saga): baseline packaged load and bounded recovery`. Infrastructure POM was
clean at entry; Inventory POM is mixed with earlier work and needs selective hunk review.
These commands are suggestions only, not executed:

```powershell
git add -- apex-infrastructure/pom.xml inventory-cell/src/test/java/com/apex/inventory/FinancialBuildIT.java
git add -p -- inventory-cell/pom.xml
git add -- inventory-cell/src/test/java/com/apex/inventory/SagaPerformanceIT.java tools/m30/verify-m30.ps1 tools/m30/README.md APEX_M30_FINANCIAL_BUILD_PERFORMANCE_REPORT.md
```

For independent reproduction use tools/m30/README.md and the All runner. The executed
acceptance was the equivalent ordered Financial fixture → complete existing M29
runner → performance phase commands recorded above. It did not contact shared
Financial/Kafka resources or start any real payment operation.
