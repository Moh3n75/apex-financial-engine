# M30: isolated Financial build and bounded Saga measurements

Run from the repository root with Java 21, Maven 3.9.x and a populated local Maven
cache. Native PostgreSQL 14.15 binaries come from the existing Zonky test dependency;
Kafka/KRaft comes from spring-kafka-test. Docker and external services are unnecessary.
Allow localhost child processes and ensure adequate temporary disk/RAM. Tests own every
database, topic and consumer group. Do not point these fixtures at deployment services.

```powershell
$env:JAVA_HOME = '<your Java 21 JDK>'
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File tools/m30/verify-m30.ps1 -Offline -MavenCommand mvn.cmd
```

The runner builds/tests Platform+Starters, generates and compiles Financial in an
owned native PostgreSQL fixture, runs focused Financial tests, then runs unchanged
M22/M26/M27/M29 regression and the new packaged three-Cell load fixture. No tests are
skipped. Repeated suites count once. Financial generation is deliberately offline;
provision Maven artifacts/native binaries in the local cache first. Without the cache,
offline runs fail rather than contact any application database. Omit -Offline for the
ordinary dependency-resolution steps only if downloading dependencies is authorized.

Independent phases:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/m30/verify-m30.ps1 -Phase Financial -Offline -MavenCommand mvn.cmd
# Performance requires current verified Cell JARs/build receipts from the full M29 runner.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/m30/verify-m30.ps1 -Phase Performance -Offline -MavenCommand mvn.cmd
```

## Financial schema provenance

The authoritative generation input is **only**
`apex-bootstrap/src/main/resources/db/migration/V1..V16`, applied in version order by
Flyway to a fresh private PostgreSQL server. Infrastructure's older V1 copy alone is
insufficient. No Cell Platform V1001/V1002 migrations are mixed into Financial schema.
No applied migration is edited and no outOfOrder is used. The test records each input
SHA-256, schema version and generated file count in out/m30-financial-schema-evidence.json.

FinancialBuildIT supplies only its allocated local JDBC URL to jOOQ 3.21.7, then runs:

```text
mvn -B -ntp -o -pl apex-bootstrap -am clean test-compile
mvn -B -ntp -o -pl apex-bootstrap -am test -Dtest=CreditAccountTest,CreditReservationTest,FinancialTransactionTest,ThreadLocalExecutionContextProviderTest -Dsurefire.failIfNoSpecifiedTests=false
```

The child commands also receive `-Djooq.codegen.skip=false` and fixture-owned
`apex.jooq.url/user/password` arguments; these are never real/development credentials.
Ordinary Infrastructure builds now skip generation by default, preventing implicit
database connections. A fresh Financial compile must use the fixture runner: skipping
code generation does not supply/register missing generated types. Do not disable the
skip flag against an arbitrary external JDBC URL to obtain a green build.
The Financial execution is explicitly invoked by its execution ID and is not bound
to a Cell lifecycle phase: its nested reactor clean must precede refreshed artifact
receipts, not invalidate them in the middle of a Cell verify. The All runner enforces
that order before the existing regression workflow and performance execution.

The resolved plugin's actual Mojo registers the generated source root after successful
generation. Its unsupported addCompileSourceRoot option has been removed. Generated
output stays under ignored target/generated-sources/jooq; never commit it or fake types.
This compiles all Financial tests, but runs only the four named pure Domain/Context
classes. Financial database/Kafka integration tests are **NOT VERIFIED** by this step.

## Measurement contract

SagaPerformanceIT reuses the packaged test launcher, validates current artifact
receipts, starts three java -jar children (64 MiB initial / 256 MiB max heap), three
independent native PostgreSQL servers and one disposable KRaft broker. Six sources and
six matching DLTs are explicitly provisioned, one partition each, auto-creation off.
All three measurement databases explicitly enable fsync, synchronous_commit and
full_page_writes; the fixture asserts the effective settings. This overrides native
test defaults only for these disposable servers, never global PostgreSQL settings.
Reliable consumer/Inbox/Outbox behavior is unchanged. Payment uses explicit per-order
seeded decisions in its own test DB, inserted immediately after HTTP acceptance;
no default approval or test HTTP backdoor exists. Seeder overhead is outside measured
HTTP latency and can influence downstream retry/latency.

Default phases: 2s warmup, 4s smoke; 30s moderate (at most 12 RPS, chosen from observed
smoke acceptance), 30s higher hotspot (at most 30 RPS), 4s spike (at most 60 RPS),
6s post-spike recovery; 3s Payment-down backlog and 3s Inventory-consumer-down backlog.
Restart retains the same DB/group and offsets. The latter is a consumer-process fault,
not broker failover. No unknown outcome is turned into a business decline.

Each phase is an open-loop paced offered rate with bounded in-flight requests and no
unbounded queue. Non-admitted arrivals, non-201s and exceptions are counted separately.
Drain polls for at most 60 seconds for terminal outcomes, then up to 30 seconds for
Outbox/offset convergence. Pending/timeout counts are reported, never discarded.
HTTP latency runs from send to response; Saga latency starts at accepted response
and ends at the first independent owning-DB terminal observation. It includes polling
resolution (50ms plus query time), and is not a precision distributed trace.
Warmup is reported separately. Percentiles use nearest rank; all errors remain counts.

Override bounded rate/duration/concurrency/SKU count directly using Maven:

```powershell
mvn.cmd -B -ntp -o -f inventory-cell/pom.xml -Pm30 test-compile surefire:test@saga-performance `
  -Dm30.higher-hotspot.rps=20 -Dm30.higher-hotspot.seconds=15 `
  -Dm30.higher-hotspot.concurrency=8 -Dm30.higher-hotspot.skus=1
```

Every phase is capped at 1,000 offered requests; RPS 1..100, duration 1..30 seconds,
concurrency 1..16, SKUs 1..64. All SKUs start with 10,000 units; ordinary requests
reserve one unit, every tenth candidate declines, every twentieth requests 100,000
units intentionally and is rejected without Payment. Mixed phases use separate SKUs;
the hotspot concentrates a phase onto one SKU. Defaults are workstation observations,
not supported production TPS. Do not run parallel heavy jobs and compare their results
as if operating conditions were equal.

Evidence: inventory-cell/target/m30-performance-evidence.json and partial phase output;
JUnit target/performance-reports; out/m30-financial-* and full runner logs. CPU time
is the child-process observed cumulative delta during each phase (startup included in
restart phases); host CPU is a sampled gauge. Final jstat heap/GC snapshots are not
continuous heap profiles or GC-pause percentiles. Independent pg_stat_database counts
connections; pool utilization/wait is NOT VERIFIED without pool instrumentation.

The fixture verifies owning-DB conservation, trusted authorization, decline + release
ACK ordering, no negative stock, business keys and duplicate deliveries. Admin clients
capture per-partition committed/end offsets and DLT counts at cutoff/drain. Nonzero DLT
or dead Outbox fails validation; no record is silently treated as successful recovery.
No DLT listener/replay/reset exists in this fixture. Approved operator recovery remains
covered by unchanged M29 regression. All child/server handles close on success/failure;
logs, generated sources and reports are ignored outputs. No shared resources are mutated.
