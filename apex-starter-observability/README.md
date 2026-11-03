# Optional APEX operational observations

```xml
<dependency>
  <groupId>com.apex</groupId>
  <artifactId>apex-starter-observability</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

This optional library adds bounded, metadata-only local diagnostics. It is absent
from the default `apex-spring-boot-starter` graph. It starts no web endpoint,
Collector, Kafka consumer, database, discovery client or authorization service.
Do not add an Infrastructure/Business Cell executable as an SDK dependency.

Explicit activation (all three switches default false):

```properties
apex.observability.enabled=true
apex.observability.active-enabled=true
apex.observability.export-enabled=false
```

`OperationalObserver.tryRecord(new Sample(Operation.QUERY, Outcome.OBSERVED, nanos))`
accepts only enums and a nonnegative duration. False means lost diagnostic coverage,
never business failure. No arbitrary tags, identifiers, exception, request or payload
fields exist. Existing Kafka/gRPC runtime hooks use native Micrometer Observation;
they have no dependency on this starter. Its handler ignores unsupported names and
does not copy arbitrary Observation attributes. Existing native HTTP/JVM/Kafka/pool
instrumentation remains owned by Spring/Micrometer; this starter adds no second
HTTP timer or wire span. OPS-02 does not qualify end-to-end W3C/OTLP tracing.

`ActiveOperations.snapshot()` returns at most the configured number of local entries,
attributed to instance ID, random boot epoch and local attempt ID. These diagnostic
IDs are not metric labels. Elapsed time is monotonic within the process. TTL expiry
means visibility was lost; it does not mean the operation completed. Close handles
in finally. There is no public endpoint or multi-replica aggregation.

| Property suffix under `apex.observability` | Default | Meaning |
|---|---:|---|
| `max-records` | 1024 | Drop new records when queue is full |
| `max-bytes` | 1048576 | Metadata reservation budget, also bounded by record count |
| `max-record-bytes` | 2048 | Reject an admission exceeding the reservation limit |
| `max-active` | 256 | Process-local active slots |
| `active-ttl` | 30s | Lazy expiry during registry access/admission |
| `batch-size` | 64 | Maximum records removed per export batch |
| `export-interval` | 100ms | Isolated pump cadence |
| `export-timeout` | 1s | Maximum pump wait; timeout opens export circuit |
| `shutdown-timeout` | 1s | Global bounded worker-stop wait; remaining diagnostics dropped |
| `instance-id` | local | Deployment-supplied safe instance label; boot epoch disambiguates restarts |

Each fixed-shape sample reserves 256 bytes for admission accounting. This is a
conservative policy unit, not a JVM heap profiler or a serialized OTLP byte count.
Queue plus one in-flight bounded batch and registry/executor overhead must be
included when sizing a process. Tests measure allocation separately. Bounds are
experimental resource policy, not an approved throughput/latency SLA.

With `export-enabled=true`, supply an optional `ObservationBatchExporter` bean.
It runs only on the isolated daemon sender, never the transaction/listener/deadline
thread. Without a bean, startup succeeds and no export worker starts. The SPI is
not an OTLP protocol implementation. Adapters must enforce their own I/O timeout,
honor interruption and avoid payload logging. Exceptions drop the batch; there is
no retry spool. A timeout opens the circuit until restart, cancels the send and
does not create replacement threads. An uncooperative custom adapter may retain
one daemon thread; Java cannot safely kill arbitrary code. Close never waits
past its configured global budget. No exporter is a readiness dependency.

Own aggregate metrics: `apex.observability.admitted`, `.dropped{reason}`,
`.queue.records`, `.queue.accounted.bytes`, `.export.failures`. A contended snapshot
uses -1 (unknown), not zero. Native Observation meters use fixed operation names
and `apex.outcome`; the scoped filter rejects other dimensions on these names.
This filter does not sanitize arbitrary application logs/traces or legacy metrics.

Interpret `outbox.ack` as broker acknowledgement observed, `outbox.mark.attempt`
as method/transaction attempt and `outbox.mark:COMMITTED` only as a local callback
from the actual outer transaction after an affected-row update. Likewise
`inbox.transaction:COMMITTED` is a best-effort after-completion hint, not a durable
telemetry journal. `ack.invocation:SUCCEEDED` means the acknowledge method returned;
independent Kafka Admin evidence is needed to prove group offset advancement.
`quarantine:SUCCEEDED` means confirmed DLT publication, not business success.
Client/server response completion never proves a universal business result.

Acceptance: `tools/ops02/verify-ops02.ps1 -Offline -MavenCommand <mvn.cmd>`.
The `ops02-acceptance` Maven profiles in demo Cells include a **test classifier**
with controlled exporters; normal Cell builds omit it. Never deploy that classifier
or activate `ops02.fixture.*` outside the disposable acceptance fixture.

No Console, history database, owner-state API, control action, Saga decision,
Kafka header/payload mutation, token propagation or M36 delegation is implemented.
