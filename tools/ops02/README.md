# OPS-02 verification

Use Java 21 (Oracle JDK recommended on the historically qualified Windows host),
Maven 3.9.x and populated third-party/native caches. No Docker, cluster, shared
broker or database is needed. Never direct these fixtures at deployment resources.

```powershell
$env:JAVA_HOME = '<installed Java 21 JDK>'
$env:Path = "$env:JAVA_HOME/bin;$env:Path"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/ops02/verify-ops02.ps1 `
  -Offline -MavenCommand '<absolute mvn.cmd>'
```

Full mode builds/tests instrumentation, then uses the existing three-Cell runner
with an explicit acceptance-only Maven profile. It reuses the unchanged M30 traffic,
durable PostgreSQL settings, stock/payment/outcome/offset assertions and bounded
restart/drain behavior. Four real packaged runs use disabled/healthy/failing/disabled
diagnostics. Each owns three native PostgreSQL servers and one KRaft broker. Export
failure is a test-classifier SPI exception; the real-socket sink/unavailable socket
is separately tested in the starter. No real OTel Collector is claimed.

The runner then rebuilds **default** artifacts and runs the complete supported M35
product regression, including the independent external consumer's new optional
observability mode. No skip flag, altered deadline or relaxed assertion is used.
`-Phase Foundation` and `-Phase PackagedMeasurements` are bounded development phases,
not full acceptance. PackagedMeasurements requires the source-built starter/test JAR
installed by Foundation. The full gate remains required.

Evidence stays under ignored `out/ops02/run-*`: logs, predeclared policy, admission
CSV and per-run M30 JSON/JUnit/process logs plus aggregate exporter receipts. Case
counts must deduplicate classname + testcase name across profiles and repeated runs.
Preserve first failures. Do not copy old XML into a successful gate's evidence.

The admission microexperiment uses three A/B/A/C/A rounds, 3,000 warmup and 30,000
samples per window. It measures current-thread allocation and CPU, not total process
allocation or a distributed latency stage. Packaged HTTP and Saga timings remain
separate; compare identical phases and disclose host/JIT/GC/drift. A failing exporter
may cause diagnostic drops; that is expected coverage loss. Business/offset/stock
assertions must remain green. There is no approved numeric overhead SLA, so measured
overhead cannot be marketed as an SLA PASS.

No benchmark can prove production capacity, hard-real-time scheduling, real PSP,
multi-broker failover, production PKI or Kubernetes behavior on this single host.
