# Five-cell Contract Discovery LIVE acceptance — 2026-11-09

Verdict: **LIVE_PARTIAL — automatic duplicate-instance detection remains unimplemented.**
This is real process/socket/browser acceptance, not the static manifest integration test.
Full CI is **not green**: the unrelated Order test-compilation failure remains present.
Final live run: 2026-11-09 14:39:42–14:41:46 UTC (124.5 seconds).

Evidence: [live acceptance receipt](evidence/control-center-live/live-acceptance.json),
[real Console topology](evidence/control-center-live/live-topology.json),
[wrong-cluster snapshot](evidence/control-center-live/wrong-cluster-topology.json),
[expired/unavailable snapshot](evidence/control-center-live/expired-unavailable-topology.json).

## Runtime and isolation

The run reuses `OrderInventorySagaIT.launch`, the OPS-03B ephemeral CA/certificates,
the M36A disposable Identity and authenticated Next/Playwright bridge, embedded PostgreSQL,
and embedded KRaft Kafka. No parallel deployment system was introduced.
Docker's daemon was unavailable; the repository's embedded infrastructure ran successfully.
An existing process on port 8761 was left untouched. Five explicit QueryGateway destinations
used the actual bound test-owned gRPC ports, so no Eureka deployment was required.

Five packaged cell JVMs, a packaged Console, a packaged Identity server, Next.js and a real
Chrome browser were launched. Each cell had its own PostgreSQL server; Identity owned another.
Customer and Reporting gRPC were enabled only by test-process arguments. Default application
configuration remains unchanged. All test-owned JVMs and embedded infrastructure were stopped.

Environment: `control-center-live`; logical Kafka cluster: `owned-kraft`.

| Contacted cell | Explicit instance | Live manifest result |
|---|---|---|
| order-cell | order-cell-one | SUCCESS |
| inventory-cell | inventory-cell-one | SUCCESS |
| payment-cell | payment-cell-one | SUCCESS |
| customer-cell | customer-cell-one | SUCCESS |
| reporting-cell | reporting-cell-one | SUCCESS |

Every primary call used the real `QueryGateway` and the platform's gRPC client, over required
mTLS. Manifest schemaVersion=1, exact service identity, environment, cluster, explicit instance,
effective publisher routes, subscriptions, consumer groups and event handler registrations
were asserted. The receipt records the call timestamps and redacted individual manifests.
Runtime launch values are resolved from each packaged JAR's `application.yml`, with controlled
broker/database/TLS/identity overrides. Static comparison manifests do not configure the cells.

## Security evidence

* All cell servers used `client-auth=require`, a test-owned CA truststore, exact URI SAN
  identity policy and per-peer contract grants. `insecure-loopback-test-only=false` was explicit.
* The existing `m32-client` certificate was granted only `QUERY:OPS_CONTRACT_DISCOVERY:1`.
  The separate trusted `m32-observer` identity was intentionally granted a different query.
* A self-signed, untrusted client certificate failed with sanitized gRPC `UNAVAILABLE`.
  A CA-trusted peer without the discovery contract received `PERMISSION_DENIED`.
  These were real calls to the running Order server, not mocked errors. The sanitized gateway
  does not export the underlying TLS exception; no stronger TLS diagnostic is claimed.
* Anonymous Console access returned 401. Authenticated mutation returned 403 from the
  security chain, which precedes the read-only filter in this runtime.
* A second authenticated user granted only Order visibility saw exactly six nodes and four
  declared publication edges. Other owners and their consuming relationships were absent.
* The browser used the real Identity login and normal OIDC callback/session path. No bearer
  token or session cookie was injected by the test. The Next BFF forwarded its server-held
  bearer to the JWT-protected Console. Browser requests never targeted Console directly;
  no token was found in rendered HTML, and BFF mutations returned 405.

No transport validation, peer identity, JWT authentication or owner authorization was disabled.
Identity and BFF retain the existing controlled loopback HTTP test boundary; this is not a
production deployment qualification.

## Live topology and workflow

The real Java Console returned schemaVersion=2 with **21 nodes and 28 connections**:
five CELL nodes, five SERVICE nodes, seven KAFKA_TOPIC nodes and four CONSUMER_GROUP nodes.
Five contract providers reported AVAILABLE / EFFECTIVE_DECLARATION. The authenticated
Next BFF returned the same five services and 28 connections; the actual Topology page
rendered all five services and the Order publication link.

An actual HTTP Order placement went through Kafka, Inventory and Payment. Independent
queries against their owned databases confirmed Order CONFIRMED, Inventory RESERVED and
Payment AUTHORIZED. This proves the exercised business workflow completed; it does not
turn declaration edges into observed traffic. Every graph connection remained DECLARED,
with null lastSeen. No OpenTelemetry spans were collected.

## Comparison with source-generated static declarations

All five live publication lists, subscriptions, consumer groups and registered event handlers
matched the checked-in source-generated fixtures exactly. All fixture gRPC capabilities
were present. The live registries additionally exposed `QUERY:OPS_CONTRACT_DISCOVERY:1`
on each cell because the opt-in provider was actually enabled. Expected identity differences
were environment, cluster, instance, epoch, collectedAt and sourceMode. The individual live
manifests and comparison receipt preserve these facts without endpoints or credentials.

## Failure matrix

| Scenario actually exercised | Result |
|---|---|
| Reporting process stopped | Provider SOURCE_UNAVAILABLE; retained declarations were not relabeled as current observations |
| Retained Reporting manifest aged beyond 30 seconds | Timestamp expiry asserted; coverage remained SOURCE_UNAVAILABLE and cached declaration graph retained 28 edges |
| Console restarted while Reporting remained stopped | Four available manifests, 26 connections; absent consumer links were omitted |
| Reporting restarted with `wrong-cluster` | Five available manifests but only 26 connections; exact cluster joins prevented Customer-to-Reporting consumer links and reported unresolved matches |
| Untrusted mTLS client | Discovery failed, sanitized UNAVAILABLE |
| Trusted client without query grant | PERMISSION_DENIED |
| Authenticated user denied other owners | Six visible Order nodes, four publication edges; hidden owners did not participate in joins |
| Two simultaneous Reporting processes using the same service/instance name | Both were contacted over mTLS, with distinct real epochs; **automatic duplicate detection is unimplemented** |

The duplicate probe demonstrates a gap, not a passing deduplication guarantee. The provider
polls one configured destination per logical service and does not have an authoritative
multi-instance registry. Different epochs can also mean a legitimate restart, so the run
does not invent a conflict rule that would reject normal restarts.
The wrong-cluster case deliberately changes the logical label while keeping the owned broker;
it verifies label-based isolation, not physical Kafka cluster identity verification. Expiry
under transport failure is represented by the original timestamp plus SOURCE_UNAVAILABLE;
there is no separate combined expired/unavailable status.

The direct-call ledger records seven successes (five primary probes and two duplicate probes)
and two expected failures. Console's periodic polling also made real background RPCs; their
exact attempt count is not instrumented and is not included in that direct-probe total.

## Repeatability and results

```powershell
$env:JAVA_HOME='C:\Program Files\Java\jdk-21'
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/control-center/verify-live.ps1
```

The script checks ownership-sensitive ports without killing foreign processes, runs the real
prerequisite gate, installs current platform dependencies, records the full Order test-compile
result, packages current working-tree sources into isolated Maven output directories, builds
Next, and runs `ControlCenterFiveCellLiveIT`. `-PrepareOnly` stops after packaging and explicitly
reports PREPARED_NOT_LIVE_ACCEPTANCE. Temporary packaging POMs are removed in finally; existing
POMs and unrelated staged/uncommitted changes are preserved.
`-RunPrepared` explicitly reuses the prepared JARs, records their hashes, rebuilds Next and
reruns live acceptance. The complete packaging and resumed live paths were exercised. The
final tightened packaged-YAML launch check was also rerun through the same targeted Maven IT.

The script uses `-Dmaven.test.skip=true` only for packaging, with the unrelated full test-compile
failure captured separately. The live IT compiles and runs normally in Inventory. No full-suite
failure is silently converted into a passing result.

Tests: real prerequisite IT passed; five-cell live IT passed its implemented gates while
reporting LIVE_PARTIAL; authenticated Chrome test passed; frontend lint and production build
passed. The live IT includes the failure scenarios above, rather than counting static tests
as live checks. Earlier attempts exposed two harness assumptions: a mutation status expected
405 while Security returned a valid denial of 403, and a Java HTTP/2 readiness probe failed
with EOF although Next was ready. The final harness accepts the actual security rejection
and reuses OPS-04B's `HttpURLConnection` readiness strategy. Security was not weakened.
The PowerShell launcher also handles native stderr as diagnostics and checks exit codes,
so Next's existing middleware deprecation warning does not terminate a successful build.

Authoritative redacted receipts are under `out/control-center-live/`: `live-acceptance.json`,
`prerequisites.json`, `live-topology.json`, the five `*-live-manifest.json` files,
`denied-owner-topology.json`, `expired-unavailable-topology.json`,
`missing-consumer-topology.json` and `wrong-cluster-topology.json`.
`launcher.json` records orchestration/build hashes; the live receipt is authoritative for
contacts and outcome. Raw JVM/browser/native logs and generated private key material are
not publication artifacts. A selected redacted snapshot is retained in `docs/evidence/control-center-live/`.

## Separate unrelated failure and unimplemented capabilities

`order-cell/.../integration/OrderOperationsPropertiesTest.java` still references the staged-deleted
`OrderOperationsProperties` class (lines 17, 25 and 27). The full `test-compile` command exits 1.
Its local log is `out/control-center-live/order-full-test-compile.log`. This task does not
restore the deleted class, change the unrelated test, or claim full CI is green.

Still unimplemented: automatic duplicate-instance detection; complete multi-instance/runtime
membership; OTel observed interaction ingestion/service graph and trace queries; Kafka Admin
metadata/lag as a Control Center provider; Actuator/orchestrator health provider; automatic
gRPC caller discovery; the proposed separate services/instances/connections/traces/sagas/messaging
read APIs and execution-trace/saga overlays. Explicit publisher-without-consumer diagnostics
are also not emitted: missing consumers currently surface through provider coverage and absence
of topic-to-group/service links. There are no runtime mutation controls.

No commit, push or deployment to an existing shared environment was performed.

Files added for this iteration: `tools/control-center/verify-live.ps1`,
`inventory-cell/src/test/java/com/apex/inventory/ControlCenterLivePrerequisitesIT.java`,
`inventory-cell/src/test/java/com/apex/inventory/ControlCenterFiveCellLiveIT.java`,
`apps/operations-web/playwright.control-center-live.config.ts`,
`apps/operations-web/e2e/control-center-live.spec.ts`, this report and the twelve selected
redacted JSON receipts under `docs/evidence/control-center-live/`. `inventory-cell/pom.xml`
adds only a test-scoped gRPC client dependency for live probes. No production application
source was changed in this iteration.
