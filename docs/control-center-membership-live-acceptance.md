# Authoritative multi-instance LIVE acceptance — 2026-11-10

Status: LIVE acceptance for scoped Eureka membership, automatic duplicate detection and restart
distinction. This closes the previous duplicate-instance LIVE_PARTIAL blocker. It does not
qualify production readiness or full CI.
Final successful live run: 2026-11-10 07:45:22–07:48:22 UTC (179.9 seconds).

Evidence: [live receipt](evidence/control-center-membership/live-acceptance.json),
[two replicas](evidence/control-center-membership/membership-two-replicas.json),
[restart](evidence/control-center-membership/membership-restart.json),
[automatic duplicate detection](evidence/control-center-membership/membership-duplicate.json),
[cross-environment filtering](evidence/control-center-membership/membership-cross-environment.json),
[registry-confirmed disappearance](evidence/control-center-membership/membership-disappeared.json),
[registry unavailable](evidence/control-center-membership/membership-registry-unavailable.json).

## Architecture and scope of authority

No parallel registry was created. `CellServiceDirectory` now has a membership snapshot SPI,
implemented by `CloudCellServiceDirectory` over the existing Spring Cloud adapters. The live
test starts the existing `apex-discovery-server` in a separate owned process. Its HTTPS
listener requires a certificate issued by the test CA. Exact URI SAN identities control
registry reads and per-service registration/renewal/deletion. The Console identity can read
but cannot write. Configurable registry authorization runs before Eureka's Jersey filter.

The first live run exposed an ordering defect: Jersey served a request before a lower-priority
authorization filter. That attempt failed; the filter was moved to highest precedence and
the denied-peer gate was rerun successfully. No security check was disabled to obtain a pass.

The existing Eureka adapter registers an opaque unique registry ID for each application-context
incarnation. Registry IDs differ from the logical application instance ID. A shared
`CellRuntimeIdentity` supplies environment, cell, instance ID and epoch to both registry
metadata and the running contract provider. The old transport-only registration behavior is
preserved when the new membership opt-in is disabled.

`RuntimeMembershipPoller` reads registry membership for an explicitly allowed set of services.
Each member is queried through `InstanceQueryGateway`, using the existing gRPC runtime and
discovery channel adapter pinned to an opaque registry ID. Registry metadata cannot supply
TLS trust or a caller-selected URL. TLS authority remains local reviewed policy; normal
certificate validation, caller URI SAN policy and per-contract authorization remain active.
The returned service, environment, cell, instance ID and epoch must match the registry member.

A duplicate requires all of the following: distinct epochs for the same environment/service/
logical instance ID, fresh successful pinned mTLS replies, and a stable overlapping authoritative
registry set before and after the scan. Cached epochs, expired leases, failed RPCs or partial
metadata alone cannot establish a duplicate. A sequential restart can leave an old registry
lease temporarily visible; because the old process cannot answer the pinned read, it does not
become a simultaneous duplicate. During registry failure or stale coverage, duplicate evaluation
becomes unknown rather than retaining a current-positive claim indefinitely.

Authority is explicitly scoped to the configured registry and service allowlist. Eureka is
lease-based and eventually consistent; this is not a globally atomic process-liveness oracle.
The existing directory enumerates UP/transport-ready replicas. Leaving that eligible set is
membership disappearance, not proof that a machine or process is dead; unready/draining
replicas outside that set are not claimed as complete runtime inventory.
The controlled live registry fetch interval is one second, maximum accepted registry age three
seconds, and positive incarnation evidence is bounded to fifteen seconds. Health remains UNKNOWN.

## API and UI

Deployment configuration remains server-only and opt-in. Cells require
`apex.discovery.provider=eureka`, `apex.discovery.membership.enabled=true`, enabled contract
discovery and explicit environment/instance identity. Console requires
`apex.ops.membership.enabled=true`, the matching discovery provider/environment and allowed
owners, `apex.discovery.registration.enabled=false`, `eureka.client.register-with-eureka=false`,
an HTTPS Eureka URL and `eureka.client.tls.enabled=true`. The existing SSL bundle/keystore,
registry URI-principal policy, reviewed per-cell TLS authorities and gRPC contract grants
must be configured. The live harness supplies these from owned runtime facts and temporary
certificates. None of these settings is a browser-supplied URL or credential.

Schema version 2 adds `memberships` while retaining the previous fields and accepting older
responses with no membership array. Each member carries environment, cell/service, instance ID,
epoch, registry ID/identity, discovery source, lastSeen, freshness, membership state, coverage
and transport status. No host, address, port, certificate, credential or message body is exported.
Registry-only unverified members have registry provenance; previously verified observations
retain their actual lastSeen when transport fails.

The projection filters owner/environment authorization before joining contracts, creating
instance nodes or aggregating results. It shows every admitted visible instance, historical
disappearance tombstones and service summaries. Replicas can have different contracts during
rolling deployment: declarations are joined per manifest, rather than combining a handler on
one replica with subscriptions from another. Descriptor/connection identities are deduplicated.

The poller admits at most eight services and sixteen current members per service, retains a
bounded disappearance history, and uses bounded RPC deadlines. The projection caps membership
rows at 128 and reports truncation as PARTIAL. Unsupported/custom adapters, invalid metadata,
cross-environment entries, changing registry sets and unavailable transports expose limited
coverage. A complete empty registry result is distinct from an unavailable registry.
Confirmed disappearance tombstones remain disappeared during later registry outages. Cached
contracts from those departed incarnations are not resurrected by an UNKNOWN registry state.

The real Next BFF validates the extended contract. The existing Topology view renders states,
provenance, coverage, registry identity, epochs and service counts. It continues to use OIDC
sessions, server-held JWTs, Console owner grants and read-only routes. Browser requests never
target the registry or Console directly.

## Actual LIVE validation

The existing five-cell fixture was extended, using the same embedded PostgreSQL/KRaft,
certificate helper, disposable Identity, packaged-cell launcher and real authenticated browser
bridge. It launched Order, Inventory, Payment, Customer, Reporting, Console, Identity, Next,
and the existing Eureka server. Additional Reporting JVMs exercised the scenarios below.
Customer/Reporting gRPC and Eureka registration were enabled only for this controlled run.
Configuration was resolved from packaged YAML; static comparison fixtures did not configure
the running services.

All five primary mTLS manifest calls succeeded. Their publications, subscriptions, groups and
event handlers matched source declarations. Independent authenticated registry reads returned
the two actual Reporting registry IDs, and both matched verified Console members. The real
browser tested baseline five instances, six current instances after scale-out, and two
automatically detected duplicate members. No cookies or tokens were injected by the test.

| Scenario | Required live observation |
|---|---|
| Baseline | 5 services, 5 verified instances, 26 graph nodes, 28 declaration edges |
| Two simultaneous Reporting replicas with distinct IDs | 6 verified instances; no duplicate |
| Sequential restart using the same logical instance ID | New epoch, retained old evidence, no duplicate |
| Two simultaneous Reporting processes sharing logical instance ID | Two distinct verified epochs automatically marked DUPLICATE; UI showed both |
| Foreign-environment Reporting registration | Excluded from visible members and joins; coverage PARTIAL; no cross-environment duplicate |
| Stopped original Reporting process | Independent registry returned 404; the Console marked that exact original epoch DISAPPEARED under complete scoped coverage |
| Registry stopped | REGISTRY_UNAVAILABLE / unknown current membership; stale instance evidence retained without current duplicate claims |
| Untrusted gRPC certificate | Sanitized UNAVAILABLE |
| Trusted gRPC peer lacking discovery grant | PERMISSION_DENIED |
| Trusted registry certificate lacking registry access | HTTP 403, enforced before Jersey |
| Operator granted only Order | Only Order data: 7 nodes and 4 declaration edges, including its one visible instance |

Order → Inventory → Payment also completed: CONFIRMED / RESERVED / AUTHORIZED, checked against
the independently owned databases. All 28 messaging links remained DECLARED with null lastSeen.
Registry observations and verification RPCs are not OpenTelemetry traffic evidence.

Disappearance checks deliberately wait for registry confirmation of the original epoch; an
unavailable RPC alone is not accepted as proof of removal. Native registry cache delays are
visible as uncertainty until that confirmation arrives. Node totals after restarts include
bounded historical members and are not counts of current live replicas.

Final receipt counts:

| Snapshot | Nodes | Declaration edges | Current verified members | Duplicate members |
|---|---:|---:|---:|---:|
| Baseline | 26 | 28 | 5 | 0 |
| Two Reporting replicas | 27 | 28 | 6 | 0 |
| Sequential restart | 28 | 28 | 6 | 0 |
| Simultaneous duplicate ID | 29 | 28 | 6 | 2 |
| Registry-confirmed Reporting disappearance | 27 | 26 | 4 | 0 |
| Registry unavailable | 27 | 26 | 0 | 0 |

Four confirmed disappearance tombstones remain in the final outage snapshot. The departed
consumer links remain absent; the outage does not resurrect the former 28-edge graph.

## Reproduce and test results

```powershell
$env:JAVA_HOME='C:\Program Files\Java\jdk-21'
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/control-center/verify-live.ps1 -Membership
```

`-PrepareOnly -Membership` builds isolated current-source artifacts. `-RunPrepared -Membership`
explicitly reuses recorded artifacts and hashes, rebuilds Next and repeats live acceptance.
No user process is terminated to obtain a port. The owned registry is bound to its allocated
port and configured to refer only to itself, not the pre-existing listener on 8761. All owned
processes and embedded infrastructure are stopped at completion. Public receipts are copied
through a credential scan; raw logs and private keys remain outside published artifacts.

Targeted backend checks passed for directory validation, Eureka registration, gRPC resolver/
channel behavior, Kubernetes adapter tests, topology authorization, runtime membership,
registry peer policy and Console JWT security. Unit tests use controlled inputs and are
reported separately from the real LIVE receipt. Frontend: 26 unit tests passed, lint and
production build passed. Three real OIDC/Chrome runs covered baseline, scale-out and duplicate
states. The final real integration test and its owned prerequisites passed.

## Order compilation and full CI — separate finding

The deleted legacy `OrderOperationsProperties`, `StalledOrderController` and `StalledOrderQuery`
were not restored. Their obsolete unit test was updated to verify the retired surface stays
absent and the existing bounded `OPS_OWNER_SNAPSHOT:1` replacement contract remains valid.
Both replacement tests pass, and full Order test compilation now succeeds.

The complete Order default test target was also run: **42 tests, 7 failures, 7 errors, 0 skipped**.
Remaining failures involve `InventoryOutcomeIntegrationTest`, `OrderOperationsIntegrationTest`,
`OrderCellIntegrationTest`, `OrderGrpcLocalTest` and `OrderReceiptVersionTest`. Examples include
legacy profile expectations, assertions about removed admin endpoints and a missing event-handler
registry when tests disable messaging while the intentionally changed saga preflight is active.
Restoring full CI requires separate migration of those tests/runtime expectations. This task
does not revert intentional saga/profile changes or label that full target green.

Those legacy tests briefly used their default loopback Eureka registration settings. Their
logs record test-instance deregistration with HTTP 200 during shutdown; no unrelated instance
was deleted or stopped. Subsequent live acceptance used the separate authenticated registry.
Future legacy/full-suite runs should explicitly disable external Eureka or own their registry.

## Limitations and files

Kubernetes membership is not LIVE-qualified here. Its existing EndpointSlice adapter exposes
partial coverage when environment/incarnation metadata is unavailable; Pod metadata enrichment
and a real Kubernetes run remain work. Custom adapters default to unsupported membership.
There is no claim of complete global membership or instantaneous lease expiry. Duplicate
decisions require distinct epochs and fresh overlapping authoritative evidence.

Still unimplemented: OTel interaction ingestion/service graph, distributed trace queries,
Control Center Kafka Admin metadata/lag provider, orchestrator/Actuator health provider,
automatic gRPC caller graph, separate proposed services/instances/connections/traces/sagas/
messaging APIs and execution/saga overlays. No destructive runtime controls were added.

Main changes: discovery SPI (`CellMembership`, `CellRuntimeIdentity`, directory/adapters),
pinned `InstanceQueryGateway` and gRPC discovery adapter, Console membership poller/projection,
additive topology DTO/schema/UI, secure registry peer filter, five-cell live harness and
PowerShell membership mode, focused tests and the retired-admin regression test. Customer/
Reporting retain discovery-off defaults. The original application and unrelated working-tree
changes are preserved. No commit, push or deployment to a shared environment was performed.
