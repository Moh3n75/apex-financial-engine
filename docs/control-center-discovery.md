# Control Center: contract discovery iteration

Implemented on `credit-engine-event-sourcing`; existing application and unrelated changes retained.
No commit or push performed. See [initial audit](control-center-audit.md) for the original inventory.

## HTTP exchange bounds

`ops-client.ts` now owns the timeout and one of four concurrency slots until the complete
HTTP response has been consumed. It checks Content-Length early and counts incoming bytes
incrementally, aborting/cancelling above 65,536 bytes. The four-second deadline covers
credential lookup, connection, response headers and streaming body. A timeout race also
terminates stalled stream reads. Cleanup runs for success, HTTP denial, stream error,
oversize and timeout; late headers are cancelled too. Redirects are rejected. Only the
bounded in-memory response reaches the existing JSON/schema parsers. Error messages remain
redacted. This is a process-local budget, consistent with the existing deployment model.

## Versions and evidence

* `GET /ops/v1/topology` defaults to schema version 1 and retains its base fields/semantics.
  The additional empty `providers`, `descriptors`, `unresolved` arrays are additive.
* `GET /ops/v1/topology?schemaVersion=2` adds authorized contract-discovery results;
  unsupported versions return 400. The BFF requests version 2, and validates both versions.
* The shared Java DTO is `TopologySnapshot` in `apex-operations-contracts`; the matching
  Zod schema supports CELL, SERVICE, INSTANCE, KAFKA_TOPIC, CONSUMER_GROUP and HTTP/GRPC/KAFKA.
  Graph and evidence lists are bounded. The joiner limits the graph and removes dangling
  edges; truncation is reported by a PARTIAL provider entry.
* DECLARED and OBSERVED evidence remain separate entries. Declarations have null lastSeen;
  instance snapshots cannot support observed traffic edges. Observed interactions require
  their own evidence (future OTel adapter); health requires runtime health evidence.
* Effective manifests, source-derived static declarations, and fixture data carry distinct
  sourceMode values. Reading configuration inside a running process still produces a
  declaration, not a traffic observation or broker existence claim.

## Provider architecture and deployment

`apex-platform-contract-discovery` is an opt-in platform module with a Spring Boot
auto-configuration. Each of the five cells includes it. It exposes the existing gRPC
`CellGateway.InvokeQuery` contract `OPS_CONTRACT_DISCOVERY:1` through `ContractDiscoveryQuery`.
The provider gathers:

* effective `PublisherRouting` type/version/topic routes; or, for single-topic publishers,
  explicit `PublishedContracts` beans referencing the immutable contracts used by handlers;
* exact effective `ConsumerTopics` and consumer group, plus registered event handler keys;
* sanitized `HandlerRegistry` gRPC descriptors (kind/type/version only).

The provider does not inspect message bodies, databases, Kafka, environment dumps or sockets.
No handler objects, Java class names, payload types, credentials or endpoints cross the API.
No hardcoded service graph exists in Console or React. Cells declare their publication
catalogs next to their business code. Reporting has no publication catalog because its
outbox publisher is disabled. Disabled publisher/consumer switches and effective property
overrides are respected. A disabled auto-start listener remains a declaration with a gap;
it does not become a live subscription claim.

Set these in the deployment's existing server-side configuration:

```properties
apex.contract-discovery.enabled=true
apex.contract-discovery.environment=<actual-environment>
apex.contract-discovery.cluster=<reviewed-logical-cluster-id>
apex.contract-discovery.instance=<actual-instance-id>
```

Cluster IDs are explicit logical labels, never bootstrap addresses. Unknown cluster identities
are unresolved and are never joined together. The existing authenticated gRPC transport and
peer contract policy must allow `QUERY:OPS_CONTRACT_DISCOVERY:1` for Console. No policy is
automatically relaxed. Order/Inventory/Payment already have gRPC servers. Customer/Reporting
now include the server starter with `spring.grpc.server.enabled` defaulting to
`${apex.grpc.server.enabled:false}`: opt in to `apex.grpc.server.enabled=true` and supply the
existing platform's mTLS identity/authorization and client destination configuration to query
them remotely. Default startup remains HTTP/Kafka only. No new public HTTP endpoint is added.

Console configuration is independent of the existing owner-snapshot inventory:

```properties
apex.ops.contract-discovery.environment=<same-actual-environment>
apex.ops.contract-discovery.owners=order-cell,inventory-cell,payment-cell,customer-cell,reporting-cell
```

The existing QueryGateway must have real destinations/discovery metadata for those names.
No destination URLs are guessed here. The poller admits at most eight owners, polls serially
every ten seconds with one-second RPC deadlines, and caches sanitized manifests. Environment
and service identity, effective-declaration source mode, and collection clock are checked.
Old manifests remain explicitly stale/unavailable; they do not assert continued traffic.
Configured discovery without an explicit environment or transport fails at startup.

## Join semantics and authorization

Resource visibility is evaluated before retaining manifests, joining declarations or
aggregating counts. It uses the manifest's verified environment and owner. A hidden publisher
cannot establish a consumer connection or leak through provenance/provider names.

A service-to-topic edge requires an exact publication contract and effective destination.
A topic-to-group and group-to-service chain requires the same authorized environment,
logical Kafka cluster, exact topic name and event type/version in a consumer's registered
handlers and subscriptions. A subscribed topic alone cannot identify its event contracts.
No-visible-match gaps do not reveal whether a hidden producer exists. Unknown clusters,
missing registries/catalogs, disabled listener auto-start and unmatched declarations remain
explicit unresolved facts. gRPC registrations appear as capabilities only; neither handler
presence nor peer allowlists identify callers.

The existing owner-poller gRPC edges remain DECLARED because there is explicit caller code
and configured targets for those reads. They are not inferred from discovered handlers.
The browser continues to call only Next; no browser infrastructure access or mutations exist.

## Example and validation

[Full version 2 topology JSON](examples/control-center-topology-v2.json) is generated by the
Java joiner from the five cells' actual YAML and actual handler annotations. Its provider
source modes are STATIC_DECLARATION. It is an example of real repository contracts, not a
live environment snapshot. It contains nine complete Kafka chains (three edges each) plus
the unconsumed legacy Order publication, for 28 connections. It includes no inferred gRPC
caller edges and no live observations.

`RepositoryContractDiscoveryTest` in each cell uses its actual application.yml,
`PublishedContractConfiguration` and handler classes with mocked handler dependencies.
It exercises the real platform registries, contract provider and YAML placeholder resolution
without invoking business handlers or starting Kafka/PostgreSQL. The tests write static
manifests; checked-in copies make Console integration tests repeatable on a clean checkout.
`RepositoryTopologyIntegrationTest` optionally verifies freshly generated cell artifacts;
the frontend parses and renders the exported graph through the production Zod schema.

Reproduce the targeted Java checks and regenerate the example (Java 21):

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/control-center/verify-contracts.ps1 -Offline
```

The script accepts `-MavenCommand` for other installations. It isolates compilation of the
new Order test with a temporary POM because the existing unrelated
`OrderOperationsPropertiesTest` references the staged-deleted `OrderOperationsProperties`.
The temporary file is removed in finally; the real POM and unrelated test remain intact.
This is targeted validation, not a claim that the full Order test suite is green.

Frontend checks: `pnpm lint`, `pnpm test`, `pnpm build`, `pnpm test:e2e:fixture` in
`apps/operations-web`. Streaming tests cover stalled bodies, incremental oversize rejection,
concurrency held during body reads and release after failures. Other tests cover exact joins,
environment/permission isolation, source modes, version compatibility, read-only HTTP routes,
registered gRPC contract encoding, provider opt-in and effective overrides.

No deployed multi-cell environment, live Kafka metadata, mTLS remote discovery, OTel backend
or orchestrator was contacted for validation. Those live results are unavailable here;
the UI retains provider coverage and makes no live-data claim.

Validation receipt: 21 targeted Java tests passed (five repository checks, one per cell, two
provider tests and fourteen Console projection/integration/security tests); 25 frontend
tests and four fixture Playwright checks passed. ESLint, TypeScript checking and the Next
production build passed. Next reports its existing middleware-to-proxy deprecation.

## Files changed in this iteration

* Root `pom.xml`: registers the new platform module.
* `apex-platform-contract-discovery/pom.xml`, auto-configuration imports, and
  `src/main/java/com/apex/platform/contracts/{ContractDiscoveryAutoConfiguration,ContractDiscoveryProvider,PublishedContracts}.java`;
  corresponding `ContractDiscoveryProviderTest.java`.
* `apex-operations-contracts/src/main/java/com/apex/operations/contracts/{ContractDiscoveryQuery,ContractManifest,TopologySnapshot}.java`.
* `apex-platform-messaging/src/main/java/com/apex/platform/messaging/consumer/{EventHandlerRegistry,DefaultEventHandlerRegistry}.java`
  and `consumer/internal/DiscoveringEventHandlerRegistry.java`.
* `apex-platform-grpc/src/main/java/com/apex/platform/grpc/internal/HandlerRegistry.java`.
* `apex-operations-console/src/main/java/com/apex/operations/console/{TopologyApi,ContractDiscoveryPoller,ContractTopology}.java`;
  `src/test/java/com/apex/operations/console/{TopologyApiTest,ContractTopologyTest,RepositoryTopologyIntegrationTest}.java`;
  `security/OpsM36aHttpSecurityIT.java`; five static manifests in `src/test/resources/contracts/`.
* Each of `order-cell`, `inventory-cell`, `payment-cell`, `customer-cell`, `reporting-cell`:
  `pom.xml` and `src/test/java/com/apex/<cell>/RepositoryContractDiscoveryTest.java`.
* Order/Inventory/Payment/Customer: `src/main/java/com/apex/<cell>/PublishedContractConfiguration.java`.
* `ThreeCellOrderHandler.java`, `ReleasePlacedOrderHandler.java`, `PaymentHandler.java`:
  immutable publication constants exposed for the catalog; handler behavior unchanged.
* Customer/Reporting `src/main/resources/application.yml`: gRPC disabled by default, ready for explicit opt-in.
* `apps/operations-web/src/server/{ops-client.ts,ops-client.test.ts}`;
  `src/lib/{topology.ts,topology.test.ts,repository-topology.test.ts}`;
  `src/components/topology-view.tsx`; `e2e/smoke.spec.ts`; `README.md`.
  The existing topology BFF route delegates through the updated client and retains auth/no-store/mutation rejection.
* `tools/control-center/verify-contracts.ps1`, this document, and `docs/examples/control-center-topology-v2.json`.
