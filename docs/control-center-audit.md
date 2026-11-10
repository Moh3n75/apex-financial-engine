# Apex Control Center audit and implementation plan

Audited 2026-11-09 on `credit-engine-event-sourcing`, including current uncommitted changes.
The working tree already contains changes to cell configuration, gateway and saga recovery;
these are inputs to this audit and must be preserved. Source declarations are not live discovery.

## Reuse decision: evolve the existing app

`apps/operations-web/package.json` pins Next 16.4.4, React 19.3, pnpm 9.15.9;
Tailwind 4, SWR 2, Zod 4, jose and openid-client are already present. No replacement,
new UI framework, graph library, state store or parallel console is needed for this slice.

| Area | Implemented and reusable | Gap / decision |
|---|---|---|
| Routes | `/`, `/cells`, `/events`, `/inbox`, `/workflows`, `/requests`, `/operations`, `/diagnostics`, `/login`; App Router server pages and `/api/ops/*` handlers | Add `/topology` and a corresponding BFF read route |
| Components/style | `AppShell`, `Shell`, `Panel`, `EmptyState`, `ErrorState`, `MetaRow`, `Badge`; Tailwind zinc light/dark palette, Geist fonts | Reuse; expandable accessible HTML is sufficient initially |
| State | `recent-table.tsx`: local React search/window/cursor state, explicitly armed SWR requests | Preserve search semantics; topology uses request-time server rendering and manual refresh |
| API | `server/ops-client.ts`: fixed loopback Console origin, server-only bearer, GET, no-store, timeout/concurrency/response-size limits; `schemas.ts` validates DTOs | Reuse client path; existing size check occurs after reading body and timeout ends at headers, worth hardening separately |
| Auth | middleware cookie-presence gate; Node session/scope validation; encrypted session, OIDC routes; M36B resource prechecks; Console remains authoritative | Preserve optional M36A mode and fixture isolation. New inventory must filter by actual snapshot environment, not assume `test` |
| Evidence | snapshots distinguish missing/stale/fresh/clock uncertainty/conflicts; recent search separates committed owner facts from broker acknowledgement | Preserve uncertainty. Never treat received snapshots as observed traffic edges or liveness of every replica |
| Tests | Vitest policy/auth tests; Playwright fixture smoke, live and M36A/M36B suites | Add projection, authorization, DTO, BFF and topology UI coverage; fixtures remain synthetic |
| Runtime limits | README and scripts bind Next and Java Console to loopback; no browser infrastructure credentials | Preserve deployment restrictions and existing pages |

## Backend audit and source inventory

* `apex-operations-console`: `ReadApi` exposes `/ops/v1/overview`, `/cells`,
  `/events/{id}`, `/workflows/{owner}/{type}/{ref}`, `/operations/active`.
  `SearchApi` and `RecentSearchApi` expose bounded events/inbox/requests/workflows searches
  and `/recent/search/sources`. `OwnerPoller` polls at most eight explicitly configured
  `apex.ops03.owners` through `QueryGateway`, `OPS_OWNER_SNAPSHOT:1`, with a one-second
  query deadline, cache and failure backoff. `EvidenceCache` keeps bounded conflicting
  snapshots; it is not a membership or trace store. `ReadOnlyFilter` rejects non-GET
  requests; optional JWT security covers `/ops/v1/**`. M36B owner visibility is reusable.
  Existing `ReadApi.visibleViews()` assumes environment `test`; the new endpoint must not.
* `apex-operations-contracts`: `OwnerSnapshot`, `EvidenceQuery`, `OwnerSearch`,
  `OwnerRecentSearch` are bounded gRPC contracts. Snapshot identity includes environment,
  owner, instance and epoch; facts are explicit keys, partial current state without history.
  `apex-operations-owner-jdbc` provides bounded owner reads, recent indexes and known-group
  messaging evidence. These are not broker-wide lag or trace APIs.
* `apex-platform-messaging`: `EventHandlerRegistry`, `DiscoveringEventHandlerRegistry`,
  `AnnotatedEventHandlerDiscovery` and `@OnCellEvent` resolve consumer contracts;
  `PublisherRouting` resolves exact type/version destinations, `ConsumerTopics` parses
  exact subscriptions. Outbox/processed-event stores, reliable consumption, DLT publishing
  and provisioning already exist. `ApexTopicProvisioner` is a mutating provisioning tool,
  so do not reuse it as the Control Center's metadata reader. DLT destination is source
  topic plus configured suffix (`ReliableKafkaConsumerRuntime`), not one global DLT.
* `apex-platform-grpc`: `HandlerRegistry` discovers request/query handlers;
  `Contracts`, `RpcDispatcher`, `GenericGrpcService` and `GrpcInvocationClient` implement
  `apex.cell.rpc.v1.CellGateway.InvokeRequest/InvokeQuery` (`cell_gateway.proto`).
  `DefaultMultiResponderGateway` adds collector semantics. Handler existence and peer
  allowlists alone do not prove a caller-to-callee dependency.
* `apex-platform-discovery`: `CellServiceDirectory`, `CellEndpoint`,
  `CloudCellServiceDirectory` validate named gRPC ports, cell identity, authority and
  freshness. Eureka, Kubernetes and gRPC discovery adapters exist in separate modules.
  Registry membership is runtime evidence, not proof of successful traffic or health.
* `apex-starter-observability`: opt-in Micrometer operational observations, bounded
  exporter and `ActiveOperations` TTL membership. `RpcObservation` and
  `MessagingObservation` instrument local stages. No OTel distributed span ingestion,
  trace query backend, W3C context propagation pipeline or service graph query was found
  in the audited sources. Attempt IDs/epochs are not trace IDs.
* Saga: `ThreeCellOrderHandler`, `JdbcOrderRepository`, Inventory reserve/release handlers
  and `PaymentHandler` implement durable choreography. Separately, `apex-application`
  provides saga orchestrator/recovery/timeout/compensation interfaces and implementations;
  `apex-infrastructure` persists saga/context/outbox data; `apex-bootstrap` schedules
  recovery/timeouts and declares the read-only Actuator endpoint `saga`. These are distinct
  saga models and must not be conflated. Actuator exposure/auth must be verified at runtime.

### Actual services and declared connections

Names below come from `spring.application.name` in each module's configuration. Paths
are repository-relative; defaults can be overridden and do not imply running endpoints.
The application names also include `apex-bootstrap` and `apex-cell-template` (their YAML),
`apex-identity-server` (properties, HTTP default 9000), `apex-discovery-server`
(properties, HTTP default 8761), `apex-api-gateway` and `apex-operations-console`.
These are deployment units, not automatically cells; the template is not evidence of a
deployed service. Sample client targets are source declarations, not registry membership.

| Source → destination | Protocol / contract | Source evidence |
|---|---|---|
| apex-api-gateway → order-cell | HTTP `lb://order-cell`, `/order-cell/api/v1/order-placements[/**]`, StripPrefix=1 | `apex-api-gateway/src/main/resources/application.properties` |
| order-cell → apex.order.events.v1 → inventory-cell | Kafka `ORDER_PLACED:1` | Order YAML, `OrderContracts`, Inventory `ReservePlacedOrderHandler` |
| inventory-cell → apex.inventory.events.v1 → order-cell | Kafka `INVENTORY_RESERVED:1`, `INVENTORY_RESERVATION_REJECTED:1`, `INVENTORY_RELEASED:1` | Inventory YAML/handlers, `ThreeCellOrderHandler` |
| order-cell → apex.order.payment-requests.v1 → payment-cell | Kafka `ORDER_READY_FOR_PAYMENT:1` | Order routing YAML, `ThreeCellOrderHandler`, `PaymentHandler` |
| payment-cell → apex.payment.events.v1 → order-cell | Kafka `PAYMENT_AUTHORIZED:1`, `PAYMENT_DECLINED:1` | Payment YAML/handler, `ThreeCellOrderHandler` |
| order-cell → apex.order.inventory-release-requests.v1 → inventory-cell | Kafka `ORDER_CANCELLATION_REQUESTED:1` | Order routing YAML, Inventory `ReleasePlacedOrderHandler` |
| order-cell → apex.order.legacy.events.v1 | Kafka `ORDER_CREATED:1`; no consumer established in this audit | Order routing YAML |
| customer-cell → apex.customer.events.v1 → reporting-cell | Kafka `CUSTOMER_CREATED:1` | Customer/Reporting YAML, `ProjectCustomerCreatedHandler` |
| apex-operations-console → configured owners | gRPC `OPS_OWNER_SNAPSHOT:1`, `OPS_OWNER_SEARCH:1`, `OPS_OWNER_RECENT_SEARCH:1` | `OwnerPoller`, `OwnerSearchClient`, `OwnerRecentSearchClient`; targets from configuration |
| sample-grpc-client → order-cell | gRPC create/get order requests/queries | `sample-grpc-client/.../SampleGrpcClientApplication.java` |

gRPC servers in Order expose `CREATE_ORDER:1`, `GET_ORDER:1`; Order, Inventory and Payment
expose `GET_ORDER_PERSPECTIVE:1`. Owner operations handlers expose the bounded OPS
contracts when their gates permit. Multi-responder discovery must determine actual target
sets; do not draw all-to-all edges from `test-clients` allowlists.

Default consumer groups from cell YAML:

| Cell | Group | Default subscribed topics |
|---|---|---|
| order-cell | order-cell-inventory-outcome-v1 | apex.inventory.events.v1, apex.payment.events.v1 |
| inventory-cell | inventory-cell-order-placed-v1 | apex.order.events.v1, apex.order.inventory-release-requests.v1 |
| payment-cell | payment-cell-authorization-v1 | apex.order.payment-requests.v1 |
| reporting-cell | reporting-cell-customer-created-v1 | apex.customer.events.v1 |

`OperationalRecoverySagaIT` declares replay topics `apex.inventory.order-placed.replay.v1`,
`apex.order.inventory-outcome.replay.v1`, `apex.payment.authorization-request.replay.v1`,
`apex.order.payment-outcome.replay.v1`, `apex.inventory.release-request.replay.v1` and
randomized `order-m29-*`, `inventory-m29-*`, `payment-m29-*` groups. They are test/runtime
overrides, not default production topology. Its order/payment downtime, deferred COMMIT
faults, quarantine and replay assertions establish useful future acceptance scenarios:
missing response remains UNKNOWN, and compensation completes only after durable release
acknowledgement. Do not add recovery actions to this UI.

HTTP controllers: Order POST `/api/v1/orders`, POST `/api/v1/order-placements`, GET
`/api/v1/order-placements/{orderId}`; Customer POST `/api/v1/customers`, GET
`/api/v1/customers/{id}`; Reporting GET `/api/v1/reports/customers/count` and
`/api/v1/reports/customers/{customerId}`. The deleted staged `StalledOrderController`
is not an available route in this working tree. Never construct health links from HTTP ports.

### Runtime environments

Console defaults to 127.0.0.1:9190; Next to 127.0.0.1:3000. Cell YAML uses environment
overrides, Eureka on loopback:8761, random HTTP/gRPC ports for Order/Inventory/Payment;
Customer/Reporting HTTP defaults are 8081/8082. `docker-compose.yml` declares local
PostgreSQL and single-node KRaft; it does not prove any service is running. Kafka config
defaults differ from compose, reinforcing that effective runtime configuration is required.
`tools/m34/compose.yaml` and `tools/m36*`/OPS acceptance harnesses describe additional local
test environments. Embedded PostgreSQL/Kafka recovery tests launch packaged cells.
No production runtime identity or live orchestrator inventory was established.
Payment YAML comments say listener paused while the effective default is true: trust
resolved configuration and listener runtime status, not comments.

## Target data model

All IDs are environment-scoped opaque IDs. Never merge services across environments.

* Node: `{id, kind: CELL|SERVICE|INSTANCE|KAFKA_TOPIC, name, environment,
  cellId?, serviceId?, instanceId?, epoch?, scope: SERVICE|INSTANCE,
  evidence[], lastSeen: Instant|null, health: UNKNOWN|UP|DEGRADED|DOWN,
  freshness, uncertainty}`. Instance identity includes epoch to prevent restart collisions.
* Connection: `{id, source, target, environment, protocol: HTTP|GRPC|KAFKA,
  scope, sourceInstanceId?, targetInstanceId?, contract?, consumerGroup?,
  evidence[], lastSeen, health}`. Evidence is separately typed
  `DECLARED` or `OBSERVED`, with `{source, reference, collectedAt, lastSeen,
  environment, scope, instanceId?, epoch?, traceId?, spanId?}`. Both kinds may support one
  relationship; retain their timestamps independently. Declaration has null lastSeen.
* Envelope: `{schemaVersion, asOf, coverage, capabilities, nodes[], connections[]}`.
  Coverage includes source limits, sampling, retention and permissions. Provider status
  distinguishes NOT_CONFIGURED, AVAILABLE, STALE, SOURCE_UNAVAILABLE and PERMISSION_GAP.
  Missing or expired observations never negate a declared dependency. Health requires
  specific Actuator/orchestrator evidence; snapshot freshness is not health.
* Trace: trace/span IDs, parent and links, start/end, status, service/instance resources,
  protocol, route/contract and allowed message identifiers. Preserve partial traces and
  sampling. Correlation/causation and saga refs are separate identifiers, never automatic
  proof of a parent span. No message bodies, SQL text, credentials or arbitrary attributes.

## Views

1. Cell: environment selector, cells and aggregate protocol edges; distinguish declared,
   observed and mixed evidence, plus partial coverage. Aggregate only visible children.
2. Service: expand a cell into services. Expanding a Kafka edge shows explicit topic nodes
   and separate producer/topic and topic/group/consumer connections, including contracts.
3. Instance: expand a service into registry/owner/orchestrator instances with epoch,
   lastSeen, freshness, health source and protocol endpoints. Never claim complete membership.
4. Execution Trace: selected sampled execution waterfall and span links; highlight
   asynchronous Kafka hops, retries, errors and gaps. Link saga committed facts separately.

The first slice renders an expandable service/instance inventory for configured owners
and their declared Console gRPC dependencies. It does not fabricate Kafka, cell grouping
or trace views before providers exist. Those levels are follow-up work.

## Read-only API proposal

Java owns `/ops/v1`; browser-facing equivalents are `/api/ops`. All are GET, no-store,
authenticated under existing optional security, with environment/owner grants enforced
before returning nodes or edges. No browser Kafka/Docker/Kubernetes/secret access.

| Resource | Proposed reads | Provider and limits |
|---|---|---|
| `/topology` | environment, level, service, includeTopics | composed snapshot, max 200 nodes/400 edges, continuation, provider status |
| `/services` | environment, cell, cursor, limit ≤50; `/{id}` | effective contract manifests + service directory |
| `/instances` | environment, service, cursor, limit ≤50 | directory + allowlisted Actuator/orchestrator adapters, endpoint redaction |
| `/connections` | environment, service, evidence, protocol, from/to | declared manifests + OTel service graph, window ≤1h, limit ≤100 |
| `/traces` | environment, service, time window, cursor; `/{traceId}` | server-side trace backend, ≤15min search, ≤50 results, ≤1000 spans/detail |
| `/sagas` | environment, owner, model, ref, cursor | existing owner workflow queries and separate legacy saga adapter, ≤16 rows |
| `/messaging` | environment, cluster, topic/group, cursor | AdminClient metadata cache; ≤50 topics/groups, lag age and partition coverage |

Unknown filters return 400; denied/unknown resources use consistent non-disclosing responses.
Permission filtering precedes pagination and aggregate counts. Errors are redacted.
All proposed APIs except the initial unfiltered `/topology` are future contracts, not implemented routes.

## Implementation sequence and acceptance

1. **This iteration:** add Java `/ops/v1/topology` projecting the existing bounded poller.
   Dependencies are DECLARED from `apex.ops03.owners` + `OwnerPoller` contract; observed
   instance presence comes only from `OwnerSnapshot` and is not observed interaction.
   Snapshot environment scopes authorization; missing environment is `unknown` and needs
   an explicit matching grant when resource auth is enabled. Return UNKNOWN health,
   retained freshness/conflicts, no inferred traffic. Zero owners yields an explicit empty
   inventory. Expose disabled provider capabilities. Reuse BFF client, DTO validation,
   shell/panel/badges; synthetic mode returns an empty, explicitly labelled demo graph.
   Test missing/stale/conflicting evidence, instance epochs, environment visibility,
   dangling-edge prevention, BFF auth/errors/mutations, validation and rendered UI.
2. Add versioned, bounded per-cell declared-contract introspection. Export sanitized
   effective `PublisherRouting`, subscriptions/groups, event registry and gRPC handler
   descriptors. Join publishers/consumers only through exact topic + contract matches.
   HandlerRegistry currently has no safe public topology DTO: add one without exposing
   handler objects/reflection. Add HTTP mappings from reviewed route manifests; resolve
   gateway routes separately. Compare manifests to source in contract tests.
3. Add OTel instrumentation/collector/trace backend. Use standard service.name,
   service.instance.id and deployment environment resources, HTTP/gRPC client/server spans,
   Kafka producer/consumer spans and W3C trace propagation. Persist creation context across
   outbox commits and link asynchronous/replay processing correctly. Derive OBSERVED edges
   only from spans in the selected window. Support span links and partial/sampled traces;
   do not infer success from consumer offsets. OTel Collector exports telemetry; a trace
   backend adapter supplies queries. Verify end-to-end across outbox and retry boundaries.
4. Add a distinct read-only Kafka AdminClient adapter: allowlisted clusters/topics/groups,
   describeTopics, describeConsumerGroups, listConsumerGroupOffsets and listOffsets.
   Broker metadata confirms topics/groups, not application semantics. Cache with ≤5s calls,
   partial failures, rate/concurrency budgets, read-only credentials and no create/reset APIs.
5. Add registry + Actuator/orchestrator runtime adapters with deployment identity,
   explicit freshness, health provenance, timeouts and redacted endpoints. No arbitrary URL
   fetches or browser access. Finish Cell/Service/Instance navigation and topic expansion.
6. Add trace waterfall and saga evidence overlays. Reuse recovery-test scenarios as
   future live acceptance, keeping fixture and real-stack receipts separate. No destructive
   controls, deployment or broader network exposure in this iteration.

References: [OTel messaging span/link semantics](https://github.com/open-telemetry/semantic-conventions/blob/main/docs/messaging/messaging-spans.md),
[Kafka Admin API](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/admin/KafkaAdminClient.html).

## Validation receipt

* Java 21 / Maven offline targeted reactor tests: `TopologyApiTest` and
  `OpsM36aHttpSecurityIT`: 8 tests passed. Includes authenticated grant filtering by
  snapshot environment, missing-environment denial, stale/clock/conflict evidence,
  declaration semantics and JWT endpoint gating.
* Frontend Vitest: 18 tests passed, including existing policy tests and new schema,
  rendered topology and BFF auth/error/mutation checks.
* ESLint and Next production build passed; the build includes TypeScript checking
  and both new routes. Next reports the existing middleware-to-proxy deprecation.
* Playwright fixture smoke: 4 passed, including topology labels/empty coverage,
  no browser requests to Console, existing Overview/Events behavior and mutation rejection.
* These are unit, HTTP security and synthetic browser checks. No live Kafka,
  orchestrator, OTel backend or multi-cell live acceptance was run. This slice reports
  their capabilities as NOT_CONFIGURED and does not claim full runtime discovery.

