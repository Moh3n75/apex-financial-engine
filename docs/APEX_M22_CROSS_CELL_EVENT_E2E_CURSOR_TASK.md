# APEX — M22: Real Cross-Cell Event Delivery (Customer → Reporting)

**Document type:** Executable implementation instructions for Cursor Agent  
**Milestone:** M22 — Publish and consume the first *real*, independently deployed cross-Cell event  
**Prerequisites:** M20 generic Cell Template and M21 independently running `customer-cell`, verified by the user  
**Outcome:** A successful `POST /api/v1/customers` atomically persists Customer + a durable Outbox event; the event reaches Kafka through existing APEX Platform messaging; an **independent** `reporting-cell` consumes it exactly once in business effect, stores a projection in **its own PostgreSQL database**, and exposes a read API. The two Cell processes can be started, stopped and deployed independently.  
**Status:** Task specification, **NOT** a claim that anything below has already been implemented.

## 0. Execute against existing code — don't hallucinate APIs

Read this specification **and inspect all real repositories** before editing. Locate:

1. Actual `customer-cell` implementation, package layout, create use case, Flyway migrations, transaction setup, DTOs, Dockerfile and tests from M21.
2. Actual APEX Platform source/artifacts (which might live in a separate workspace/repository): `apex-platform-core`, `apex-platform-events`, `apex-platform-messaging`, `apex-infrastructure`, existing Util/ObjectMapper/exception/observability artifacts, and any *real* Cell SDK. Inspect available versions and Maven coordinates.
3. The **real** event/outbox interfaces and SQL schema, publisher scheduler, Kafka publishing and topic selection, serialization/metadata handling, message consumers (if any), correlation propagation, and test infrastructure.
4. The M20 `apex-cell-template` and its verified configuration conventions.

**First create a concise Integration Ledger** in the final report: contract/interface → module/artifact → existing/not existing → action. Reuse real code; do not assume names such as `ApexEventPublisher`, `@EventHandler`, `ApexLogger` or any Maven artifact exist merely because prior architecture documents mentioned them. Do not introduce an `apex-cell-sdk` artifact solely to match a diagram if compatible Platform libraries already exist.

If the Platform source or published Maven dependencies are unavailable, identify the *exact blocking interfaces/dependencies*; do not fake event delivery using only in-memory events, an HTTP callback, a Kafka producer directly in the Cell, or a startup-only runner. Ask for the missing code/artifact rather than silently implementing a competing framework.

## 1. Non-negotiable architecture boundaries

- **A Cell owns business capability only**: its module's API, application, domain, persistence adapter, migrations, business event definitions/contracts, business-owned UTF-8 translations, minimal application bootstrap/wiring and its Dockerfile.
- Shared message envelope, Outbox mechanics, publisher scheduling, Kafka transport, consumer container, schema/version validation, retry, deduplication *infrastructure*, logging, trace propagation, security, exception/JSON/translation resolution belong in **existing APEX Platform libraries**. Keep Cell business handlers/ports and persistence for Cell-owned projection in the Cell. Platform may install infrastructure tables via reusable Flyway migrations **within each owning Cell DB**; never query another Cell DB.
- Both `customer-cell` and `reporting-cell` are independently runnable Spring Boot applications, each with its **own database** and **own Dockerfile**. Neither imports the other's internal modules, classes, repositories, or database schema. Business event contracts are public versioned message contracts, not imported Customer entity classes.
- Keep `apex-cell-template` business-neutral. Generic improvements belong in Platform and/or template only when genuinely reusable; do not copy a Kafka consumer framework or a publisher scheduler separately into each Cell.
- No cross-Cell transactions, no Saga, no Query Gateway, no security-platform reinvention, no new deployment manifests, no reporting data warehouse, no speculative modularization.
- Preserve M21 POST/GET behavior, DB constraints, translations, exception contracts and Docker bootability.
- Use Java 21 and the **versions actually in the repositories** (previous APEX baseline: Spring Boot 4.1.1, Maven, PostgreSQL/Flyway, Spring Kafka, jOOQ and Jackson 3 `tools.jackson.*`). Do not mix Jackson 2 and 3 casually or use obsolete Spring Boot 3 APIs.
- Do not use a fixed IP/port, hard-coded secret or a pre-existing demo financial topic as a dependency. All broker and topic locations must be configurable. Do not introduce a central per-event router or registry lookup in the hot path; Kafka native subscriptions handle delivery.

## 2. Precise business scenario and acceptance behavior

### Producer: `customer-cell`

Existing M21 contract remains:

`POST /api/v1/customers`

Request example:

```json
{"externalReference":"CUST-M22-0001","displayName":"مشتری آزمایشی"}
```

Successful response remains HTTP `201` with the persisted Customer ID. For each **new successful creation only**, enqueue exactly one `CUSTOMER_CREATED` **version 1** integration event **in the same local PostgreSQL transaction** as the Customer insert. A rejected, validation-failed or duplicate request creates **neither** a new Customer **nor** an Outbox event. The durable outbox entry is committed before HTTP 201 is returned; Kafka delivery can be asynchronous.

Customer event payload example (field choices should match actual M21 Customer model; do not invent unrelated attributes):

```json
{
  "customerId": "<UUID from actual committed customer>",
  "externalReference": "CUST-M22-0001",
  "displayName": "مشتری آزمایشی",
  "status": "ACTIVE",
  "createdAt": "<ISO-8601 instant from Customer>"
}
```

Envelope metadata **must use the actual APEX Platform `EventEnvelope` and `EventMetadata` contract**, preferably existing factory and execution context. Earlier APEX code has fields:

```text
eventId: UUID                 immutable per logical event
eventType: CUSTOMER_CREATED
eventVersion: 1
correlationId: UUID
causationId: UUID or null as contract permits
aggregateId: Customer UUID serialized as String
aggregateType: CUSTOMER
sourceService: customer-cell
cellId: stable configured Cell identity
occurredAt: Instant
```

**Inspect real current definitions.** Do not create incompatible duplicate envelope/metadata or assume outdated generic bounds. Construct and persist metadata **once** with the outbox transaction. Publisher reconstructs this **same** event ID, timestamp and correlation — never generate them again during retry/replay. New root correlation is acceptable when request has none; propagate an existing validated correlation if Platform context supports it.

### Transport: APEX Platform → Kafka

- Reuse the existing lease-based Outbox claim/mark-published/retry/Dead Letter behavior if it exists; **do not create a second Outbox implementation**. If the reusable publisher is currently wired only in a demo bootstrap, move/wire it through supported Platform configuration rather than copying implementation into Customer. A business Cell should configure properties, not own the publishing mechanism.
- The publisher **must run continuously while the application is alive**, not only on startup.
- Topic configurable; proposed dev topic: `apex.customer.events.v1`. This is a proposal, not a hard-coded requirement. Existing financial demo topic must continue to work. If the existing publisher hardcodes `FINANCIAL_EVENTS_TOPIC`, generalize only the necessary routing/config with backward compatibility and focused tests; do not globally rewrite messaging.
- Kafka message key is a stable Customer aggregate identifier (or namespaced `CUSTOMER:<UUID>`); equal aggregate events must preserve partition ordering where the broker's guarantees apply. Topic creation should be explicit in integration test/dev bootstrap, not silently assume auto-create in production.
- Publisher acknowledges Outbox only **after broker acknowledgment** (on a confirmed Kafka send); failures remain retryable through current retry/lease implementation.
- Transport is **at-least-once**. Do **not** claim exactly-once Kafka delivery.

### Consumer: `reporting-cell`

Create a new **separate** `reporting-cell` application using the verified generic M20 template (not by importing Customer source and not inside `customer-cell`). Have a single business `reporting` module with API/application/domain/infrastructure/events/queries only where actually needed. It owns a separate `reporting_db` and migrations. The sample reporting capability is deliberately minimal and serves as a cross-Cell proof.

Consume `CUSTOMER_CREATED` version 1 through the generic APEX consumer Platform adapter. Convert the message to a **reporting-owned event-facing contract/DTO or normalized Platform payload**; never import `Customer` domain entity or Customer internal DTO class. Event schema must be versioned, documented, and validated at the boundary; keep a machine-readable v1 contract/schema with the producer and consumer compatibility tests (use the repository's established contract tooling if present, otherwise a concise JSON Schema and test). Unknown event type or unsupported version must not be processed as a known event.

Create **Reporting-owned** schema, e.g.:

```text
reporting.customer_summary
  customer_id        UUID PRIMARY KEY
  external_reference VARCHAR(64) NOT NULL
  display_name       VARCHAR(150) NOT NULL
  status             VARCHAR(16) NOT NULL
  created_at         TIMESTAMPTZ NOT NULL
  projected_at       TIMESTAMPTZ NOT NULL

reporting.processed_event
  consumer_name      VARCHAR(...) NOT NULL
  event_id           UUID NOT NULL
  processed_at       TIMESTAMPTZ NOT NULL
  PRIMARY KEY (consumer_name, event_id)
```

Names/schema may follow actual team conventions; **all tables are in `reporting_db`, not Customer DB**. Generic dedup/inbox table schema, if Platform already defines one, must be reused instead of creating an incompatible parallel mechanism.

On receiving a valid event, perform **one local DB transaction**:

1. Atomically claim/deduplicate `(consumer identity, eventId)` via a DB unique constraint (`INSERT ... ON CONFLICT DO NOTHING` or a correctly equivalent pattern).
2. If already successfully processed, do not mutate the projection again; finish with a successful acknowledgment.
3. If new, insert/update the Customer read model according to v1 contract (for this M22 event, an insert with appropriate uniqueness handling suffices) and commit the inbox marker **within the same transaction**.
4. Only after commit, acknowledge/commit the Kafka offset using real Spring Kafka listener semantics and the actual configured container acknowledgment mode. **Avoid an ack that precedes DB commit**. On failure roll back the DB work and follow bounded retry/error handling; no silent swallow-and-ack of unprocessed events.

Reason about concurrency: two threads can receive the same event; the unique DB key (not an in-memory `Set`) must be the final arbiter. If the projection write fails, the inbox marker **must not** commit. This provides *exactly-once business outcome for a given eventId* under retries even though transport is at-least-once. Account for multiple consumer group members/restarts.

Expose small Reporting-local read endpoints to show the actual materialized projection, **not** an HTTP query to Customer:

- `GET /api/v1/reports/customers/{customerId}` → 200 with projected Customer data, or a standard 404 from verified Platform exception handling.
- Optional `GET /api/v1/reports/customers/count` → total distinct projected customers, if useful for manual duplicate proof. If an existing route clashes, use an explicit name that does not shadow the UUID route.

These are local Reporting endpoints for proof; do not build the APEX Query Gateway yet.

### Independent failure behavior

- Stop Reporting; create a Customer; its row and Outbox event must persist and Kafka must receive the event; restart Reporting and verify projection catches up, assuming topic retention covers downtime.
- Stop Kafka; create a Customer. If PostgreSQL is healthy, customer create should succeed and Outbox remain pending; restart Kafka, verify publisher drains backlog and Reporting eventually projects. **If actual current publisher has known broker startup coupling, diagnose/fix shared infrastructure** rather than misrepresenting this test.
- Re-deliver **the exact same envelope with identical `eventId`** (not a new event with the same Customer ID). It must not create duplicate projections or repeat business effects. Document a reproducible re-delivery method through a test fixture, controlled offset replay, or integration test; don't require operators to hand-edit records.
- Poison/unsupported events must receive a defined bounded policy (retry / dead letter / quarantine as supported by existing Platform); they must **not** be endlessly hot-looped or acknowledged as successful. Don't invent a whole new enterprise DLQ framework if Platform already has one; report a specific blocker if missing.

## 3. Implement in this order (small, end-to-end increments)

### M22-A — Integration audit and contracts

- Inspect M21 and Platform real sources. Write the Integration Ledger and determine exactly what is reusable.
- Agree on `CUSTOMER_CREATED` v1 payload, JSON schema, metadata and Kafka topic/consumer group naming.
- Define dependency direction so Customer and Reporting compile without importing one another.
- Identify how Platform messaging is made available to independently deployable Cells, not only to `apex-bootstrap`.

### M22-B — Customer atomic Outbox publication

- Add the Customer-owned event payload/contract under its `modules/customer/events` package and versioned event schema/resource.
- Invoke the verified Platform event factory/outbox port in the existing Customer create transaction; DO NOT publish directly to Kafka inside the DB transaction.
- Migrate only required Customer-owned DB schema or install reusable Platform Outbox migration into Customer DB, following existing migration/version conventions.
- Wire Platform's existing background publisher and broker topic config into the Customer runtime, using property/env registration. Avoid accidental broad component scanning across unrelated projects.
- Test insertion + outbox atomicity and runtime publication before moving on.

### M22-C — Generic reusable consumer platform capability

- Inspect existing Platform consumer capability. If present, use/finish it. If absent, implement the **minimum reusable consumer transport/dispatch, correlation/metadata validation, retry/ack integration and transactional inbox support in an existing appropriate Platform module**. Only add a new Maven artifact after a concrete reason and documented approval.
- Do not hardwire Customer or Reporting event names and payload types into the generic Platform handler registry. Use explicit registered handler/contract mapping without per-message calls to a centralized registry service.
- Keep listener infrastructure inside Platform, while event business handling and reporting persistence stay inside Reporting.
- A generic consumer adapter may call an application handler within a Platform-managed transaction around inbox + projection only if the actual DI and transaction boundary guarantees are demonstrated in tests.

### M22-D — Reporting Cell and read API

- Generate a separate Reporting application from M20 template, with its own Maven identity, Spring application name, HTTP port (e.g., 8082 if free), PostgreSQL database, Dockerfile and UTF-8 translation JSON resources owned by Reporting.
- Add only the Reporting business handler, projection model/repository and local GET endpoint(s).
- Add Flyway migrations for projection and inbox in Reporting DB; reuse generic inbox schema installation where available.
- Ensure neither Cell requires the other process to be running to start. Kafka and Postgres are external services; Reporting may need Kafka for active consumption but should not directly call Customer.

### M22-E — End-to-end verification and evidence

Build the Platform changes, then both Cell applications. Use environment-supplied broker, database and port configurations, e.g. those already established in M21; don't bake previous demo IP `10.10.1.57:29092` into code. Create or provision the event topic in the test environment.

Run real integration and smoke tests with Docker Compose or Testcontainers where supported; **deployment orchestration may reside in a central test/dev-infra workspace**, not committed as per-Cell deployment configuration. A Cell's `Dockerfile` stays inside that Cell.

## 4. Mandatory automated tests

Write runnable tests with assertions, not merely testing comments or empty mocks:

| Test | Verifiable assertion |
|---|---|
| Customer create success | One Customer row and exactly one durable outbox event in same committed transaction |
| Customer create rollback | Forced local transaction failure produces neither Customer nor Outbox entry |
| Customer duplicate rejection | HTTP 409, original row unchanged, **zero** additional outbox events |
| Envelope invariant | Persisted vs broker metadata has same `eventId`, `correlationId`, `occurredAt`, event version and aggregate identifier |
| Publishing while running | Event arrives after POST without restarting Customer |
| Reporting projection | Kafka event results in one row in `reporting_db` with exact Customer id and unchanged UTF-8 Persian display name |
| No cross-Cell DB dependency | Reporting works with **no** connectivity/credentials to Customer DB and Customer works without Reporting DB |
| Duplicate delivery | Replayed identical eventId yields one processed-inbox marker and one projection, with unchanged effect/count |
| Concurrent duplicates | Competing handlers for identical eventId still yield a single business effect due to DB uniqueness |
| Failed projection rollback | A forced projection failure rolls back inbox marker and permits a later successful retry |
| Consumer restart | Message published while Reporting is offline becomes visible after restart |
| Broker outage | Customer transaction succeeds and pending Outbox drains after broker recovery |
| Unsupported event/version | Safely rejected to existing configured error policy; **not** applied to read model |
| Two replicas (if environment permits) | Same group and same event don't double-apply projection; no false exactly-once claims |
| Build and Docker | Both independently package and start, with healthy endpoints and separate database identities |

Where one test is impossible due to missing environment/platform feature, clearly mark `BLOCKED` or `NOT RUN`, with command, dependency and why; do not call M22 complete. Prefer Testcontainers PostgreSQL + Kafka where tooling and Docker are available; skip no tests silently.

## 5. Manual API and DB smoke-test checklist

Use the *actual* configured ports and input DTOs. The following illustrates an expected local setup only, not a claim about the current code:

```powershell
# In customer-cell repo, with its own DB + Kafka reachable:
mvn clean verify
mvn spring-boot:run

# In reporting-cell repo, with a DIFFERENT DB + same event broker:
mvn clean verify
mvn spring-boot:run
```

In another PowerShell console, save the following as `m22-customer.json` **encoded as UTF-8**:

```json
{"externalReference":"CUST-M22-0001","displayName":"مشتری آزمایشی"}
```

Then:

```powershell
curl.exe -i -X POST http://localhost:8081/api/v1/customers `
  -H 'Content-Type: application/json; charset=utf-8' `
  --data-binary '@m22-customer.json'

# Replace <UUID_FROM_CUSTOMER_POST> with the actual returned ID:
curl.exe -i http://localhost:8082/api/v1/reports/customers/<UUID_FROM_CUSTOMER_POST>
```

Expected: HTTP 201 from Customer and **eventually** HTTP 200 from Reporting (async, not guaranteed immediately). The Reporting response contains the same UUID and uncorrupted Persian display name.

Verify in distinct PostgreSQL connections:

```sql
-- Execute ONLY against customer_db (adjust actual table names):
SELECT COUNT(*) FROM customers WHERE id = '<UUID_FROM_CUSTOMER_POST>';
SELECT event_id, event_type, published_at, retry_count
FROM outbox_event
WHERE aggregate_id = '<UUID_FROM_CUSTOMER_POST>'
ORDER BY occurred_at DESC;

-- Execute ONLY against reporting_db (adjust actual schema names):
SELECT * FROM customer_summary WHERE customer_id = '<UUID_FROM_CUSTOMER_POST>';
SELECT consumer_name, event_id FROM processed_event ORDER BY processed_at DESC LIMIT 10;
```

Actual table/column names must be read from applied migrations before writing queries. **Do not claim the sample SQL is valid without inspecting the real schema.**

### Verify reporting downtime

1. Stop Reporting application.
2. Create a **new** Customer (new `externalReference`). Verify HTTP 201 and the durable Customer Outbox event.
3. Restart Reporting, wait for group consumption and query its projection.
4. Confirm exactly one projection of that Customer in Reporting DB.

### Verify broker downtime

1. Stop Kafka without stopping PostgreSQL or Customer.
2. Create another new Customer. Check HTTP 201, pending Outbox row, and **no** forced synchronous Kafka dependency in request path.
3. Restart Kafka and verify the same Outbox event ID eventually becomes published and projected.
4. Confirm retries did **not** generate different `eventId` values.

### Verify duplicate event

Use an integration test that sends/reprocesses identical v1 envelope with the same `eventId` or resets a *test consumer group's* offset (without damaging shared environments). Assert one inbox marker and one projection; log/measure a duplicate-suppression result from existing Platform telemetry if available.

## 6. Safety and operational constraints

- **Do not** automatically create events for every GET, and do not create a chain `CustomerCreated → Payment` without a meaningful business request/workflow.
- Do not turn local atomic `Customer + Outbox` persistence into distributed Saga; two-cell asynchronous propagation is enough for M22.
- Schema changes must be additive, safe and versioned. Do not break M21 clients or current financial demo event consumers.
- Incoming Kafka offsets and DB transaction behavior must be consistent: ack **after** successful DB commit, not before.
- Exactly-once **business result** is proven by transactional inbox/dedup in Reporting, not by claiming Kafka guarantees no duplicates.
- No static/global in-memory dedup; it fails on restart and concurrent replicas.
- No system-specific fixed service names or financial event topics hardcoded into generic Platform code. Use config and clearly owned event types.
- Use the existing APEX exception handling, object mapper and ECS observability; don't add a new local `ControllerAdvice`, mapper or logger to either Cell.
- Log event/correlation IDs for troubleshooting without exposing customer personal data or dumping raw payloads unnecessarily.
- No secrets committed. Do not modify infrastructure outside available authorized workspaces; supply precise reproducible steps if local Kafka or separate databases are missing.
- Do not create a Git commit or push unless the user separately requests it.

## 7. Definition of Done (DoD)

M22 is `DONE` **only if**:

1. Customer creation + Outbox persistence are truly atomic in `customer_db`, and an existing/shared scheduler publishes later with original metadata intact.
2. Kafka is actually used with a documented v1 contract and configurable topic.
3. Reporting is a separate runnable application and consumes through reusable Platform facilities, not a business-cell-local Kafka framework.
4. Reporting persists a projection and processed-message dedup state **in its own database**, with one local atomic transaction and acknowledgment after commit.
5. HTTP 201 from Customer → actual broker delivery → eventually HTTP 200 from Reporting is reproduced in a real test environment; Persian UTF-8 survives.
6. Duplicate redelivery, restart and at least one failure/recovery path are proven with test results and logs.
7. Both Cell Dockerfiles build and run, Maven tests pass or documented blockers are clearly called out. Generic template remains business-free; previous M21 tests still pass.

If unavailable artifacts or an environment blocker prevents one of these, finish the safe subset, write what is missing **precisely**, and report `PARTIAL` or `BLOCKED` instead of `DONE`.

## 8. Required Cursor final report

```text
M22 IMPLEMENTATION REPORT
1. Repository paths/commits used: Platform, customer-cell, reporting-cell, template
2. Integration Ledger: discovered real event/outbox/consumer SDK interfaces/artifacts
3. Files changed by repository, including any shared Platform code
4. Published v1 event contract, topic(s), group ID and envelope sample
5. DB migrations in EACH cell database, and inbox/outbox ownership
6. Proof Customer + Outbox atomicity; broker acknowledgment and retry behavior
7. Proof Reporting transactional idempotency and offset acknowledgment mode
8. Commands actually run and test results: PASS/FAIL/SKIPPED counts
9. Recorded example POST response, event metadata, Reporting GET response
10. Restart/outage/duplicate/unsupported-version test results
11. Docker build/runtime and Actuator checks for BOTH Cells
12. No cross-cell DB access, business-domain import, or shared library duplication
13. Specific missing pieces, limitations and final status: DONE/PARTIAL/BLOCKED
```

**Stop at M22.** Don't start Query Gateway, distributed Saga, performance tuning or large-scale projections in the same milestone.
