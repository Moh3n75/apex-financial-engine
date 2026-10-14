# APEX — M21: First Independent Business Cell (Customer)

**Document type:** Direct implementation instruction for Cursor Agent  
**Milestone:** M21 — Create and validate the first independently runnable business Cell  
**Input:** The **completed and verified** `apex-cell-template` from M20  
**Output:** A **separate** `customer-cell` application, a functioning Customer module, its own PostgreSQL database/migrations, its own UTF-8 JSON translations, and its own Dockerfile  
**Status:** This is a task specification, not evidence of implementation or successful tests.

## 0. Cursor: execute, do not merely explain

Read this entire document and inspect the real M20 template repository and any accessible APEX platform libraries before editing. **Implement the milestone in files, compile it, exercise the APIs, and report actual test evidence.** If the workspace contains only the generic template, create the Customer Cell in a **separate sibling directory/repository** (`../customer-cell`) rather than adding Customer business code to `apex-cell-template`. If the environment cannot create a sibling project, ask the user to open a new empty `customer-cell` repository with the verified M20 template copied into it; do not contaminate the generic template.

**Keep `apex-cell-template` generic and reusable.** The only permissible changes to the template are fixes to a clearly demonstrated *generic* defect; explain those separately.

### Architectural invariant

Each Cell owns **only** its business modules, business-facing APIs/contracts, persistence implementation and schema/migrations, translation **content**, necessary Spring Boot app/bootstrap/wiring, minimal runtime configuration, and its Dockerfile. Shared exception/JSON mapper/translation resolution/ECS log/tracing/security/messaging behaviors are **external platform libraries**. Central deployment/Helm/Kubernetes/pipelines remain outside the Cell.

Do not create new Maven modules, frameworks, shared `common/util`, `security`, `observability`, `sdk`, `deployment`, Kafka producer/consumer, a custom exception advice, a custom mapper/translator/logger, or made-up platform classes. A Java `modules/` directory is a **package organization**, not a Maven multi-module build.

## 1. First inspect and make an integration ledger

Before generating code, inspect the actual M20 `pom.xml`, package/application class, `application.yml`, Dockerfile, Flyway setup, tests and build command. Inspect **real** available APEX platform Maven artifacts/source and their APIs (in this or adjacent workspaces only if accessible). Identify exact versions, packages and APIs for:

- Existing APEX exception handling and error response format;
- Existing APEX object mapper (do not create another one);
- Existing JSON translation resolver, **if it actually exists**;
- Existing ECS logging/observability behavior, **if available**;
- Existing SDK/events — **record availability, but do not integrate messaging this milestone**.

Do not assume artifact coordinates such as `com.apex:apex-util` or invent methods like `ApexTranslation.get(...)`. If a library is unavailable, record `BLOCKED: external platform library` with the exact required artifact/API. Continue with nondependent parts but **never claim the affected integration is working**. Use known standard Spring/JDK behaviors only when they do not duplicate a platform contract. If the required exception conventions cannot be applied, leave localized error-contract acceptance pending and report this clearly.

Use the versions already verified by M20; expected baseline is Java 21 / Spring Boot 4.1.1 / Maven unless the inspected project differs legitimately.

## 2. Create a truly independent project

Copy the tested M20 template to `customer-cell`, keeping the generic template intact. Make project-specific changes:

- Unique Maven coordinates/artifact name for `customer-cell` (follow actual existing `groupId` conventions).
- Unique main class and Java package, e.g. `com.apex.customer.CustomerCellApplication` with normal package-scoped component scanning (do **not** use `scanBasePackages="com.apex"`).
- Unique `spring.application.name: customer-cell` and local HTTP port `8081` by default or equivalent environment-variable override; no collisions with existing APEX bootstrap on 8080. Docker's exposed/container port must agree with actual app configuration.
- Own `customer_db` DB and **its own DB credentials**, supplied through env vars, not hardcoded in production sources; configure JDBC + Flyway using verified M20 conventions.
- Rename Docker image artifact to `apex/customer-cell:m21` for local verification. This name is an example, not a required public registry.
- Keep single-Maven-application packaging and the existing container build standard. Do **not** copy compiled JARs, `target/`, credentials or `.git` history from the template.

### Target layout (actual group/package path may vary, but be consistent)

```text
customer-cell/
├── pom.xml
├── Dockerfile
├── .dockerignore
├── .gitignore
├── README.md
└── src/
    ├── main/
    │   ├── java/com/apex/customer/
    │   │   ├── CustomerCellApplication.java
    │   │   └── modules/customer/
    │   │       ├── api/                 # REST + request/response contracts
    │   │       ├── application/         # Create customer use case
    │   │       ├── domain/              # Customer, business invariants, repository port
    │   │       ├── infrastructure/      # Actual PostgreSQL repository adapter
    │   │       ├── events/              # Reserved for M22; may be empty
    │   │       └── queries/             # Get customer use case/read access
    │   └── resources/
    │       ├── application.yml
    │       ├── db/migration/V1__create_customers.sql
    │       └── i18n/
    │           ├── messages_fa.json
    │           └── messages_en.json
    └── test/java/com/apex/customer/...
```

Keep business logic contained within `modules.customer`. Do not create `address`, `verification`, `payment` or other speculative modules. Don't create empty interface layers purely to fill directories.

## 3. Implement one focused, real business capability

For this **reference Cell**, implement exactly these two working HTTP endpoints, with owned persistence:

### POST `/api/v1/customers`

Request example:

```json
{
  "externalReference": "CUST-10001",
  "displayName": "مشتری آزمایشی"
}
```

Expected success: `201 Created`, optionally with `Location: /api/v1/customers/{id}`. Response contains the server-generated `id` (UUID), `externalReference`, `displayName`, `status` (`ACTIVE`), `createdAt` (ISO-8601 instant). API field names and payload schema are stable. Do not include a transport-specific event envelope in the REST response.

### GET `/api/v1/customers/{id}`

Expected success: `200 OK` with exactly the same representation; an absent UUID yields a correctly standardized `404` response **through the verified platform exception handling**. Invalid UUID -> `400` by validated API boundary.

### Minimal reference-domain rules

- `externalReference` is required, trimmed, 1–64 characters, and unique **within the Customer Cell**. Enforce uniqueness in the DB, not only via a pre-read (concurrency race).
- `displayName` is required, trimmed, 1–150 characters. Persian characters are accepted unchanged as UTF-8.
- `id` is generated once as UUID; `createdAt` once using injected `Clock` or verified platform time abstraction, not by scattered system-clock calls.
- Newly created customers have status `ACTIVE`; do not implement unrequested lifecycle transitions.
- The create operation persists once in a real DB transaction. Distinguish true duplicates (`409 Conflict`, not fake idempotent success) from internal failures; map duplicate detection using the verified platform error contract where available.
- The implementation must be layered: REST/controller input translation -> application use case -> domain model/port -> PostgreSQL adapter. Domain classes **do not** import Spring, jOOQ, JPA, HTTP, Kafka or JDBC.
- Use the M20-verified persistence stack (prefer jOOQ if already standardized in the APEX workspace). Do **not** add JPA just because it is easier, or build a new repository abstraction framework.
- API controller must not contain SQL/domain logic. Read query logic should be explicit and simple, not an invented CQRS framework.

This Customer model is a small **testing fixture**, not the final specification of a real company's Customer bounded context. Avoid real personally identifiable customer data in tests.

## 4. PostgreSQL and Flyway

Create and test a real Flyway V1 migration for the Customer Cell **only**. Use ordinary PostgreSQL-friendly names and types:

```text
customers
  id                 UUID PRIMARY KEY
  external_reference VARCHAR(64) NOT NULL UNIQUE
  display_name       VARCHAR(150) NOT NULL
  status             VARCHAR(16) NOT NULL
  created_at         TIMESTAMPTZ NOT NULL
```

Implement the constraints and indexes that correspond to this actual use case. Prefer a `CHECK` constraint only if it matches the implemented status model, not a speculative enum. Database timestamps represent instants and serialize as ISO-8601 in APIs. Do not create `customer_db` inside the migration (provisioning is external), and do not add cross-Cell tables, event schemas, an invented `outbox` or Saga tables yet.

Tests must prove persistence survives application restart and that uniqueness really holds under concurrent or repeated inserts. Do not wire a repository against the DB of `apex-bootstrap` or any other Cell.

## 5. JSON translations: owned here, resolved externally

Place **real reference-domain translation keys and translated strings** in both Cell-owned UTF-8 files:

`src/main/resources/i18n/messages_fa.json`

```json
{
  "customer.error.notFound": "مشتری پیدا نشد",
  "customer.error.duplicateReference": "شناسه مرجع مشتری تکراری است",
  "customer.validation.externalReference.required": "شناسه مرجع مشتری الزامی است",
  "customer.validation.displayName.required": "نام مشتری الزامی است"
}
```

`src/main/resources/i18n/messages_en.json`

```json
{
  "customer.error.notFound": "Customer not found",
  "customer.error.duplicateReference": "Customer external reference already exists",
  "customer.validation.externalReference.required": "Customer external reference is required",
  "customer.validation.displayName.required": "Customer display name is required"
}
```

Use stable error **codes/translation keys**, not Persian literals in Java business exceptions. Persian text stays native UTF-8; no `\uXXXX` escape conversion. The Cell owns key names/values, **not** resource loading, language fallback, locale resolution, or a JSON translator implementation.

If a verified APEX translation resolver accepts these resources, connect it using its supported mechanism and test language selection, e.g. `Accept-Language: fa` and `Accept-Language: en`. **If none exists, do NOT implement it in this Cell or use a fake properties bridge:** report `BLOCKED: Platform JSON translation resolver`. Independently validate JSON content and UTF-8 in tests; mark HTTP localization unverified rather than asserting it works.

Do not use local language-dependent text as persistent DB error data, message IDs, event type names or logging identity fields.

## 6. Platform behavior and observability

- Reuse **verified existing** shared exceptions, error response structure and global handler; never add per-Cell `@ControllerAdvice` or new error JSON standard.
- Reuse existing object mapping / serialization conventions. Avoid `new ObjectMapper()` and custom global mapper configuration.
- Reuse ECS-compliant logging and correlation context facilities where the actual artifact/API exists. Do not invent an `ApexLogger` wrapper. Logging `trace.id` / `correlation.id` should be handled using the available platform instrumentation, not duplicate filters.
- Check `/actuator/health` and existing DB health behavior.
- Do not implement authorization/security in the Cell. If central security is not yet available for this runnable reference fixture, document that limitation; do not claim the endpoint is production-secure.
- No Kafka clients, Event Publisher, Outbox/Saga, event consumer, query gateway, choreography or distributed transactions in M21. M22 will implement and test event transport and delivery with verified APEX platform contracts.

## 7. Mandatory tests (real execution, not sample output)

Add focused automated unit + DB integration + HTTP tests. Use Testcontainers PostgreSQL if it is already supported and Docker is available; otherwise test against a dedicated local DB with explicitly controlled, documented setup. Don't make plain `mvn clean verify` silently pass by skipping all meaningful DB tests and then call the milestone complete.

Required checks:

1. Domain unit tests for valid creation, blank/oversized `externalReference`, blank/oversized `displayName`, Persian UTF-8 value preservation.
2. `mvn clean verify` produces `BUILD SUCCESS` and an executable Spring Boot JAR; report which tests were run versus skipped.
3. Flyway starts from an empty **customer-only database** and creates the expected `customers` table.
4. `POST /api/v1/customers` returns 201 and the DB has exactly one corresponding row.
5. `GET /api/v1/customers/{id}` returns 200 with the committed entity.
6. Repeating POST with an existing `externalReference` yields the **platform-standard** `409` response (if shared error handling is available), with no extra row. If shared error contract unavailable, mark this acceptance item BLOCKED.
7. Unknown customer UUID -> standard 404 (or BLOCKED if handler is unavailable); malformed UUID -> 400.
8. Reboot the app; GET still succeeds: the response was not from an in-memory repository.
9. JSON translation files parse correctly and preserve Persian UTF-8; run real `fa`/`en` HTTP localization tests **only if** Platform resolver is integrated, otherwise explicitly mark BLOCKED.
10. Docker: build the Cell-owned image; run it with an externally supplied Customer DB; GET `/actuator/health` returns `UP`.
11. Architecture verification: generic template remains free of business code, Customer Cell has no locally implemented platform util/security/observability/SDK/deployment components, and no cross-Cell DB use.

### Suggested manual smoke test commands (PowerShell)

Adjust environment variable names and ports to the actual verified M20 configuration; do not blindly assume these exact defaults.

```powershell
# Example isolated DB, using a dedicated local port to avoid collisions:
docker run -d --name apex-customer-db `
  -e POSTGRES_DB=customer_db `
  -e POSTGRES_USER=customer_user `
  -e POSTGRES_PASSWORD=local_dev_only `
  -p 5434:5432 postgres:16

$env:CELL_DB_URL='jdbc:postgresql://localhost:5434/customer_db'
$env:CELL_DB_USERNAME='customer_user'
$env:CELL_DB_PASSWORD='local_dev_only'

mvn clean verify
mvn spring-boot:run
```

In **another** PowerShell shell, once started:

```powershell
curl.exe -i http://localhost:8081/actuator/health

curl.exe -i -X POST http://localhost:8081/api/v1/customers `
  -H 'Content-Type: application/json; charset=utf-8' `
  --data-binary '@customer-create.json'

# customer-create.json is a UTF-8 file containing:
# {"externalReference":"CUST-10001","displayName":"مشتری آزمایشی"}

# Replace <UUID_FROM_POST_RESPONSE> below with the actual ID:
curl.exe -i http://localhost:8081/api/v1/customers/<UUID_FROM_POST_RESPONSE>
```

For Docker runtime, the database hostname from inside the container is **not** localhost; use `host.docker.internal` on supported Docker Desktop environments or a user-defined Docker network and DB container name elsewhere. Use the exact environment variable names actually configured. Verify the Dockerfile's `EXPOSE` and Spring HTTP port match the chosen values.

## 8. Definition of Done

**M21 is DONE only when** the separate Customer Cell builds and runs, the schema/migration works in its own DB, POST/GET persist real data, negative validation/duplicate/not-found behaviors meet the available verified shared error contract, tests document real results, the Docker image starts, and the generic template stays clean. If platform localization or exception artifacts are unavailable, label the specific criteria **BLOCKED** and do not report full completion.

**Do NOT proceed to event messaging/Saga/Query Gateway in the same change.** The next milestone (M22) can add `CustomerCreated.v1` through the actual APEX event/outbox path and prove independent Reporting Cell consumption. In M21, do not create a dummy event publisher or claim events are delivered.

### Required final report from Cursor

Provide these sections, backed by actual repository changes and logs:

```text
M21 IMPLEMENTATION REPORT
1. Source template commit/version and customer-cell location
2. New/changed files (and why)
3. Actual existing APEX artifact dependencies found and used
4. API contracts implemented
5. Database migration, DB name, connection configuration
6. Automated test names and PASS/FAIL/SKIPPED counts
7. Real HTTP examples and returned status/payloads (redact credentials)
8. Maven build / application startup / Docker build+run results
9. Translation/exception/ECS integration: VERIFIED or BLOCKED, with reasons
10. Remaining issues and final status: DONE / PARTIAL / BLOCKED
```

Do not create a Git commit or push unless the user specifically instructs you to. Do not claim successful tests which have not been run.
