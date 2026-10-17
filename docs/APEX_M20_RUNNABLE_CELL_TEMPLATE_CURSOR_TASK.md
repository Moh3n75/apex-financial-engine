# APEX — M20: Make the Empty Cell Template Runnable

**Document type:** Cursor Agent implementation task  
**Scope:** Modify the **existing** `apex-cell-template` repository in place.  
**Status:** Implementation requested; this document is **not** evidence the repository has been modified or tested.  
**Baseline:** APEX Cell Template Architecture v4; Java 21, Spring Boot 4.1.1 and Maven as used in the current APEX workspace, unless the actual repository conclusively specifies an already-compatible managed version.

---

## 1. The instruction to Cursor

Read this file and the current repository before editing anything. **Implement M20 directly in the open `apex-cell-template` workspace.** Do not merely suggest architecture or generate another specification. At the end, run available verification commands and report exact pass/fail results, modified files, and any missing external dependencies.

This repository currently contains only an **empty template**. Treat all business-related and cross-cell platform capabilities as **not implemented in this repository**. Do not claim that any work beyond the empty template is complete.

### Non-negotiable architecture

A Cell independently owns its:

1. Business modules, domain models, application use cases, API contracts, business event definitions, business adapters/repositories.
2. Database schema and migrations, and connection configuration.
3. UTF-8 JSON translation resources and translation keys.
4. `Dockerfile` defining the image for this application.

**Everything else is owned by the external APEX Platform:** standardized exceptions, object mapping, translations resolution, ECS logging and tracing, security, messaging transport, SDK APIs, infrastructure configuration, deployment orchestration, Helm, Kubernetes, and CI/CD deployment logic. An application entry point, Maven metadata and minimal runtime configuration are necessary wiring, not reusable platform implementations.

Do **not** create within this Cell: `util`, `common`, `shared`, `security`, `observability`, `logging`, `platform`, `sdk`, `deployment`, `kubernetes`, `helm`, or `terraform` implementations. Do **not** introduce wrappers for features already supplied by existing APEX libraries. In particular do not create a custom `ObjectMapper`, exception handler, translation resolver, logging facade, authentication filter, messaging framework, or a fake SDK.

This milestone creates **zero business use cases, controllers, entities, events, queries, or tables**.

---

## 2. Inspect before implementation

First inspect:

- The current `pom.xml`, `src/**`, Maven wrapper, `.gitignore` and Dockerfile (if any).
- The real APEX platform module/artifact coordinates and versions **only if they are available in the current workspace or in accessible Maven metadata**. Existing examples in the wider platform may include `apex-platform-core`, `apex-platform-events`, `apex-platform-messaging`, and the user's already-existing utilities. Do not assume that a library named `apex-util`, `apex-observability`, `apex-security` or `apex-cell-sdk` is published or has any invented Java API.
- Real framework compatibility: Java 21, Spring Boot 4.1.1, Flyway/PostgreSQL, health endpoint, test dependencies, and packaging.

**Dependency rule:** Add an APEX dependency only when you can verify its exact `groupId`, `artifactId`, version and availability in the active build environment. Do not fabricate new Maven artifacts or implement duplicate platform behavior to bypass a missing artifact. Record unresolvable integration as **PENDING: external platform artifact**, while keeping the isolated template build functional wherever possible.

Use the existing parent/BOM/version management if valid. Never create an extra Maven submodule just to represent a business module.

---

## 3. Target file layout

Use standard Maven source conventions. The following is illustrative; preserve existing correct naming and package layout rather than needlessly renaming everything:

```text
apex-cell-template/
├── pom.xml
├── Dockerfile
├── .dockerignore
├── .gitignore
├── README.md
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/apex/cell/
│   │   │       ├── CellApplication.java
│   │   │       └── modules/                 # grouping for FUTURE business packages
│   │   │           └── .gitkeep
│   │   └── resources/
│   │       ├── application.yml
│   │       ├── db/migration/.gitkeep
│   │       └── i18n/
│   │           ├── messages_fa.json
│   │           └── messages_en.json
│   └── test/
│       └── java/com/apex/cell/             # focused tests, no domain fixtures
└── [mvnw, .mvn/ if already present or practical to add]
```

- Java source packages must live **under `src/main/java`**, not root-level `modules/` source trees.
- Use `com.apex.cell.modules.<business_module>.<layer>` as the eventual package convention (or an equally consistent repo-specific prefix); these are **Java packages, not independent Maven modules**.
- Keep the `modules/` grouping empty in the generic template. Empty directories may be tracked with `.gitkeep` where appropriate; no `Customer` / `Payment` / `ExampleBusiness` scaffolding.
- `README.md`, test resources, Maven, and minimal application bootstrap are expected development necessities and do not violate the Cell-purity rule.
- `application.yml` is allowed for **the Cell's own identity, DB connection settings, and standard Spring runtime flags**, not for reimplementing platform cross-cutting behavior.

---

## 4. Maven and runnable application

Implement the minimal buildable Spring Boot application:

- Java 21 and Spring Boot 4.1.1 unless the inspected APEX parent/BOM requires a compatible alternative; explain any deviation.
- One runnable bootstrapping class using `@SpringBootApplication` with a normal `main` entry point.
- No broad `scanBasePackages = "com.apex"` workaround: never accidentally scan every unrelated platform/Cell package. Use the app's own package and genuine platform auto-configuration where available.
- Add only justified Spring Boot dependencies for a runnable REST-ready app, Actuator health, JDBC/PostgreSQL/Flyway, and tests. If jOOQ is part of the verified APEX baseline, reuse its managed integration; do not create business repositories yet.
- Follow the actual Spring Boot 4 dependency/module and actuator package names; do not paste Boot 3-specific imports unverified.
- Ensure the application produces exactly one executable boot JAR in the expected artifact location.
- `GET /actuator/health` must be exposed for operational checks; no fake REST business endpoint is needed.

Note: a database-dependent application should not silently pretend to be healthy without a database. A genuinely failing DB health check is correct behavior if PostgreSQL is absent.

---

## 5. Cell-owned PostgreSQL schema and migrations

Prepare **configuration and infrastructure readiness**, not business data tables:

- PostgreSQL JDBC configuration driven by environment variables, with clearly documented local defaults if needed (e.g., `CELL_DB_URL`, `CELL_DB_USERNAME`, `CELL_DB_PASSWORD`). Do not commit actual credentials or production hostnames.
- Flyway migration location: `classpath:db/migration`.
- Versioned migrations will be owned by this Cell when real business modules are developed.
- Keep the migration directory empty for now or include a safe, explicitly documented no-op baseline *only if required by the actual Flyway setup*. Never invent a `customer` table, a generic cross-cell schema or a fake `outbox` table.
- Do not expose database credentials in logs or Docker layers.
- A future Cell must use its **own database / DB credentials**. No cross-Cell database access.

**Verification:** When a local PostgreSQL instance or Testcontainers is available, actually connect and execute a simple `SELECT 1` as a connectivity check; verify Flyway initializes cleanly without creating unrelated domain tables. Clearly mark this test unexecuted rather than claiming success if DB/Docker is unavailable.

**No repository-level Docker Compose, Kubernetes manifests or deployment charts** in this milestone: central deployment ownership remains outside Cells. A temporary testing-only PostgreSQL container command may be documented in README.

---

## 6. UTF-8 JSON translations: Cell owns content, Platform owns resolution

Create valid UTF-8 resources:

```text
src/main/resources/i18n/messages_fa.json
src/main/resources/i18n/messages_en.json
```

Initial content may be `{}` in both files because this is a **domain-free template**. For future business Cells, key ownership and values remain entirely within the Cell, e.g.:

```json
{
  "customer.error.notFound": "مشتری پیدا نشد"
}
```

That example belongs in the README, **not inside the empty template's resource files**.

Rules:

- Store Persian text directly as UTF-8 without Java properties Unicode-escape conversion.
- Choose namespaced business keys (`customer.error.notFound`, `payment.error.failed`), with a consistent future key policy.
- Exception messages should be based on stable error codes/keys; localization happens at presentation/response boundaries.
- JSON resources do not automatically become Spring `MessageSource` bundles. **Do not claim localization works until the existing external APEX translation resolver has been verified and integrated.**
- If the user's existing `apex-util` translation facility is accessible and supports JSON files, integrate the *real* existing API or auto-configuration. If unavailable or `.properties`-only, leave an explicit integration TODO in README; **do not create a competing JSON loader or resolver inside the Cell**.
- Tests can verify that both resources exist, are nonempty files, are syntactically valid JSON objects and decode as UTF-8; this is **resource validation**, not translation service implementation.

---

## 7. Dockerfile: image build only

Create or fix **one** Dockerfile in the Cell repository. It must:

- Run using Java 21.
- Copy and execute the Maven-built Spring Boot JAR; the build can occur in the standard external build pipeline (`mvn package` before `docker build`). Avoid fetching unpublished APEX packages in the Docker build stage.
- Use a non-root runtime user.
- Permit all DB/runtime values via environment variables; do not bake environment settings or secrets into the image.
- Expose the application's HTTP port (default 8080 if unchanged).
- Contain **no** Kubernetes manifests, deployment decisions, replica configuration, per-environment infrastructure or credentials.
- Add `.dockerignore` to prevent leaking `.git`, secrets, test artifacts and local build caches; ensure the final packaged JAR is **not** excluded.

Document the exact local commands for Maven build, image build, container startup with a reachable PostgreSQL endpoint, and `/actuator/health` verification. A container cannot connect to host `localhost` PostgreSQL by treating the host as its own `localhost`; document the environment-specific DB hostname explicitly.

---

## 8. Tests and observable acceptance criteria

Implement tests **focused on the bootstrap itself**, not business logic:

| Test | Expected result |
|---|---|
| `mvn clean verify` | Compiles, packages executable JAR, unit/resource tests pass. If Testcontainers is included, use a clear skip strategy when Docker is unavailable and report skipped DB tests honestly. |
| Spring Boot startup with reachable PostgreSQL | Application starts without errors and initializes Flyway cleanly. |
| `GET /actuator/health` | 200 / UP with all required dependencies healthy. |
| Resource validation | Both i18n JSON files are valid UTF-8 JSON objects. |
| Docker build | `docker build -t apex-cell-template:local .` succeeds **after** Maven packaging. |
| Docker startup | Container runs with externally supplied DB settings; health is correct. |
| Purity check | No business logic, locally reimplemented platform capabilities or deployment manifests. |

Avoid writing a `@SpringBootTest` that necessarily fails `mvn verify` merely because a developer does not have PostgreSQL running. Prefer pure unit/resource tests in the default build and an explicit opt-in DB integration test using Testcontainers or a verified local DB. Document and run the relevant commands available in the environment.

### Commands to execute/report where tooling is available

```bash
mvn clean verify
mvn spring-boot:run
# GET http://localhost:8080/actuator/health
mvn package

docker build -t apex-cell-template:local .
# docker run with external DB environment variables, then check health
```

Do not say tests passed if a command was not executed or an external service was unavailable.

---

## 9. README deliverables

The README must answer:

1. What the template is and explicitly what it **does not** implement.
2. How to rename package, artifact, application name and database when creating a new Cell.
3. How to run `mvn clean verify` and start the application.
4. Which DB environment variables are required, and how to run DB integration checks.
5. How to build and run the local Docker image.
6. How to add a future business module with `api/application/domain/infrastructure/events/queries` Java subpackages.
7. Where JSON translations go, with a real Persian UTF-8 example in documentation only.
8. Which platform libraries **actually connected**, which are **pending**, and the reason/required artifact for each.
9. Why deployment orchestration and all cross-cutting implementations stay outside the Cell.

---

## 10. Explicit exclusions: do not proceed to M21

Out of scope for this Cursor run:

- `customer-cell`, `reporting-cell`, `payment-cell`, or any domain classes.
- Customer endpoints, business DTOs, integration events and Kafka consumers.
- Outbox implementation, Saga, Query Gateway, CQRS, schemas for actual domain data.
- Building `apex-util`, `apex-observability`, `apex-security` or `apex-cell-sdk` from scratch.
- Creating extra Maven modules for every conceptual capability.
- Any deployment centralization work, Helm, Kubernetes, Terraform or GitHub Actions.
- Performance claims, 10k TPS targets or production-readiness assertions.

These are independent milestones and require separate approval and E2E tests.

---

## 11. Cursor final response format (mandatory)

After implementing, respond in Persian with:

**A. Files changed:** exact paths and one-line reason each.

**B. Validation results:** a table of command, executed/not executed, result and evidence (including failures and missing external dependencies).

**C. Platform compatibility:** which APEX util/SDK dependencies were truly connected and which remain pending; never invent artifact names or method signatures.

**D. M20 exit condition:** state `PASS`, `PARTIAL`, or `BLOCKED` with concrete reasons. Do not claim M20 complete until Maven startup, DB connectivity, health and Docker are proven in the target environment.

**E. Next step only after sign-off:** creating the first business Cell from this template (M21).

**Implementation instruction:** Start coding now. Inspect first, make minimal changes, run actual checks, and report evidence. Do not produce another architecture document in place of implementation.
