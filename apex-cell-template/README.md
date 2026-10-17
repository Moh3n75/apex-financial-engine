# apex-cell-template

Empty, **runnable** APEX Cell foundation (M20).

## What this is

- An Independent Cell Application skeleton: Spring Boot entry point, Cell-owned DB config, empty Flyway location, UTF-8 JSON i18n resources, and a Cell-owned `Dockerfile`.
- Ready to be copied into a real Cell (`customer-cell`, `payment-cell`, …).

## What this is **not**

- No business use cases, controllers, entities, events, queries, or domain tables.
- No local `util` / `security` / `observability` / `sdk` / `deployment` / Helm / Kubernetes / Terraform.
- No custom `ObjectMapper`, exception handler, translation resolver, logging facade, auth filter, or messaging framework.
- Localization is **not** claimed to work until an external APEX translation resolver is verified and integrated.

```text
Cell            = Business Behavior (future modules under com.apex.cell.modules.*)
APEX Util       = Standard Technical Behavior   (PENDING)
APEX SDK        = Platform Communication Behavior (PENDING)
APEX Foundation = Runtime / infrastructure        (PENDING / external)
```

## Rename when creating a new Cell

1. Copy this directory (or repo).
2. Change Maven `artifactId`, `<name>`, and `spring.application.name`.
3. Change base package from `com.apex.cell` if desired (keep `modules.<name>.<layer>` convention).
4. Point `CELL_DB_*` at a **dedicated** database for that Cell only.
5. Add business packages under `com.apex.cell.modules.<module>.{api,application,domain,infrastructure,events,queries}`.

## Build and verify

```bash
mvn clean verify
```

Default verify runs resource/bootstrap tests. Database integration tests run only when Docker is available (Testcontainers); otherwise they are skipped.

## Start the application

Requires a reachable PostgreSQL database:

```bash
# example temporary Postgres (host tooling; not part of this Cell repo)
docker run --name apex-cell-pg -e POSTGRES_DB=apex_cell -e POSTGRES_USER=apex -e POSTGRES_PASSWORD=apex -p 5432:5432 -d postgres:16-alpine

set CELL_DB_URL=jdbc:postgresql://localhost:5432/apex_cell
set CELL_DB_USERNAME=apex
set CELL_DB_PASSWORD=apex

mvn spring-boot:run
```

Health check:

```text
GET http://localhost:8080/actuator/health
```

Expected when DB is reachable: HTTP 200 / `{"status":"UP"}`.  
If PostgreSQL is absent, a failing DB health check is **correct** behavior.

### Database environment variables

| Variable | Purpose | Local default |
|----------|---------|---------------|
| `CELL_DB_URL` | JDBC URL for **this Cell's** database | `jdbc:postgresql://localhost:5432/apex_cell` |
| `CELL_DB_USERNAME` | DB user | `apex` |
| `CELL_DB_PASSWORD` | DB password | `apex` |
| `SERVER_PORT` | HTTP port | `8080` |

Do not commit real credentials. Never access another Cell's database.

### DB integration check (Testcontainers)

```bash
mvn -Dtest=DatabaseIntegrationTest test
```

Requires Docker. Performs `SELECT 1`, confirms Flyway history without domain tables, and asserts Actuator health UP.

## Docker image (Cell-owned)

Build the JAR first (Docker build does not run Maven or fetch unpublished APEX packages):

```bash
mvn -DskipTests package
docker build -t apex-cell-template:local .
```

Run against an **external** PostgreSQL. From another container, `localhost` is the container itself — use the host gateway / compose service name / reachable hostname:

```bash
docker run --rm -p 8080:8080 ^
  -e CELL_DB_URL=jdbc:postgresql://host.docker.internal:5432/apex_cell ^
  -e CELL_DB_USERNAME=apex ^
  -e CELL_DB_PASSWORD=apex ^
  apex-cell-template:local
```

Then: `GET http://localhost:8080/actuator/health`

The image runs as a non-root user and does not embed secrets.

## Future business module layout

Java packages only (not Maven modules):

```text
com.apex.cell.modules.<module>.api
com.apex.cell.modules.<module>.application
com.apex.cell.modules.<module>.domain
com.apex.cell.modules.<module>.infrastructure
com.apex.cell.modules.<module>.events
com.apex.cell.modules.<module>.queries
```

Add versioned Flyway scripts under `src/main/resources/db/migration` for **this Cell's** schema only.

## JSON translations

Cell owns content under:

```text
src/main/resources/i18n/messages_fa.json
src/main/resources/i18n/messages_en.json
```

Template files are empty objects `{}`. Future example (documentation only — do not put sample business keys in the empty template):

```json
{
  "customer.error.notFound": "مشتری پیدا نشد"
}
```

Store Persian as UTF-8. Platform resolves translations; this Cell must not ship a competing JSON loader.

## Platform libraries

| Library | Status | Notes |
|---------|--------|-------|
| Spring Boot 4.1.1 / Java 21 | **Connected** | Parent BOM in this `pom.xml` |
| JDBC + Flyway + PostgreSQL driver | **Connected** | Cell-owned DB wiring |
| Actuator health | **Connected** | `/actuator/health` |
| `apex-util` | **PENDING** | Not published / not available as verified Maven coordinates in this environment |
| `apex-observability` | **PENDING** | Same |
| `apex-security` | **PENDING** | Same |
| `apex-cell-sdk` | **PENDING** | Same |
| Workspace `apex-platform-*` modules | **Not wired** | Exist in `apex-financial-engine` multi-module build only; not independent published Cell platform jars for this template |

## Why deployment stays outside

Kubernetes, Helm, Terraform, and CI/CD promotion belong to `apex-deployment-platform`. This repository owns only the application image definition (`Dockerfile`).
