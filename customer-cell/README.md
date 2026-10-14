# customer-cell

Independent APEX **Customer Cell** (M21), created from the verified `apex-cell-template` (M20).

## What this Cell owns

- Customer module (`api` / `application` / `domain` / `infrastructure` / `queries`; `events` reserved for M22)
- PostgreSQL schema + Flyway (`customer_db`)
- UTF-8 JSON translation **content**
- Dockerfile (`apex/customer-cell:m21`)
- Spring Boot bootstrap on port **8081**

## What this Cell does **not** own

- Shared exception handler / ApiError contract (not published as a reusable library)
- JSON translation resolver / Accept-Language localization
- ECS logging, security, messaging, Outbox, Saga, Query Gateway
- Helm / Kubernetes / CI deployment

```text
POST /api/v1/customers
GET  /api/v1/customers/{id}
GET  /actuator/health
```

## Database

| Variable | Default (local only) |
|----------|----------------------|
| `CELL_DB_URL` | `jdbc:postgresql://localhost:5434/customer_db` |
| `CELL_DB_USERNAME` | `customer_user` |
| `CELL_DB_PASSWORD` | `local_dev_only` |
| `SERVER_PORT` | `8081` |

Example isolated Postgres:

```powershell
docker run -d --name apex-customer-db `
  -e POSTGRES_DB=customer_db `
  -e POSTGRES_USER=customer_user `
  -e POSTGRES_PASSWORD=local_dev_only `
  -p 5434:5432 postgres:16-alpine
```

## Build / test / run

```powershell
mvn clean verify
mvn spring-boot:run
```

Integration tests use Testcontainers when Docker is available; otherwise Embedded PostgreSQL (`io.zonky.test:embedded-postgres`) so DB/API checks still run under `mvn clean verify`.

## Docker image

```powershell
mvn -DskipTests package
docker build -t apex/customer-cell:m21 .

docker run --rm -p 8081:8081 `
  -e CELL_DB_URL=jdbc:postgresql://host.docker.internal:5434/customer_db `
  -e CELL_DB_USERNAME=customer_user `
  -e CELL_DB_PASSWORD=local_dev_only `
  -e SERVER_PORT=8081 `
  apex/customer-cell:m21
```

## Platform integration ledger

| Capability | Status |
|------------|--------|
| JDBC + Flyway + Actuator (M20 stack) | Connected |
| `apex-util` / translation resolver | **BLOCKED** — not published |
| Shared exception/ApiError library | **BLOCKED** — only credit-coupled handler in `apex-bootstrap` |
| `apex-observability` / ECS | **BLOCKED** |
| `apex-security` | **BLOCKED** |
| `apex-cell-sdk` / events | Deferred to **M22** (not implemented here) |

HTTP 404/409 use Spring `ResponseStatusException` with stable error **codes** (not Persian literals). Platform-standard error JSON body is **not** claimed.

## Template purity

`apex-cell-template` remains generic. Do not add Customer code there.
