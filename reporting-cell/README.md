# reporting-cell

Independent Reporting Cell (M22). Consumes `CUSTOMER_CREATED` v1 from Kafka via APEX Platform messaging and projects into **its own** `reporting_db`.

## M23 dependencies

Reporting uses `apex-starter-cell-messaging` at the configured `apex.platform.version`.
Its existing Boot WebMVC dependency remains for reporting HTTP endpoints; the Messaging
Starter supplies no WebMVC dependency or HTTP filter. JDBC, Flyway/PostgreSQL, validation
and Actuator remain explicit application choices. No APEX infrastructure Beans are duplicated.

## APIs

- `GET /api/v1/reports/customers/{customerId}`
- `GET /api/v1/reports/customers/count`
- `GET /actuator/health`

## Config

| Variable | Default |
|----------|---------|
| `CELL_DB_URL` | `jdbc:postgresql://localhost:5432/reporting_db` |
| `KAFKA_BOOTSTRAP_SERVERS` | Supply explicitly for the runtime; do not rely on workstation fallbacks |
| `APEX_CUSTOMER_EVENTS_TOPIC` | `apex.customer.events.v1` |
| `SERVER_PORT` | `8082` |

Does **not** connect to `customer_db`.

Supply `CELL_DB_USERNAME` and `CELL_DB_PASSWORD` locally. Do not commit database
passwords or machine-specific broker endpoints. The Customer stream must be
provisioned explicitly; deployment client/broker auto-creation settings must not
replace infrastructure provisioning. Both Cells must receive the same
`APEX_CUSTOMER_EVENTS_TOPIC` if overriding the default. Keep the existing
`APEX_REPORTING_CONSUMER_GROUP` identity when retaining consumer offsets.

## Isolated verification

Run from the repository root with Java 21:

```powershell
./tools/m22/verify-cells.ps1
```

Use `-MavenCommand <launcher>` if Maven is not on PATH, or `-Offline` when all
dependencies are cached. This builds both Starters, their Platform dependencies and Customer from source before
Reporting verification and writes an ignored SHA-256 receipt. E2E refuses a
missing receipt, changed source/JAR or a different Platform JAR loaded by
Reporting. Rebuild with the script after changing producer inputs; merely having
`customer-cell.jar` present is insufficient. The Maven basedir sets the repository
root, so `mvn -f reporting-cell/pom.xml test` also resolves it correctly after a
successful prerequisite build.

`CustomerReportingE2ETest` uses a packaged Customer process and the real Reporting
Boot context, separate disposable PostgreSQL instances and an explicitly created
embedded Kafka topic with broker auto-creation disabled. It verifies HTTP,
scheduled publication, listener delivery, metadata/UTF-8 preservation, duplicate
suppression, rollback/retry, offset sequencing and Customer database independence.
Inherited deployment settings are excluded from test contexts/child processes;
the verification script also sanitizes child-build environment overrides.
Applications close before their fixture databases, including failure cleanup.
Logs, receipts and all `target/` outputs stay local and ignored.
