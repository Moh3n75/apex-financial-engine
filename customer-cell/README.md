# customer-cell

Independent Customer Cell. M21 REST + M22 Outbox publication of `CUSTOMER_CREATED` v1.

## Event

- Type: `CUSTOMER_CREATED` / version `1`
- Topic (configurable): `apex.customer.events.v1`
- Atomic with Customer insert via Platform `OutboxAppender`

## Run

Set `KAFKA_BOOTSTRAP_SERVERS` to the broker addresses reachable from this runtime.
Supply database settings through `CELL_DB_URL`, `CELL_DB_USERNAME` and
`CELL_DB_PASSWORD`. Do not publish workstation connection settings or rely on
local YAML fallbacks as a portable deployment contract.

Provision `apex.customer.events.v1` through your infrastructure process before
using a shared broker. This application declares no topic-provisioning bean;
broker/client auto-creation policy is separate and must not replace provisioning.

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = '<reachable-broker>:<port>'
$env:CELL_DB_URL = '<customer-jdbc-url>'
$env:CELL_DB_USERNAME = '<customer-db-user>'
# Supply CELL_DB_PASSWORD locally; never put it in a committed command or file.
mvn spring-boot:run
```

Requires PostgreSQL (`customer_db`) and Kafka for live publishing.

## Isolated verification

From the repository root with `JAVA_HOME` pointing to Java 21:

```powershell
./tools/m22/verify-cells.ps1
# Add -Offline when all Maven dependencies are cached.
# Use -MavenCommand <maven-launcher> if Maven is not on PATH.
```

The script rebuilds Platform libraries, runs all Customer tests and packages
Customer, then runs all Reporting tests. Its content-based receipt is stored
under ignored `out/`. Direct Reporting E2E runs reject missing receipts, changed
Customer/Platform inputs and changed JARs. Re-run the script after such changes;
the receipt is build evidence, not a file to edit or commit.

Tests use disposable PostgreSQL/Kafka, pin their endpoints and discard inherited
deployment overrides. The script restores its caller's environment on exit.
It does not start the financial application or use the shared development broker.
