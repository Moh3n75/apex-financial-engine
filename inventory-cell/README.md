# Inventory Cell — Order reservation Saga stage

Independent Java 21 / Spring Boot 4.1.1 application, using APEX 1.0-SNAPSHOT dependency-only Starters. Owns its PostgreSQL database. It does not depend on Order business classes or read Order's database.

Consumes ORDER_PLACED V1 through the existing annotation/Inbox executor, invokes the transactional reservation use case, and appends INVENTORY_RESERVED or INVENTORY_RESERVATION_REJECTED V1 to its own Outbox in the same database transaction. HTTP still returns 404 for `/`; there is no stock mutation, seed/admin, or health endpoint. Payment, compensation and release are outside this increment.

## Build and isolated verification

From the repository root, with Java 21 and Maven available:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File inventory-cell/tools/verify-saga.ps1 -Offline
```

Remove `-o` if dependencies have not yet been downloaded. The standalone Cell uses the installed Platform artifacts; it is intentionally outside the root reactor. The executable artifact is `inventory-cell/target/inventory-cell.jar`.

The runner builds/installs the current Platform and Starters, runs `mvn -f order-cell/pom.xml clean verify`, writes a content-based build receipt under ignored `out/`, then runs `mvn -f inventory-cell/pom.xml clean verify`. Supply `-MavenCommand` if Maven is not on PATH. Do not reuse an old receipt after editing Order/Platform source. Plain Inventory `mvn test` runs its local domain/DB/wire tests; `verify` additionally runs `OrderInventorySagaIT` after packaging and requires a fresh Order build receipt.

Tests use disposable PostgreSQL and explicitly provisioned embedded Kafka topics with auto-creation disabled. The packaged E2E launches **two separate java -jar JVMs**, each with its own temporary PostgreSQL server, checks real HTTP/Kafka/Inbox/Outbox state, redelivery, business duplicates, rejection, rollback/offsets on commit failure, retry and Inventory restart with a queued order. Inherited application/JVM connection overrides are removed. Processes, application contexts and resources are closed on success and failure. Logs, JARs and generated evidence stay under ignored `target/` and `out/`.

## Configuration

Required environment variables, with **no connection/credential defaults**:

| Variable | Meaning |
|---|---|
| `INVENTORY_DB_URL` | PostgreSQL JDBC URL for a separately provisioned, Inventory-owned database |
| `INVENTORY_DB_USERNAME` | Inventory database role |
| `INVENTORY_DB_PASSWORD` | Role password; supply outside source control |
| `KAFKA_BOOTSTRAP_SERVERS` | Broker bootstrap endpoints reachable from this process |

Optional environment variables:

| Variable | Default |
|---|---|
| `SERVER_PORT` | `8084` |
| `APEX_INVENTORY_CONSUMER_GROUP` | `inventory-cell-order-placed-v1` |
| `APEX_ORDER_EVENTS_TOPIC` | `apex.order.events.v1` |
| `APEX_INVENTORY_EVENTS_TOPIC` | `apex.inventory.events.v1` |

`spring.application.name` and `apex.cell.id` are `inventory-cell`. Both APEX Consumer infrastructure and Outbox publication are enabled. Outbox append remains separate from publication.

**Opt-in listener activation:** Base configuration retains `spring.kafka.listener.auto-startup=false`. The `saga` profile enables it, validates the ORDER_PLACED V1 handler, and opts in to Platform reliable consumption. Source/DLT provisioning is checked before Kafka containers start. Do not enable a shared deployment without reviewing stream contents and existing group history. No offsets are reset.

In reliable Saga mode, malformed/unsupported events go to `apex.order.events.v1-dlt`; exhausted handler failures go there after bounded retries. Original offsets commit only after confirmed DLT publication. Missing/unavailable DLT leaves the source uncommitted and can block consumption. Legacy consumers that do not enable reliability retain their old risks. See [Platform reliability configuration](../apex-platform-messaging/README.md) for flags, operational prerequisites and duplicate-DLT/replay boundaries.

Same eventId is suppressed by Inbox. New eventId with identical order/items returns the original reservation result without a stock deduction; because the existing result API has no first-claim flag, it appends a new outcome. Order accepts an identical target status idempotently. A different request for the same order or an existing RELEASED reservation fails. No release or compensation algorithm is added.

String serializers/deserializers, producer `acks=all` and idempotence, consumer manual immediate ACK with auto-commit disabled, and `earliest` initial offset match the current Platform transport. KafkaAdmin auto-creation and consumer topic auto-creation are disabled. Configure authentication/TLS with standard Spring Kafka properties if needed. Do not rename the group/topics casually after processing begins.

## Database ownership and migrations

`spring.flyway.locations=classpath:db/migration,classpath:db/platform` installs Platform `V1001` (Outbox), `V1002` (Inbox), then Inventory `V2000__create_inventory_tables.sql` into Inventory's own database. No applied migration is edited and out-of-order migration is not enabled. V2000 has no collision in the current artifacts and is suitable for a fresh Inventory database. Future migrations should be greater than the highest applied version, with Platform version coordination; this is not a fix for other Cells' existing V1/V2 upgrade ordering.

Inventory tables: `inventory_stock` (UUID product PK, nonnegative available/reserved counts, reserved default zero); `inventory_reservations` (UUID order PK, required creation timestamp and status limited to RESERVED/REJECTED/RELEASED); `inventory_reservation_items` (order/product composite PK, reservation FK, strictly positive quantity). The items table intentionally has no product FK, cascade, business state machine, or compensation guarantees beyond the approved schema.

Never point Inventory at Order's database. The test database is temporary; no existing Inventory deployment/Flyway history was inspected.

## Run Inventory and Order separately

Provision **two distinct databases and roles** outside this task. Supply connection values through your approved local environment/secret mechanism; do not copy existing YAML credentials or commit them. Shared broker/database provisioning requires separate authorization.

An infrastructure owner must provision `apex.order.events.v1`, `apex.inventory.events.v1` and both corresponding `-dlt` topics explicitly, selecting partitions, replication, retention and ACLs for the environment. Each DLT needs at least its source partition count. For an authorized disposable broker, the Kafka CLI form is:

```powershell
kafka-topics.bat --bootstrap-server $env:KAFKA_BOOTSTRAP_SERVERS --create --topic apex.order.events.v1 --partitions 1 --replication-factor 1
kafka-topics.bat --bootstrap-server $env:KAFKA_BOOTSTRAP_SERVERS --create --topic apex.inventory.events.v1 --partitions 1 --replication-factor 1
kafka-topics.bat --bootstrap-server $env:KAFKA_BOOTSTRAP_SERVERS --create --topic apex.order.events.v1-dlt --partitions 1 --replication-factor 1
kafka-topics.bat --bootstrap-server $env:KAFKA_BOOTSTRAP_SERVERS --create --topic apex.inventory.events.v1-dlt --partitions 1 --replication-factor 1
```

One partition/replica is a disposable-development example, not a shared deployment recommendation. Do not run creation against shared infrastructure without approval. Inventory needs read/group permissions for the Order stream and eventual write permissions for its own stream; bootstrap TCP success alone does not establish advertised broker connectivity.

In terminal A, supply the four required Inventory variables listed above and launch:

```powershell
java -jar inventory-cell/target/inventory-cell.jar --spring.profiles.active=saga --server.port=8084
```

In terminal B, supply **Order's own database** through required `ORDER_DB_URL`, `ORDER_DB_USERNAME`, `ORDER_DB_PASSWORD`, and `KAFKA_BOOTSTRAP_SERVERS`. The opt-in Order saga profile requires these rather than falling back to user-local base YAML settings:

```powershell
java -jar order-cell/target/order-cell.jar --spring.profiles.active=saga --server.port=8083 --apex.messaging.publisher.topic=apex.order.events.v1
```

These commands target only separately authorized/provisioned resources; the verification runner always uses disposable resources. Order's independent group defaults to `order-cell-inventory-outcome-v1` (override with `APEX_ORDER_CONSUMER_GROUP`). Inventory's group remains as documented above. The groups and topics must not be renamed after processing without an explicit migration plan.

For a manual disposable demo, `tools/demo-stock.sql` seeds two fixed products directly in Inventory's database. It is never invoked by production startup. POST `/api/v1/order-placements` on Order with a fresh externalReference, description and items [{productId, quantity}]. The creation response is PENDING_INVENTORY. Poll GET `/api/v1/order-placements/{orderId}`: reservation success advances to PENDING_PAYMENT, insufficient stock to CANCELLED. No outcome advances to CONFIRMED. The E2E runner automates this demo with generated references and records actual PIDs and event IDs in `inventory-cell/target/saga-two-process-evidence.json`.

Wire contracts: incoming ORDER_PLACED V1 is {orderId, externalReference, items:[{productId,quantity}], placedAt}. Both outgoing payloads are {orderId}; metadata carries occurrence time. Aggregate type is INVENTORY_RESERVATION, aggregate ID orderId. Schema labels are `inventory.reserved.v1` and `inventory.reservation-rejected.v1`; these labels are contract catalog values, not new envelope fields. Correlation is preserved and the incoming eventId becomes the outcome causationId.
