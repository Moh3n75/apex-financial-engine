# Inventory Cell — M26.4 scaffold

Independent Java 21 / Spring Boot 4.1.1 application, using APEX 1.0-SNAPSHOT dependency-only Starters. Owns its PostgreSQL database. It does not depend on Order business classes or read Order's database.

This increment provides bootstrap, portable configuration, and schema only. There is no reservation use case, stock mutation endpoint, event handler, business event producer, seed data, or Saga behavior. HTTP currently returns 404 for `/`; there is no health/inspection endpoint yet.

## Build and isolated verification

From the repository root, with Java 21 and Maven available:

```powershell
mvn -o -pl apex-starter-webmvc,apex-starter-cell-messaging -am clean install -DskipTests
mvn -o -f inventory-cell/pom.xml clean verify
```

Remove `-o` if dependencies have not yet been downloaded. The standalone Cell uses the installed Platform artifacts; it is intentionally outside the root reactor. The executable artifact is `inventory-cell/target/inventory-cell.jar`.

The three integration tests launch real disposable embedded PostgreSQL and Kafka on dynamically assigned ports, explicitly create both topics with broker auto-creation disabled, start the actual Boot application on loopback with a random HTTP port, and close the application, broker and database in that order. Tests remove OS/system property sources and override connection settings; no shared Kafka or database is needed. Tests verify Boot discovery, both APEX messaging roles, paused subscription, HTTP startup, Flyway ordering/revalidation, and database constraints. They do not prove reservation behavior or actual event consumption/publication.

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

`spring.application.name` and `apex.cell.id` are `inventory-cell`. Both APEX Consumer infrastructure and Outbox publication are enabled. Outbox append remains separate from publication; no application events are appended in this scaffold.

**Listener safety guard:** `spring.kafka.listener.auto-startup=false` is deliberate. The existing Platform consumer ACKs unknown/unhandled events. With no `ORDER_PLACED` handler yet, starting consumption could discard events for this group. Consumer Beans and subscription are registered, but containers stay stopped. Keep this guard until the next increment supplies and verifies the handler; do not override it merely to make the process look active. No offsets are reset by this project.

String serializers/deserializers, producer `acks=all` and idempotence, consumer manual immediate ACK with auto-commit disabled, and `earliest` initial offset match the current Platform transport. KafkaAdmin auto-creation and consumer topic auto-creation are disabled. Configure authentication/TLS with standard Spring Kafka properties if needed. Do not rename the group/topics casually after processing begins.

## Database ownership and migrations

`spring.flyway.locations=classpath:db/migration,classpath:db/platform` installs Platform `V1001` (Outbox), `V1002` (Inbox), then Inventory `V2000__create_inventory_tables.sql` into Inventory's own database. No applied migration is edited and out-of-order migration is not enabled. V2000 has no collision in the current artifacts and is suitable for a fresh Inventory database. Future migrations should be greater than the highest applied version, with Platform version coordination; this is not a fix for other Cells' existing V1/V2 upgrade ordering.

Inventory tables: `inventory_stock` (UUID product PK, nonnegative available/reserved counts, reserved default zero); `inventory_reservations` (UUID order PK, required creation timestamp and status limited to RESERVED/REJECTED/RELEASED); `inventory_reservation_items` (order/product composite PK, reservation FK, strictly positive quantity). The items table intentionally has no product FK, cascade, business state machine, or compensation guarantees beyond the approved schema.

Never point Inventory at Order's database. The test database is temporary; no existing Inventory deployment/Flyway history was inspected.

## Run Inventory and Order separately

Provision **two distinct databases and roles** outside this task. Supply connection values through your approved local environment/secret mechanism; do not copy existing YAML credentials or commit them. Shared broker/database provisioning requires separate authorization.

An infrastructure owner must provision `apex.order.events.v1` and `apex.inventory.events.v1` explicitly, selecting partitions, replication, retention and ACLs for the environment. For an authorized disposable broker, the Kafka CLI form is:

```powershell
kafka-topics.bat --bootstrap-server $env:KAFKA_BOOTSTRAP_SERVERS --create --topic apex.order.events.v1 --partitions 1 --replication-factor 1
kafka-topics.bat --bootstrap-server $env:KAFKA_BOOTSTRAP_SERVERS --create --topic apex.inventory.events.v1 --partitions 1 --replication-factor 1
```

One partition/replica is a disposable-development example, not a shared deployment recommendation. Do not run creation against shared infrastructure without approval. Inventory needs read/group permissions for the Order stream and eventual write permissions for its own stream; bootstrap TCP success alone does not establish advertised broker connectivity.

In terminal A, supply the four required Inventory variables listed above and launch:

```powershell
java -jar inventory-cell/target/inventory-cell.jar --server.port=8084
```

In terminal B, build Order independently (`mvn -o -f order-cell/pom.xml clean package -DskipTests`) and supply **Order's own database** and Kafka settings. The current Order YAML has user-local values; explicitly override `SPRING_DATASOURCE_URL`, `SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD`, and `SPRING_KAFKA_BOOTSTRAP_SERVERS` using that terminal's environment, then launch:

```powershell
java -jar order-cell/target/order-cell.jar --server.port=8083 --apex.messaging.publisher.topic=apex.order.events.v1
```

These are manual instructions; the scaffold task does not start either deployed process or provision shared resources. Inventory can start and install its schema, but its Kafka listener remains stopped. The next collaboratively designed increment must define an Inventory-owned compatible ORDER_PLACED V1 wire DTO, handler, reservation semantics, and failure tests before consumption is enabled.
