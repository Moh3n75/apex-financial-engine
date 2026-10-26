# Order Cell — M24 developer experience reference

An independent Java 21 / Spring Boot 4.1.1 Maven application using the existing M23 Starters.
It is intentionally outside the Platform root reactor, like Customer and Reporting.

`POST /api/v1/orders` accepts `externalReference` (1–64 nonblank characters) and
`description` (1–200 nonblank characters). It returns HTTP 201 with the Order UUID
and creation timestamp. The use case saves the Order and calls the existing
`CellEvents.append(OrderContracts.CREATED_V1, order.id(), payload)` in one PostgreSQL transaction.

The business event is `ORDER_CREATED` v1, aggregate `ORDER`, contract/schema label
`order.created.v1`. The configured reference stream is `apex.order.events.v1`.
There is no new handler API, CommandBus, routing abstraction or application-specific infrastructure configuration.

## Build and disposable verification

From the repository root, with Java 21 and Maven available:

```powershell
mvn -B -ntp -pl apex-starter-webmvc,apex-starter-cell-messaging -am install
mvn -B -ntp -f order-cell/pom.xml clean verify
# Add -o to each Maven command when dependencies are already cached.
```

The first command builds/installs current Starters and their dependencies. The second
builds this Cell independently and runs its creation and Saga integration tests against a real HTTP
server, disposable embedded PostgreSQL and an embedded Kafka KRaft broker. No Docker,
development broker or development database is required. Test topics are provisioned
only inside the disposable broker, whose auto-topic-creation is disabled.

The test fixture excludes inherited deployment environment/system properties, pins
all resource addresses and uses a random HTTP port. It closes the application before
the broker/database, including startup failure cleanup. It never manually invokes
`publishBatch()`. The same production application class is started through normal
Spring Boot auto-configuration; there are no test replacements for the persistence,
Outbox, publisher, Clock or execution-context Beans.

The executable artifact is `order-cell/target/order-cell.jar`. Tests launch the real
Boot context from compiled classes. The cross-Cell verification runner described below also
starts this packaged JAR and Inventory in separate JVMs.

## Running outside tests

Supply `CELL_DB_URL`, `CELL_DB_USERNAME`, `CELL_DB_PASSWORD` and
`KAFKA_BOOTSTRAP_SERVERS` through the deployment environment or supported external
configuration. No machine address or database credential is committed as a default.
Own and provision the Order database and Kafka stream explicitly before launch.
The application does not provision shared topics.

Optional overrides are `CELL_ID`, `APEX_ORDER_EVENTS_TOPIC` and `SERVER_PORT`.
Defaults are `order-cell`, `apex.order.events.v1` and port `8083` respectively.
Do not reuse Customer/Reporting databases for this Cell.

After provisioning and setting the environment, run `java -jar order-cell/target/order-cell.jar`.
For example, a POST body is:

```json
{"externalReference":"ORDER-DEMO-1","description":"سفارش آزمایشی"}
```

## Infrastructure work still required from the Cell developer

| Item | Actual reference implementation / responsibility |
|---|---|
| Maven setup | Independent Boot parent/version, Platform version, two Starter dependencies, validation, Flyway starter and PostgreSQL Flyway support. Test dependencies remain explicit. |
| Application bootstrap | `OrderCellApplication`: a normal `@SpringBootApplication` and `main`; no `@Bean`, custom `@Configuration`, component scan expansion or explicit APEX import. |
| Transaction boundary | One `@Transactional` on `CreateOrderUseCase.execute`; calls must go through the Spring-managed use case. |
| Business persistence | `OrderRepository`, `JdbcOrderRepository` and `V1__create_orders.sql`. This SQL belongs to the Order domain, not Platform messaging. |
| HTTP binding | `OrderController` and its nested validated request record use ordinary Spring MVC/validation annotations. |
| Event declaration | An `IntegrationEvent` record and one `EventContracts.define(...)` declaration; no direct Kafka access in business code. |
| Identity | `spring.application.name` and `apex.cell.id`. The existing filter creates HTTP root contexts; no Cell-local filter/provider is written. |
| External resources | DataSource URL/username/password and Kafka bootstrap servers must be supplied and provisioned operationally. |
| Platform migrations | Explicit Flyway dependencies and `classpath:db/migration,classpath:db/platform`. No Platform SQL was copied into the Cell. |
| Kafka policy | String key/value serializers, ACK/idempotence settings and disabled application-admin topic creation remain explicit portable YAML. |
| Roles and routing | Explicit Cell Messaging/Publisher/Consumer switches and a publisher topic. Routing is still configured independently of the event contract. |
| Test infrastructure | `DisposableOrderRuntime` constructs/owns real disposable resources and isolated Boot arguments. `OrderCellIntegrationTest` contains HTTP/Kafka observation and fault injection. These are test-only and are not runtime infrastructure Beans. |

`application.yml` contains 17 leaf settings: two identities, four external-resource
values, one migration location list, five Kafka technical policy values, three role
switches, one topic mapping and one HTTP port. Publisher-on and Consumer-off are
existing defaults; the reference states them explicitly. No dedicated consumer group
or consumer configuration is needed for this producer-only application.

The Starters provide `Clock`, the HTTP filter, `ExecutionContextProvider`, `CellEvents`,
the envelope factory, Outbox adapters, Kafka publisher and scheduler. The business
use case does not build envelopes, call Kafka or manage trace scopes for HTTP requests.

## Current constraints observed without changing the Platform

- Fresh-database migrations work. Platform V1001/V1002 share a Flyway history with
  business V1; plan upgrade/version allocation before adding business V2/V3. This
  reference does not edit applied migrations or enable blanket out-of-order execution.
- The current API takes a UUID aggregate ID and the JDBC Outbox stores UUIDs.
- Contract/schema labels do not automatically resolve destinations. The current
  envelope also does not carry the contract's schema label; M24 does not change that.
- Background/non-HTTP callers must establish an explicit `ExecutionContextProvider`
  scope before invoking the use case. The negative integration test confirms that
  missing context rolls back the already inserted Order.
- Disabling the publisher leaves appended events durable in Outbox; delivery requires
  an explicitly enabled publishing path. No fallback delivery mechanism is added.
- Selecting the existing `db/platform` location installs its bundled Inbox migration
  as well as Outbox even though this reference has no Consumer Beans.
- The original M24 POST /api/v1/orders and ORDER_CREATED contract remain compatible.
  Saga placement and its read-only status query are separate endpoints described below.

## Order / Inventory Saga stage (M26.6–M26.7)

`POST /api/v1/order-placements` accepts externalReference, description and a nonempty
items array [{productId, quantity}]. Order/items/ORDER_PLACED V1 Outbox append are atomic.
The response is HTTP 201 with {orderId,status:"PENDING_INVENTORY"}.
`GET /api/v1/order-placements/{orderId}` returns {orderId,status} or 404.

Opt-in `--spring.profiles.active=saga` enables the Inventory outcome consumer, whose
group defaults to `order-cell-inventory-outcome-v1` (APEX_ORDER_CONSUMER_GROUP override).
It subscribes to apex.inventory.events.v1 (APEX_INVENTORY_EVENTS_TOPIC override) and
retains publishing to apex.order.events.v1. This profile requires ORDER_DB_URL,
ORDER_DB_USERNAME, ORDER_DB_PASSWORD and KAFKA_BOOTSTRAP_SERVERS, with no connection
defaults. Existing base YAML/local credentials are untouched.

Before Kafka containers start, lifecycle preflight requires both INVENTORY_RESERVED V1
and INVENTORY_RESERVATION_REJECTED V1 handlers. These use Order-owned {orderId} wire DTOs;
no Inventory business JAR is imported. A conditional SQL update changes only
PENDING_INVENTORY to PENDING_PAYMENT or CANCELLED, respectively. An already matching
status is harmless; missing, conflicting and late outcomes throw and roll back Inbox.
Payment, CONFIRMED transitions and compensation are not implemented.

Run `powershell.exe -NoProfile -ExecutionPolicy Bypass -File inventory-cell/tools/verify-saga.ps1 -Offline`
from the repository root for clean Platform builds, both complete Cell suites and the
real Kafka/two-PostgreSQL/two-child-JVM demo. It records content hashes to reject stale
Order/Platform artifacts. See inventory-cell/README.md for manual startup and topic provisioning.

The saga profile opts in to Platform reliable consumption. Provision `apex.inventory.events.v1-dlt`
with at least the source partition count before startup. Unsupported/malformed records and
exhausted business failures are quarantined with the original key/raw value; source offsets
commit only after broker-confirmed DLT publication. Unavailable DLT blocks recovery. Legacy
mode without the reliability flag retains the pre-existing risks. See
[Platform reliability configuration](../apex-platform-messaging/README.md) for retry knobs,
ACL/retention requirements, duplicate-DLT boundaries and explicit M26.9 operator replay.
There is no automatic replay or exactly-once Kafka claim; Inbox/business idempotency remains required.
