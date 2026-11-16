# AOR-01: staged messaging storage separation

## Scope and compatibility decision

Starting branch `credit-engine-event-sourcing`, SHA `02f591f0e9cb3e1a9a663cc79b0c569f36f3a465`, clean working tree. Java 21, Boot 4.1.1, SDK `1.0-SNAPSHOT`. These are local development coordinates, not immutable production releases. No Oracle implementation, SQL, dependency or support qualification is introduced.

SDK-1 freezes class ownership by artifact as well as public/protected JVM declarations. Moving the existing messaging classes to another JAR would violate that exact artifact baseline. Recreating the entire messaging runtime behind compatibility wrappers would be a broad, risky duplication/refactor. AOR-01 therefore implements the safe staged boundary: driver-free consumption and separate PostgreSQL activation, while **retaining the original classes and migration resources in the legacy JAR**. This is not a completed physical extraction of all generic/SQL classes into separate JARs. There is no new `apex-sdk-messaging-postgresql` placeholder.

## Dependency graph

Before:

```text
apex-sdk-messaging -> apex-sdk-api + apex-sdk-spring-boot
                   -> Boot Kafka/Jackson/JDBC starters
                   -> PostgreSQL driver (runtime)
```

After, the old coordinate retains that graph. The additional supported composition is:

```text
apex-sdk-messaging-core -> apex-sdk-messaging
                          excluding PostgreSQL driver and Boot JDBC starter
                       -> spring-jdbc + spring-tx (framework contracts/orchestration)
```

The core composition contains a selection marker and meaningful isolation/activation tests; it does not duplicate runtime classes. It supplies Kafka/Jackson and Spring transaction/JDBC APIs, but no database driver, pool, Boot datasource activation or database-specific migration library. SQL resources still reside transitively in the legacy JAR and are never auto-installed by the SDK. This path is driver-neutral, not SQL-resource-free. Maven exclusions are path-specific: adding the legacy artifact independently can reintroduce its PostgreSQL driver. Independent dependency trees prove each selected graph, not arbitrary mixed consumers.

## Ownership inventory

| Types | Logical owner and source |
|---|---|
| EventMetadata, EventEnvelope, CellEvents and event declaration contracts | Existing apex-sdk-api; unchanged |
| OutboxAppender, OutboxStore, ProcessedEventStore | Provider-independent SPIs, physically retained in apex-sdk-messaging for v1 compatibility |
| ConfigurableOutboxPublisher | Provider-independent lease/send/mark/retry orchestration; same implementation and constructors, no SQL changes |
| SimpleTransactionalEventHandlerExecutor | Provider-independent Inbox registration + handler orchestration; existing Spring transaction proxy boundary unchanged |
| ReliableKafkaConsumerRuntime | Kafka listener recovery/DLT lifecycle, not a PostgreSQL implementation; unchanged |
| PlatformCellMessagingAutoConfiguration | Generic context/envelope/dispatch/publisher/consumer wiring; imports the separate internal PostgreSQL defaults configuration |
| PlatformConsumerReliabilityAutoConfiguration | Selected consumer/reliability factory and listener policy; unchanged, independent of provider selection |
| PlatformTopicProvisioningAutoConfiguration | Explicit opt-in Kafka topic readiness, independent of storage; unchanged |
| JdbcOutboxAppender, JdbcOutboxStore, JdbcProcessedEventStore | PostgreSQL implementations despite the generic-looking package name; public types/constructors remain in their original artifact |
| PostgresqlStorageConfiguration | New package-private configuration, owns only default PostgreSQL bean factories; native ConditionalOnMissingBean replacement |
| MessagingStorageSelection | New package-private selection condition and deferred database-product validation, no new public SPI |

All platform types above except the event/API contracts live under `apex-sdk-messaging/src/main/java/com/apex/platform/messaging/`. Existing Boot imports remain the three original public configurations under `src/main/resources/META-INF/spring/`; the selected Cell configuration imports the internal PostgreSQL configuration, so it is not globally activated on its own.

Application infrastructure remains outside this SDK refactor: apex-infrastructure's generated jOOQ PostgreSQL outbox/inbox/saga repositories and order-cell's durable request executor still own their existing business schemas and transaction manager checks. They are not promoted as generic database adapters. API, Boot context, gRPC, Identity and Gateway behavior is untouched.

## Selection and activation

New property: `apex.messaging.storage.provider`. Accepted exact values are `postgresql` and `custom`. Missing property defaults to `postgresql` for the legacy coordinate and `custom` when the core composition marker is present. Explicit configuration wins over the marker. The marker's default is deterministic for the whole classloader, including a graph that deliberately selects both entry points; document such a graph explicitly and use explicit selection if PostgreSQL defaults are desired.

`custom` is an owner-bean mode, not a database implementation. It registers no default storage. Selected CellEvents still requires an owner OutboxAppender even when polling is disabled, preserving outbound enqueue capability. A selected publisher additionally requires OutboxStore; a selected consumer additionally requires ProcessedEventStore. Missing required storage and ambiguous multiple storage candidates fail in preflight (native selection with one @Primary remains supported), with no in-memory fallback. Custom beans continue to replace each default independently in legacy PostgreSQL mode. Publisher-only does not require Inbox/group/topics; consumer-only does not require KafkaTemplate or publisher topic. The legacy PostgreSQL composition preserves its shared OutboxStore and appender defaults even when polling is disabled.

Messaging disabled means no provider validation, storage instantiation or SDK connection. It does not disable an application's independently configured native datasource initializer or Kafka infrastructure. Installing an optional capability is distinct from enabling it. Unsupported explicitly selected providers, including `oracle`, fail clearly when messaging is selected; Oracle never selects PostgreSQL as a supported provider.

Selecting PostgreSQL defaults requires its driver. Constructors/bean factories do not open a connection. At the first actual operation, JDBC metadata must report `PostgreSQL` before the adapter executes its PostgreSQL SQL. This validation is distinct from configuration selection; it is not a dialect inference or Oracle capability. It uses JdbcTemplate and the owner's transaction-bound datasource connection; there is no replacement datasource/transaction manager, cached database-product decision or XA. Outside an owner transaction, validation and SQL can acquire separate connections, so routed datasources must maintain stable product routing; atomic business/outbox work already requires the owner transaction. No metadata lookup is performed for owner custom beans.

## PostgreSQL assumptions retained

The driver dependency is `org.postgresql:postgresql` at runtime. JdbcOutboxAppender uses `CAST(? AS jsonb)` and a false literal; metadata UUIDs are bound directly, aggregateId is parsed as UUID. JdbcOutboxStore uses boolean literals, `LIMIT ? FOR UPDATE SKIP LOCKED`, UUID claim tokens, UUID getObject mappings, OffsetDateTime mappings and PostgreSQL payload-object string conversion. JdbcProcessedEventStore uses `ON CONFLICT (consumer_group,event_id) DO NOTHING`. All SQL strings, parameters, result mappings, lease/retry/dead-letter transitions and transaction annotations remain unchanged.

V1001/V1002 remain at `classpath:db/platform`, with unchanged bytes/checksums. They use BIGSERIAL, UUID, JSONB, TIMESTAMPTZ, BOOLEAN, NOW(), IF NOT EXISTS and a partial index. They are explicitly owner-installed, not portable migrations. No new schema, table, column or migration is introduced.

The application jOOQ adapters also use generated PostgreSQL types/JSONB and conflict clauses. The order receipt executor uses PostgreSQL set_config/clock_timestamp/ON CONFLICT and the exact owner datasource manager, returning only after commit. Those are inspected compatibility consumers, not refactored by AOR-01.

## Transaction and failure guarantees

No transaction-sensitive storage method is wrapped behind a self-invocation facade. The original JdbcOutboxStore bean still receives native Spring transaction interception. Business+append and Inbox+effect must share the owner's transaction. Claims commit before broker I/O; marks/retries/dead-letter updates remain fenced by row ID + claim token. Leases allow recovery; an ACK before a lost mark can cause redelivery. Guarantees remain at-least-once with Inbox/business idempotency, never exactly-once across Kafka and SQL.

## Future work

Before a physical module extraction, review an artifact-level v2 ownership contract and migration/relocation strategy while preserving v1 consumers. The original v1 baseline cannot be silently rewritten. AOR-02 planning can target the existing OutboxAppender/OutboxStore SPIs with a real optional adapter; AOR-03 can target ProcessedEventStore. Neither should fork orchestration. AOR-04 requires separate explicit dialect migrations and an approved tested database/version/license target. No Oracle provider value becomes valid until that implementation and its real database qualification exist.
