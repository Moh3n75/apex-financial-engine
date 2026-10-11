# Apex Financial Engine — Architecture Handoff & Continuation Guide

**Checkpoint:** End of M18.7 — Consumer Framework Complete  
**Project:** `apex-financial-engine`  
**Runtime:** Java 21, Spring Boot 4.1.1, Maven multi-module, PostgreSQL, Flyway, jOOQ, Kafka, Micrometer/Actuator  
**Architecture style:** Hexagonal / Clean Architecture  
**Messaging semantics:** Transactional Outbox + Kafka at-least-once delivery + transactional consumer idempotency = effectively-once business processing

## 1. Purpose

This is the handoff/source-of-truth document for continuing the project from the current checkpoint. A future developer or AI should continue from here without rebuilding foundations already completed.

The next architecture task should start **after M18.7**, with multi-instance Outbox hardening before Saga orchestration.

## 2. Current high-level architecture

```text
Business Use Case
      |
      v
Application Service
      |
      v
Domain + Ledger + Financial Transaction
      |
      | SAME PostgreSQL transaction
      v
Transactional Outbox
      |
      v
outbox_event
      |
      v
Outbox Publisher
      |
      +---- retry / backoff / producer DLQ state
      |
      v
Kafka Producer
      |
      v
Kafka Topic
      |
      v
Kafka Consumer Adapter
      |
      v
Envelope Validation / Deserialization
      |
      v
Versioned Event Handler Registry
      |
      v
Transactional Consumer Idempotency
      |
      v
Business Handler
      |
      +--------------------+
      |                    |
   Success              Failure
      |                    |
      v                    v
processed_event      Retry + Backoff
                           |
                           v
                 consumer_event_failure
                           |
                           v
                          DLT
                           |
                           v
                    Controlled Replay
                           |
                           v
                       RECOVERED
```

## 3. Maven / module structure

Current intended active modules:

```text
apex-domain
apex-application
apex-infrastructure
apex-bootstrap
apex-platform-core
apex-platform-events
apex-platform-messaging
```

Notes:
- Kafka + Outbox were consolidated into `apex-platform-messaging`.
- Consumer abstractions should also live in `apex-platform-messaging`; avoid separate permanent `apex-platform-consumer` module.
- Only `apex-bootstrap` is the runtime application.
- Avoid Maven module explosion.

Suggested package structure:

```text
com.apex.platform.messaging
├── kafka
├── outbox
└── consumer
```

## 4. Event model already completed

### `IntegrationEvent`
Exists in `apex-platform-events`.

### `EventMetadata`

```java
public record EventMetadata(
    UUID eventId,
    String eventType,
    int eventVersion,
    UUID correlationId,
    UUID causationId,
    String aggregateId,
    String aggregateType,
    String sourceService,
    String cellId,
    Instant occurredAt
) {}
```

Semantics:
- `eventId`: globally unique event identity
- `eventType`: logical event contract
- `eventVersion`: contract version
- `correlationId`: end-to-end correlation
- `causationId`: cause of this event; nullable for roots
- `aggregateId`: aggregate public identifier
- `aggregateType`: aggregate type
- `sourceService`: producing service
- `cellId`: deployment/cell origin
- `occurredAt`: occurrence timestamp

### `EventEnvelope<T>`

Current design:

```java
public record EventEnvelope<T>(
    EventMetadata metadata,
    T payload
) {}
```

It is intentionally generic and is **not** constrained with `T extends IntegrationEvent`, because transport reconstruction may use `EventEnvelope<JsonNode>`.

### `EventEnvelopeFactory`
Already exists and creates metadata once at event creation time.

Critical invariant:

> Publisher/consumer code must not generate a new `eventId`, `correlationId`, or `occurredAt` for an already-persisted integration event.

## 5. Execution context

`apex-platform-core` contains `ExecutionContext`:

```text
correlationId
causationId
sourceService
cellId
```

Purchase currently creates a temporary root context similar to:

```text
sourceService = apex-financial-engine
cellId        = local-cell-1
```

Future Gateway/HTTP tracing should inject this context. Application services should eventually stop creating root correlation context themselves.

## 6. Purchase event production

`PurchaseApplicationService` creates a `PurchaseCompletedIntegrationEvent`, then an `EventEnvelope`, then appends it to the Outbox within the same Unit of Work.

```text
Purchase
  |
  v
FinancialTransaction
  |
  +--> Ledger
  |
  +--> PurchaseCompletedIntegrationEvent
              |
              v
        EventEnvelopeFactory
              |
              v
        OutboxRepository.append(...)
```

The envelope is created once.

## 7. Transactional Outbox

### `OutboxRepository`

Conceptual contract:

```java
void append(EventEnvelope<? extends IntegrationEvent> event);
```

Infrastructure serializes payload JSON and persists metadata in explicit columns.

### `outbox_event` current fields

```text
id
event_type
aggregate_id
payload
published
created_at

event_id
event_version
correlation_id
causation_id
aggregate_type
source_service
cell_id
occurred_at

retry_count
published_at
last_error
next_retry_at

dead_letter
dead_letter_at
```

Known migrations:

```text
V5  standardized Outbox metadata
V6  retry_count
V7  retry/delivery tracking
V8  producer dead-letter support
```

Producer error fields belong only to producer delivery. Do not store consumer failures in `outbox_event.last_error`.

## 8. Important Outbox aggregate-id caveat

Current DB `aggregate_id` is a local/internal BIGINT for financial transactions, while `EventMetadata.aggregateId` was originally a public UUID.

This can cause reconstructed events to expose the internal DB ID instead of the original public aggregate ID.

Recommended future correction:

```text
local_aggregate_id / aggregate_fk
aggregate_key / aggregate_public_id
```

Preserve the public aggregate identifier independently. This matters for partitioning, contracts, replay, tracing, and service boundaries.

## 9. Outbox messaging contracts

Inside `apex-platform-messaging`:

### `OutboxRecord`
Contains delivery fields approximately:

```text
id
eventId
eventType
eventVersion
correlationId
causationId
aggregateId
aggregateType
sourceService
cellId
occurredAt
payload
retryCount
nextRetryAt
```

### `OutboxStore`

```java
List<OutboxRecord> findBatch(int size);
void markPublished(Long id);
void markFailed(Long id, String error, Instant nextRetryAt);
void moveToDeadLetter(Long id, String error);
```

### `OutboxEventMapper`
Reconstructs the persisted envelope. Infrastructure `DefaultOutboxEventMapper` uses Jackson 3 and may produce `EventEnvelope<JsonNode>`.

Publisher must not call `EventEnvelopeFactory`.

## 10. Outbox retry / dead-letter behavior

`RetryPolicy` governs producer-side Outbox retries.

Historical schedule:

```text
0 -> 30 sec
1 -> 5 min
2 -> 30 min
3 -> 2 hr
default -> 6 hr
max retries ~10
```

Cleanup candidate:
- if `RetryPolicy` still uses `Instant.now()`, inject `Clock`.

## 11. Time policy

Project decision:

> Do not hard-code UTC in Java infrastructure conversions.

Use injected `Clock`, defaulting to:

```java
Clock.systemDefaultZone()
```

Examples:

```java
OffsetDateTime.now(clock)
OffsetDateTime.ofInstant(instant, clock.getZone())
```

DB `TIMESTAMPTZ` maps to `OffsetDateTime`; platform/application prefers `Instant`.

## 12. Kafka producer

Broker:

```text
10.10.1.57:29092
```

Main topic:

```text
apex.financial.events.v1
```

DLT topic:

```text
apex.financial.events.v1-dlt
```

Kafka UI is available in the environment.

## 13. Kafka producer hardening complete

Current config includes:

```yaml
spring:
  kafka:
    bootstrap-servers: 10.10.1.57:29092

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        delivery.timeout.ms: 120000
        request.timeout.ms: 30000
        compression.type: zstd
        batch.size: 65536
        linger.ms: 10
        retries: 2147483647
```

Kafka idempotent producer does not make the full pipeline exactly-once.

Correct guarantee:

```text
Kafka transport = at-least-once
Business effect = effectively-once via DB idempotency
```

## 14. Kafka partition key

Publisher moved from random `eventId` keying toward:

```text
aggregateType + ":" + aggregateId
```

Goal: preserve per-aggregate ordering.

Before finalizing this strategy, fix the public-vs-internal aggregateId inconsistency.

## 15. Topic resolution

Topic string should not remain duplicated/hard-coded in multiple classes.

Long term:
- introduce a destination/topic resolver or persist destination with Outbox metadata
- production topic lifecycle belongs in deployment/IaC
- runtime topic creation is acceptable only for development

## 16. Producer-side observability complete

Actuator + Micrometer enabled.

Management endpoints include:

```text
health
metrics
prometheus
```

Custom metrics:

```text
apex.outbox.published
apex.outbox.failed
apex.outbox.deadletter
apex.outbox.pending.count
apex.outbox.oldest.age.seconds
```

Key components:

```text
OutboxMetrics
OutboxStatistics
JooqOutboxStatistics
OutboxMetricsCollector
```

`@EnableScheduling` is enabled.

## 17. Consumer Framework — M18

Consumer platform contracts live under:

```text
apex-platform-messaging
com.apex.platform.messaging.consumer
```

Kafka/Jackson/jOOQ/Spring implementations live under:

```text
apex-infrastructure
com.apex.infrastructure.messaging.consumer
```

Keep framework-specific code out of platform contracts.

## 18. Consumer core contracts

### `EventHandlerKey`
Routes by:

```text
eventType + eventVersion
```

### `ConsumerContext`
Contains:

```text
consumerGroup
topic
partition
offset
messageKey
receivedAt
```

### `EventHandler<T>`

```java
public interface EventHandler<T> {
    String eventType();
    int eventVersion();
    Class<T> payloadType();

    void handle(
        EventEnvelope<T> event,
        ConsumerContext context
    );
}
```

Generic by design.

### `EventHandlerRegistry`
Per-service registry only. Do not create a global organization-wide registry.

Duplicate handler registrations should fail fast at startup.

## 19. Jackson dispatcher

Infrastructure:

```text
JacksonEventMessageDispatcher
```

Flow:

```text
raw Kafka String
    |
    v
JsonNode
    |
    +--> metadata
    +--> payload
    |
    v
EventMetadata
    |
    v
EventHandlerRegistry
    |
    v
handler.payloadType()
    |
    v
typed payload
    |
    v
EventEnvelope<T>
    |
    v
EventHandlerExecutor
```

No event metadata is regenerated.

## 20. Kafka consumer adapter

Infrastructure:

```text
KafkaEventConsumer
```

Manual ACK behavior:

```text
HANDLED   -> ACK
DUPLICATE -> ACK
IGNORED   -> ACK
Exception -> error handler owns retry/recovery
```

Consumer config currently resembles:

```yaml
spring:
  kafka:
    consumer:
      group-id: apex-financial-engine-v2
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      enable-auto-commit: false
      auto-offset-reset: latest

    listener:
      ack-mode: manual_immediate

apex:
  messaging:
    topics:
      financial-events: apex.financial.events.v1

    consumer:
      concurrency: 3
```

`latest` was used in development to avoid legacy topic messages predating the standardized envelope.

## 21. Legacy-message lesson

Initial consumer error:

```text
Event metadata is missing
```

Cause: new consumer group read old topic records created before the standardized envelope.

Development fix:

```text
new consumer group
auto-offset-reset = latest
```

Long-term compatibility must be intentional; do not silently fabricate metadata for legacy messages.

## 22. Transactional consumer idempotency

Migration:

```text
V9__add_processed_event_idempotency.sql
```

Table:

```text
processed_event
```

Key:

```text
PRIMARY KEY (consumer_group, event_id)
```

This allows the same event to be independently processed by multiple consumer groups while remaining idempotent within each group.

## 23. `ProcessedEventStore`

```java
boolean registerIfAbsent(
    EventMetadata metadata,
    ConsumerContext context
);
```

Implementation uses:

```sql
INSERT ...
ON CONFLICT (consumer_group, event_id)
DO NOTHING
```

Meaning:

```text
inserted = 1 -> first processing
inserted = 0 -> duplicate
```

## 24. `TransactionalEventHandlerExecutor`

Core transaction:

```text
BEGIN

INSERT processed_event

if duplicate:
    return DUPLICATE

handler.handle(...)

mark failure recovered when applicable

COMMIT
```

If handler fails:

```text
ROLLBACK
```

Therefore the idempotency insert also rolls back, allowing a valid retry.

## 25. Consumer result states

`DispatchResult`:

```text
HANDLED
DUPLICATE
IGNORED
```

Duplicates are acknowledged because they represent already-processed events for that consumer group.

## 26. Consumer retry + DLT — M18.4

Uses Spring Kafka:

```text
DefaultErrorHandler
DeadLetterPublishingRecoverer
FixedBackOff
```

Current behavior:

```text
original attempt
+ 2 retries
= 3 total attempts
```

Approximately 1 second between retries.

`setCommitRecovered(true)` is used with manual-immediate semantics.

## 27. Retryable vs non-retryable failures

Exceptions include:

```text
NonRetryableEventException
MalformedEventException
```

Non-retryable examples:

```text
invalid JSON
missing metadata
invalid metadata
invalid payload for known handler type
```

Retryable examples:

```text
temporary DB outage
temporary network failure
temporary downstream dependency failure
```

Important:
- Parsing/deserialization failures must remain distinct from handler/runtime failures.
- Do not wrap business handler exceptions as malformed data.

## 28. Consumer failure audit — M18.5

Migration:

```text
V10__add_consumer_event_failure_audit.sql
```

Table:

```text
consumer_event_failure
```

Tracks approximately:

```text
id
consumer_group

event_id
event_type
event_version

topic
partition_id
offset_value

attempt_count

error_type
last_error

status

first_failed_at
last_failed_at

dead_letter_topic
dead_lettered_at

recovered_at
```

Unique delivery identity:

```text
consumer_group
topic
partition_id
offset_value
```

## 29. Consumer failure statuses

`ConsumerFailureStatus` includes:

```text
RETRYING
DEAD_LETTERED
RECOVERY_FAILED
REPLAYING
REPLAY_FAILED
RECOVERED
```

Use the enum instead of scattered string literals.

## 30. Why failure audit uses `REQUIRES_NEW`

Handler failure should roll back:

```text
business changes
processed_event insert
```

but failure audit must survive.

Therefore audit writes use an independent transaction:

```text
Propagation.REQUIRES_NEW
```

## 31. Consumer failure retry listener

`ConsumerFailureRetryListener` connects Spring Kafka retry lifecycle to:

```text
ConsumerFailureAuditService
ConsumerMetrics
```

Conceptually:

```text
failedDelivery(...)
    -> update attempt_count / last_error / RETRYING

recovered(...)
    -> DEAD_LETTERED

recoveryFailed(...)
    -> RECOVERY_FAILED
```

Malformed records may have null event identity fields; topic/partition/offset still identify delivery.

## 32. Consumer failure metrics

```text
apex.consumer.delivery.failed
apex.consumer.deadletter.published
apex.consumer.deadletter.publish.failed
```

## 33. Controlled DLT Replay — M18.6

Migration:

```text
V11__add_consumer_event_replay_support.sql
```

Replay fields include:

```text
message_key
original_payload

replay_count
replay_requested_at
last_replayed_at
last_replay_error
```

Critical rule:

> Replay the original serialized envelope exactly as stored.

Do not generate a new `eventId`, `correlationId`, or `occurredAt`.

## 34. Replay contract

```java
public interface ConsumerFailureReplay {
    void replay(long failureId);
}
```

Infrastructure:

```text
ConsumerFailureReplayService
```

Replayable states:

```text
DEAD_LETTERED
REPLAY_FAILED
```

## 35. Replay concurrency control

Replay atomically claims a row:

```text
DEAD_LETTERED
      |
      v
REPLAYING
```

This prevents multiple operators/nodes from replaying the same failure concurrently.

Do not hold DB locks/transactions open during Kafka network publishing.

## 36. Replay result

On Kafka publish success:

```text
replay_count += 1
last_replayed_at = now
```

On publish failure:

```text
status = REPLAY_FAILED
last_replay_error = ...
```

## 37. Recovery completion

`ConsumerFailureRecoveryStore` marks previous failures:

```text
RECOVERED
recovered_at = ...
```

This also handles:

```text
attempt 1 fails
attempt 2 succeeds
```

so audit status does not remain incorrectly at `RETRYING`.

## 38. Internal replay API

Development/admin endpoint resembles:

```text
POST /internal/messaging/consumer-failures/{failureId}/replay
```

Production requirements:
- not public
- authenticated/authorized
- operator identity auditing
- rate limits / replay controls

## 39. Consumer operational observability — M18.7

Statistics components:

```text
ConsumerFailureStatistics
JooqConsumerFailureStatistics
ConsumerOperationalMetricsCollector
ConsumerHandlerMetrics
```

Metrics:

```text
apex.consumer.failure.retrying
apex.consumer.failure.deadlettered
apex.consumer.failure.replay.failed
apex.consumer.failure.unresolved
apex.consumer.failure.oldest.age.seconds

apex.consumer.handler.success
apex.consumer.handler.failed
apex.consumer.handler.latency
```

Unresolved states:

```text
RETRYING
DEAD_LETTERED
RECOVERY_FAILED
REPLAYING
REPLAY_FAILED
```

`RECOVERED` is excluded.

## 40. jOOQ oldest-failure typing fix

Do not use:

```java
.fetchOne(0)
```

for the `min(FIRST_FAILED_AT)` aggregation because it can degrade to `Object`.

Correct pattern:

```java
var record =
    dsl.select(
        min(CONSUMER_EVENT_FAILURE.FIRST_FAILED_AT)
    )
    .from(CONSUMER_EVENT_FAILURE)
    .where(...)
    .fetchOne();

if (record == null) {
    return null;
}

OffsetDateTime value =
    record.value1();

return value != null
    ? value.toInstant()
    : null;
```

## 41. Handler metrics rule

Idempotency check occurs before handler timing.

Duplicates must not count as successful handler executions.

## 42. Complete consumer lifecycle

```text
Kafka
  |
  v
KafkaEventConsumer
  |
  v
JacksonEventMessageDispatcher
  |
  v
metadata + payload validation
  |
  v
EventHandlerRegistry
  |
  v
TransactionalEventHandlerExecutor
  |
  +--> processed_event idempotency
  |
  v
EventHandler
  |
  +-------------------------+
  |                         |
success                    exception
  |                         |
  v                         v
HANDLED                 Spring Kafka Retry
  |                         |
  v                         +--> failure audit
ACK                         |
                            +--> retry
                            |
                            +--> DLT
                                    |
                                    v
                         consumer_event_failure
                                    |
                                    v
                              controlled replay
                                    |
                                    v
                                RECOVERED
```

## 43. Delivery guarantee

Do not describe the architecture as end-to-end exactly-once.

Correct wording:

```text
Producer persistence:
local PostgreSQL transaction + Outbox

Kafka transport:
at-least-once

Consumer:
transactional DB idempotency

Business effect:
effectively-once per consumer group
```

## 44. Critical unresolved issue: multi-instance Outbox safety

This is the next hardening priority.

Current `JooqOutboxStore.findBatch()` conceptually uses:

```text
WHERE published = false
AND dead_letter = false
AND next_retry_at <= now

FOR UPDATE
SKIP LOCKED
```

But `FOR UPDATE SKIP LOCKED` only protects rows while the transaction holding the lock is still open.

If `findBatch()` returns and its transaction/auto-commit scope ends before Kafka publishing, multiple app instances can still select/publish the same rows.

Do **not** fix this by holding DB row locks while waiting on Kafka network I/O.

Recommended next model:

```text
READY
  |
  | short DB transaction
  v
CLAIMED
claim_token
claimed_at
claimed_until
  |
  | transaction closes
  v
Kafka publish outside DB transaction
  |
  +--> success -> PUBLISHED
  |
  +--> failure -> retry / claim release
```

Add lease expiry so crashed publishers do not permanently own rows.

## 45. Suggested next milestone

### Multi-instance Outbox Claim / Lease

Suggested schema:

```text
publish_status
claim_token
claimed_at
claimed_until
publisher_instance_id
```

or equivalent minimal fields.

Required behavior:

1. atomically claim a batch in a short transaction
2. use `SKIP LOCKED` only while claiming
3. commit the claim immediately
4. publish outside DB transaction
5. finalize success/failure by claim token
6. expired claims become claimable again
7. expose claim/stale-claim metrics
8. integration test with two concurrent publisher instances

Only after this should distributed Saga orchestration become the main focus.

## 46. Saga direction after Outbox hardening

Future M19:

```text
Service A
  |
  | local transaction
  +--> state change
  +--> outbox event
          |
          v
        Kafka
          |
          v
Service B
  |
  | local transaction
  +--> reservation/state mutation
  +--> event
```

Failures are handled with compensation/state transitions.

Cross-cell workflows should use:

```text
reservation
saga state
timeouts
compensation
idempotent commands/events
```

Do not use XA / 2PC.

## 47. jOOQ code generation rule

The code generator uses an explicit `<includes>` list.

Whenever a new table is added:

```text
1. add Flyway migration
2. apply migration
3. add table to jOOQ includes if needed
4. regenerate jOOQ
5. verify generated table + record classes
```

Historical issue:
`processed_event` existed in PostgreSQL but was not generated because it was missing from `<includes>`.

Generated package:

```text
com.apex.infrastructure.jooq.generated
```

Typical import:

```java
import static
com.apex.infrastructure.jooq.generated.tables.ProcessedEvent.PROCESSED_EVENT;
```

## 48. Technology / conventions

```text
Java 21
Spring Boot 4.1.1
Spring Kafka 4.1.1
PostgreSQL
Flyway
jOOQ
Spring JDBC
Jackson 3
Micrometer
Spring Boot Actuator
```

Jackson package style:

```java
tools.jackson.databind.json.JsonMapper
```

## 49. Architectural rules not to reverse casually

1. Database is authoritative; Kafka is not the source of truth.
2. Financial transaction + Ledger + Outbox append belong in one local PostgreSQL transaction.
3. Publisher never creates replacement event metadata.
4. Replay preserves original eventId.
5. Kafka transport remains at-least-once.
6. Consumer idempotency key is `(consumer_group, event_id)`.
7. Failed handlers roll back processed-event registration.
8. Consumer failure audit survives handler rollback.
9. Producer and consumer failures are separate state models.
10. Poison/malformed messages should not consume transient retry budget.
11. Do not build a global business EventTypeRegistry.
12. Keep Spring Kafka/Jackson/jOOQ adapters in infrastructure.
13. Avoid Maven module explosion.
14. Do not use XA/2PC for cross-service workflows.
15. Use injected `Clock`.
16. Do not hold DB row locks across Kafka network calls.
17. Production topic lifecycle belongs in deployment/IaC.

## 50. Important class checklist

### Platform Core
```text
ExecutionContext
```

### Platform Events
```text
IntegrationEvent
EventMetadata
EventEnvelope<T>
EventEnvelopeFactory
```

### Platform Messaging — Outbox/Kafka
```text
OutboxRecord
OutboxStore
OutboxPublisher
OutboxEventMapper
RetryPolicy
KafkaMessagePublisher
SpringKafkaMessagePublisher
```

### Platform Messaging — Consumer
```text
ConsumerContext
EventHandlerKey
EventHandler<T>
EventHandlerRegistry
DefaultEventHandlerRegistry
EventMessageDispatcher
DispatchResult
ProcessedEventStore
EventHandlerExecutor
NonRetryableEventException
MalformedEventException
ConsumerFailureStatus
ConsumerFailureRecoveryStore
ConsumerFailureReplay
ConsumerFailureStatistics
```

### Infrastructure — Producer/Outbox
```text
JooqOutboxRepository
JooqOutboxStore
DefaultOutboxEventMapper
DefaultOutboxPublisher
OutboxMetrics
JooqOutboxStatistics
OutboxMetricsCollector
KafkaPlatformConfiguration
OutboxConfiguration
```

### Infrastructure — Consumer
```text
KafkaEventConsumer
JacksonEventMessageDispatcher
JooqProcessedEventStore
TransactionalEventHandlerExecutor
KafkaConsumerErrorConfiguration
ConsumerFailureAuditService
ConsumerFailureRetryListener
ConsumerMetrics
JooqConsumerFailureRecoveryStore
ConsumerFailureReplayService
JooqConsumerFailureStatistics
ConsumerOperationalMetricsCollector
ConsumerHandlerMetrics
```

### Admin/HTTP
```text
ConsumerFailureReplayController
```

Exact packages may differ slightly based on current project organization.

## 51. Flyway checkpoint

Known sequence:

```text
V5  standardize Outbox metadata
V6  retry_count
V7  retry/delivery tracking
V8  Outbox dead-letter support
V9  processed_event idempotency
V10 consumer_event_failure audit
V11 consumer replay support
```

Do not renumber already-applied migrations.

## 52. Behaviors already tested

```text
standardized EventEnvelope published to Kafka
new consumer reads standardized envelope
legacy malformed message detected
transient handler failure retries 3 total attempts
exhausted failure goes to DLT
malformed/non-retryable event does not consume normal retries
consumer failure attempt_count updates
last_error persists
status becomes DEAD_LETTERED
processed_event prevents duplicate business execution
failed handler does not leave processed_event committed
controlled replay republishes original payload
successful replay becomes RECOVERED
custom producer/consumer metrics visible in Actuator
```

## 53. Validation commands

```bash
mvn clean verify
```

When DB schema changes:

```bash
mvn clean generate-sources
```

Useful queries:

```sql
SELECT * FROM outbox_event ORDER BY id DESC;
SELECT * FROM processed_event ORDER BY processed_at DESC;
SELECT * FROM consumer_event_failure ORDER BY id DESC;
```

Endpoints:

```text
/actuator/metrics
/actuator/prometheus
/actuator/health
```

## 54. Observability inventory

### Producer / Outbox
```text
apex.outbox.published
apex.outbox.failed
apex.outbox.deadletter
apex.outbox.pending.count
apex.outbox.oldest.age.seconds
```

### Consumer retry / DLT
```text
apex.consumer.delivery.failed
apex.consumer.deadletter.published
apex.consumer.deadletter.publish.failed
```

### Consumer failure state
```text
apex.consumer.failure.retrying
apex.consumer.failure.deadlettered
apex.consumer.failure.replay.failed
apex.consumer.failure.unresolved
apex.consumer.failure.oldest.age.seconds
```

### Handler
```text
apex.consumer.handler.success
apex.consumer.handler.failed
apex.consumer.handler.latency
```

Recommended future metrics:

```text
Kafka consumer lag
Outbox claimed rows
Outbox expired claims
Replay success/failure counters
DLT age
Consumer throughput
```

Avoid high-cardinality metric tags such as:

```text
eventId
correlationId
aggregateId
```

## 55. Security / operations future work

Before production exposure:

```text
secure replay endpoint
add authn/authz
audit operator identity
rate-limit replay
limit bulk replay
add approval controls for sensitive financial workflows
```

Do not expose raw failure payloads publicly.

## 56. Direct continuation prompt for another AI

```text
Continue development of the Java 21 / Spring Boot 4.1.1 Maven multi-module project `apex-financial-engine`.

The project has completed M18.7: a Transactional Outbox producer and generic Kafka consumer framework with:

- standardized EventMetadata and generic EventEnvelope<T>
- transactional Outbox
- Kafka idempotent producer
- producer retry + dead-letter state
- Outbox observability
- versioned EventHandlerRegistry
- Jackson Kafka dispatcher
- manual Kafka ACK
- processed_event idempotency keyed by (consumer_group, event_id)
- transactional handler execution
- consumer retry/backoff using Spring Kafka DefaultErrorHandler
- DeadLetterPublishingRecoverer
- malformed/non-retryable event classification
- consumer_event_failure audit with REQUIRES_NEW semantics
- retry attempt / last_error tracking
- controlled DLT replay preserving original payload/eventId
- RECOVERED / REPLAYING / REPLAY_FAILED lifecycle
- consumer operational metrics and handler latency metrics.

Do NOT recreate EventEnvelope, EventMetadata, EventEnvelopeFactory, Outbox, retry/DLT, consumer idempotency, failure audit, or replay foundations.

Do NOT claim end-to-end exactly-once. The model is at-least-once Kafka delivery + transactional DB idempotency = effectively-once business processing.

Before starting Saga/M19, first harden the Outbox for multiple concurrent application instances. The current FOR UPDATE SKIP LOCKED selection is insufficient if the row lock is released before Kafka network publishing.

Implement a short-transaction claim/lease model such as:
- claim_token
- claimed_at
- claimed_until
- optional publisher_instance_id / publish_status

Then publish outside the DB transaction and finalize success/failure by claim token. Expired claims must be recoverable. Add concurrent multi-publisher integration tests and metrics.

Also preserve the public aggregate identifier separately from any local BIGINT aggregate FK before finalizing aggregate-based Kafka partition semantics.

Keep platform contracts framework-neutral and adapters in apex-infrastructure. Avoid new Maven module explosion and avoid XA/2PC.
```

## 57. Recommended Git checkpoint

For M18.7:

```bash
git add .
git commit -m "Complete consumer operational observability"
```

Optional milestone tag:

```bash
git tag -a m18-consumer-framework-complete   -m "Consumer framework complete through M18.7"
```

Then:

```bash
git status
mvn clean verify
```

## 58. Final checkpoint summary

At this point the platform has:

```text
Local ACID financial write
        +
Transactional Outbox
        +
Reliable Kafka producer
        +
Standard event envelope
        +
Versioned consumer routing
        +
Transactional idempotency
        +
Retry / DLT
        +
Failure audit
        +
Controlled replay
        +
Operational observability
```

Largest known infrastructure risk before Saga work:

```text
MULTI-INSTANCE OUTBOX CLAIM SAFETY
```

Fix that next, then proceed to Saga / Distributed Workflow Foundation.
