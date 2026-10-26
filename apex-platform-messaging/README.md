# APEX Cell consumer reliability (M26.8)

Reliable mode reuses Spring Kafka's `DefaultErrorHandler` and `DeadLetterPublishingRecoverer`. It is opt-in; no new business listener, retry-topic topology, database migration or replay consumer is introduced.

```yaml
apex:
  messaging:
    cell:
      enabled: true
    consumer:
      enabled: true
      topics: apex.order.events.v1
      reliability:
        enabled: true
spring:
  kafka:
    consumer:
      group-id: inventory-cell-order-placed-v1
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      properties:
        allow.auto.create.topics: false
    listener:
      ack-mode: manual_immediate
    admin:
      auto-create: false
```

Supply broker connectivity/security through existing Spring Kafka configuration. No bootstrap endpoint or credential is supplied by this library. Required source topics and DLTs must already exist. The example uses the existing Inventory group; do not rename deployed groups or reset their offsets to adopt reliability.

## Activation, scope and compatibility

All three flags `apex.messaging.cell.enabled`, `apex.messaging.consumer.enabled` and `apex.messaging.consumer.reliability.enabled` must be true. The last flag defaults to false. Disabled/absent reliability retains the existing legacy listener factory and IGNORED behavior, including the known logging-only recovery/ignored-ACK risks. Producer-only and disabled roles create no APEX reliable runtime or DLT sender.

Reliable APEX listeners select `apexReliableKafkaListenerContainerFactory`. It uses Boot 4.1's `ConcurrentKafkaListenerContainerFactoryConfigurer` and the configured ConsumerFactory, including normal security, poll, observation and thread settings. Boot container customizers are applied. Record mode, MANUAL_IMMEDIATE, enable-auto-commit=false, sync commits, no async ACKs and no container/Kafka transaction manager are required; incompatible settings fail instead of silently falling back. The Inbox executor owns the database transaction.

The APEX factory's recovery policy cannot be replaced by an unrelated global ErrorHandler or container customizer. Global discard filters are not inherited by this factory: every record must reach APEX handling or DLT. The default factory and its filters/error handler remain unchanged for unrelated listeners. Arbitrary replacement listeners/interceptors that discard records are outside this supported setup; do not install them on the APEX container.

The private DLT template/factory is held inside `ReliableKafkaConsumerRuntime`, not registered as a global KafkaTemplate, ProducerFactory, KafkaMessagePublisher, OutboxPublisher or CommonErrorHandler bean. Consumer-only mode therefore needs no publisher topic or general Outbox publication. Standard Boot producer infrastructure may still exist. Outbox append remains available independently.

The DLT producer clones Boot's DefaultKafkaProducerFactory (including its client customizations), uses effective source bootstrap endpoints, and forces String serialization, acks=all, idempotence, compatible retries/in-flight limits, and bounded send timings. Weak general-producer settings do not weaken DLT confirmation. Custom factories must support the standard copy operation and yield DefaultKafkaProducerFactory. Kafka-transactional producer factories are rejected in this DB-Inbox mode. These restrictions affect only opt-in reliability.

## Policy and configuration

All knobs below have prefix `apex.messaging.consumer.reliability`:

| Property | Default | Meaning / validation |
|---|---|---|
| `enabled` | `false` | Explicit reliable-mode migration |
| `max-attempts` | `3` | Initial handler delivery included; 1–100 |
| `retry-delay` | `1s` | Fixed retry delay and recovery-failure backoff; 1ms–60s |
| `send-timeout` | `10s` | Private producer delivery timeout and max metadata-block time; 1ms–60s |
| `dlt-suffix` | `-dlt` | Explicit source-topic suffix; 1–64 Kafka-safe characters; total topic length ≤249 |
| `preflight-enabled` | `true` | Before listener startup, describe sources/DLTs and check partition capacity |
| `preflight-timeout` | `10s` | Provisioning query timeout; 1ms–60s |

The recoverer's positive-send wait is at least `send-timeout` and includes a 1s buffer over producer delivery timeout. A metadata-blocking send can precede this wait. Budget `max.poll.interval.ms` for these timings plus handler execution/retry work. Producer network retries are time-bounded by delivery timeout, separate from the handler attempt count.

HANDLED and Inbox DUPLICATE are ACKed after dispatch returns. Unsupported type/version or a custom dispatcher's IGNORED result throws `UnsupportedEventContractException` in reliable mode. Malformed JSON/metadata/payload throws a non-retryable event exception. These permanent contract failures go directly to DLT. Ordinary handler/DB failures get the configured bounded attempt cycle; changing exception classes does not reset this budget. Failures are never changed into a business rejection.

After exhaustion, the recoverer sends the original key/raw String value to `<source-topic><dlt-suffix>`, using the original partition. `setFailIfSendResultIsError(true)` and a send-result wait prevent logging-only recovery. `setCommitRecovered(true)` commits the source offset only after recovery succeeds, with MANUAL_IMMEDIATE. Failed/timeout DLT send throws; the failed record remains uncommitted and is redelivered. Recovery failures restart retry cycles with backoff: there is deliberately no overall discard limit while quarantine remains unavailable. This can block a consumer and delay other partitions assigned to it.

There is no atomic Kafka DLT-write/source-offset transaction. If DLT write succeeds and source commit fails, DLT duplicates are possible. HANDLED DB commit followed by source ACK failure is protected by the existing Inbox/business idempotency on redelivery. No Kafka exactly-once guarantee is claimed.

## Provisioning and evidence

For the Saga profile, explicitly provision:

| Source | DLT | Service group |
|---|---|---|
| `apex.order.events.v1` | `apex.order.events.v1-dlt` | `inventory-cell-order-placed-v1` |
| `apex.inventory.events.v1` | `apex.inventory.events.v1-dlt` | `order-cell-inventory-outcome-v1` |

DLT must have at least as many partitions as the source. Partition selection does not fall back to another partition. Operators own replication, min ISR, retention, cleanup policy and ACLs: the service needs source/group read, metadata describe and DLT write permissions; operators need authorized DLT inspection/replay access. Preserve raw-event confidentiality. Prefer a retention/cleanup policy that preserves quarantine records for the required recovery window. Durability is within that broker retention/replication policy, not indefinite archival. One-replica disposable tests do not prove production failover resilience.

Preflight never creates topics. Setting `preflight-enabled=false` is intended for controlled diagnostics/tests; it does not weaken runtime failed-send behavior. Missing/unavailable DLT blocks successful recovery and source commit until provisioning/connectivity/permissions are restored. Topic auto-creation is not a recovery strategy.

DLT retains the raw envelope (eventId/correlationId/causationId/occurredAt included) and key, plus Spring Kafka original topic, partition, offset, timestamp and consumer-group headers. Exception FQCN and a safe message are added; business/Jackson exception details are redacted and no new exception stacktrace header is emitted. Original business data is still present in the raw record, so protect DLT and application logs appropriately.

## Verification and operator handoff

`ReliableConsumerIntegrationTest` runs native disposable PostgreSQL and KRaft Kafka with explicit topics, source partition 1 and matching DLT partitions. It checks exact raw/key/header preservation, broker offsets, real commit rollback, missing DLT recovery, restart, and consumer-only behavior. Existing Platform DDL is installed only into that test database. It creates no new migration. `ConsumerReliabilityWiringTest` covers roles, settings, legacy compatibility and Boot customizers.

Run the existing isolated Cell workflow from the repository root:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File inventory-cell/tools/verify-saga.ps1 -Offline -MavenCommand mvn.cmd
```

The two-child-JVM test now explicitly provisions both DLTs and proves permanent business/unsupported-version quarantine and committed offsets, alongside all existing Saga assertions. Base application.yml values remain unchanged; only opt-in Saga profiles enable reliability.

M26.9 replay must be operator-triggered, audited and bounded. Inspect a chosen DLT topic/partition/offset using an independent read-only observer with auto-commit disabled; do not reset application group offsets. Resolve the cause, authorize a limited replay batch, preserve original event identity/metadata/key/raw value and record replay provenance separately. Expect duplicate DLT entries and duplicate deliveries; use origin coordinates/eventId and existing Inbox/business idempotency. No DLT consumer or startup replay loop is provided here.

API reference checked against resolved Spring Kafka 4.1.1 classes/sources: [Spring Kafka exception handling reference](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html). The live reference currently identifies 4.1.2; exact APIs and behavior here were verified locally against 4.1.1 and by real-broker tests.
