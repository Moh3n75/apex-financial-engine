# APEX Cell Messaging Starter

Add `com.apex:apex-starter-cell-messaging:${apex.platform.version}` to an independently built Spring Boot 4.1.1 / Java 21 Cell.
The existing `apex-platform-messaging` dependency supplies Events, Core, Context auto-configuration,
Boot Kafka/JDBC/Jackson integration and the PostgreSQL driver transitively. No WebMVC dependency is added.
This library aggregator defines no production Java classes, alternative runtime or additional auto-configuration imports.

The existing `PlatformCellMessagingAutoConfiguration` and all public APIs remain responsible for runtime behavior.
Enable Cell Messaging explicitly with `apex.messaging.cell.enabled=true`.

| Role | `apex.messaging.outbox.publisher.enabled` | `apex.messaging.consumer.enabled` |
|---|---|---|
| Producer | `true` | `false` |
| Consumer | `false` | `true` |
| Both | `true` | `true` |
| Append only | `false` | `false` |

When Cell Messaging is enabled, the Publisher default remains `true` and the Consumer default remains `false`.
Publishing requires `apex.messaging.publisher.topic`. Consuming requires `spring.application.name`, `apex.cell.id`,
`apex.messaging.consumer.topics` and `spring.kafka.consumer.group-id`; a publisher topic is not required for a Consumer-only role.
Supply the Cell-owned DataSource, Kafka connection settings and infrastructure migrations through normal deployment configuration.
There are no embedded database, broker or fallback publisher Beans in production.

`CellEvents`, `OutboxAppender` and `EventEnvelopeFactory` remain available in all four enabled-Cell modes.
Context auto-configuration remains independent of the messaging flags.
**Disabling publication does not discard or publish appended Outbox events. An explicitly enabled publishing path is required for eventual delivery.**
The Starter does not provision topics, install migrations automatically, change groups, or introduce a new delivery path.
Applications independently choose Flyway, validation, Actuator and HTTP dependencies as needed.
