APEX topic provisioning is opt-in for Cells using `apex.messaging.cell.enabled=true`:

```yaml
apex:
  messaging:
    provisioning:
      enabled: true
      partitions: 1
      replication-factor: -1
      timeout: 10s
spring:
  kafka:
    admin:
      auto-create: false
    consumer:
      properties:
        allow.auto.create.topics: false
```

Keep broker `auto.create.topics.enable=false`. Replication factor `-1` uses the broker's
configured default, including production replication policies. An explicit positive
replication factor must be supported by the connected cluster. The partition and
replication settings apply only to newly created topics; existing topics are never
altered. DLTs use at least the source's actual partition count.

The authorized plan uses the enabled Outbox publisher's topic, enabled validated
contract routing destinations, and enabled consumer's exact topic list. Reliable
consumers also authorize their source topics plus the existing configured DLT suffix.
No topics are inferred from event payloads, handler names, or Kafka's topic listing;
replay topics require an explicit authorized publisher/consumer declaration.

Provisioning finishes during singleton initialization, before reliability preflight
and listener lifecycle startup. Reliability preflight remains enabled by default and
executes independently. Missing topics require Kafka Create and Describe permissions;
existing topics require Describe. Failures abort startup and identify the affected
topic without retaining Kafka exception details. Concurrent creators accept only
TopicExists as an idempotent success, then validate actual metadata. Incompatible
DLT partitions require an operator to correct the topic explicitly.

With provisioning disabled (the default), no provisioning bean or additional Kafka
Admin connection is created and existing startup behavior is preserved.
