# Operator DLT approval tool — M26.9 PARTIAL

Implemented: **inspect → plan → approve → status**, with independent PostgreSQL audit.
`execute` and `reconcile` always exit 2 with `NOT_IMPLEMENTED_NO_PUBLICATION_AVAILABLE`,
before opening any infrastructure connection. Approval does not publish anything.
There is no producer, subscription, scheduled job, source offset commit/reset or automatic replay.
Do not use this partial delivery to claim recovery of a business operation.

## Build and isolated verification

From the repository root, Java 21 and Maven 3.9.x:

```powershell
mvn -B -ntp -o -f apex-replay-tool/pom.xml clean verify
```

Remove `-o` only if dependency downloads are needed. The standalone project uses Boot's
4.1.1 dependency management, Spring Kafka 4.1.1, Kafka clients 4.2.1, Jackson 3 and
Flyway PostgreSQL. It imports no Cell, Starter or Platform runtime and is outside the
root reactor. Tests start their own native EmbeddedPostgres/KRaft resources, with
broker topic auto-creation disabled; no Docker or inherited deployment connection is used.
The integration-test phase invokes the actual packaged operator JAR in child JVMs.
Those processes are **not** an Order/Inventory replay E2E.

## Explicit connection setup

Obtain authorization and provision a **separate, empty operator-owned PostgreSQL
database**. Never supply a Cell database. `init-audit` refuses a populated public
schema unless it contains only this tool's recognized audit tables/history and marker.
It installs only `db/replay/V1__replay_approval_audit.sql`; inspect/status/approve/plan
never run migrations implicitly. This public-schema check is a guard, not proof of
exclusive database ownership; operators must provision a dedicated database/role.

Supply these environment variables through your approved secret mechanism:

| Variable | Required by | Default |
|---|---|---|
| `REPLAY_AUDIT_DB_URL` | init-audit, plan, approve, status | None; PostgreSQL JDBC URL, no credentials embedded |
| `REPLAY_AUDIT_DB_USERNAME` | same | None |
| `REPLAY_AUDIT_DB_PASSWORD` | same | None; explicit empty value permitted for isolated tests |
| `REPLAY_KAFKA_BOOTSTRAP_SERVERS` | inspect, plan | None |
| `REPLAY_KAFKA_PROPERTIES_FILE` | optional for inspect/plan | None; Java properties file for TLS/SASL settings |

Keep credentials/properties outside Git. Do not put passwords in URLs, command arguments,
actor/reason values or route/selector manifests. The CLI suppresses nested exception details
and never prints raw envelopes. Kafka clients may log connection metadata; protect logs.
Every Kafka command requires `--allow-kafka-interaction`. This is operator opt-in, not
permission to access shared infrastructure; all implementation validation used temporary resources.

## Tested CLI syntax

The following commands show the exact supported argument structure. Placeholder values
must come from an authorized environment and inspection; examples are not provisioning commands.
All options shown are mandatory, including `--limit`; unknown/duplicate options are rejected.
Use PowerShell's actual backtick continuation or write each command on one line.

```powershell
java -jar apex-replay-tool/target/apex-replay-tool.jar init-audit

java -jar apex-replay-tool/target/apex-replay-tool.jar inspect `
  --routes apex-replay-tool/routes.example.json `
  --dlt-topic apex.order.events.v1-dlt --partition 1 --offset 7 `
  --allow-kafka-interaction
```

Inspection assigns/seeks the exact DLT coordinate without a group ID, subscription or
commit. It returns coordinates, raw SHA-256/byte length, parseable event identity,
origin and exception class. Exception text is always redacted. Broken provenance is
flagged, never fabricated. Inspection accepts only DLTs listed in the explicit route file.

Create a selector manifest outside publishable source (e.g. ignored `out/replay-selector.json`):

```json
[
  {
    "dlt": {"topic": "apex.order.events.v1-dlt", "partition": 1, "offset": 7},
    "eventId": "<UUID returned by inspect>",
    "rawSha256": "<64 lowercase hex characters returned by inspect>"
  }
]
```

```powershell
java -jar apex-replay-tool/target/apex-replay-tool.jar plan `
  --routes apex-replay-tool/routes.example.json --selector out/replay-selector.json `
  --target-group inventory-cell-order-placed-v1 `
  --replay-topic apex.inventory.order-placed.replay.v1 `
  --actor operator-id --reason "Reviewed transient database failure" `
  --limit 10 --allow-kafka-interaction

java -jar apex-replay-tool/target/apex-replay-tool.jar approve `
  --plan-id <returned-plan-UUID> --confirm-digest <returned-digest> `
  --actor approver-id --reason "Explicit approval after inspection"

java -jar apex-replay-tool/target/apex-replay-tool.jar status --plan-id <returned-plan-UUID>
```

Approval is a separate durable command, under a PostgreSQL row lock. Wrong digest,
missing plan, repeated/concurrent approval fail. Actor values are **unverified CLI
claims**, not authenticated RBAC, and separation of commands does not enforce a
two-person approval policy. Kafka ACLs and dedicated DB roles enforce external access.
Status distinguishes APPROVED from `NOT_IMPLEMENTED_ZERO_PUBLICATIONS` and
business processing `NOT_VERIFIED`. No failure-resolved execute attestation is accepted
because publication is unavailable in this increment.

## Validation and audit policy

The route file is an explicit operator-owned whitelist; preserve production group identities.
The supplied example maps the two approved Saga streams. Renaming entries cannot deploy
handlers or subscribe Cells. Only `ORDER_PLACED V1` for Inventory, or the two Inventory
outcome V1 contracts for Order, are accepted. V99, unknown types, malformed envelopes,
missing/conflicting headers, wrong original topic/group, mismatched selection hash/event ID
or DLT coordinates all refuse planning. This is structural metadata validation and a local
contract allowlist, **not proof of deployed handler support or business payload validity**.
Business conflicts require reconciliation, not blind replay.

Mandatory Spring Kafka headers (verified against 4.1.1):

| Header | Encoding |
|---|---|
| `kafka_dlt-original-topic` | UTF-8 |
| `kafka_dlt-original-consumer-group` | UTF-8 |
| `kafka_dlt-original-partition` | 4-byte big-endian int |
| `kafka_dlt-original-offset` | 8-byte big-endian long |

Exactly one of each is required. DLT partition must match original source partition.
Metadata-only preflight describes source, source DLT, replay and replay DLT, creating none.
Replay partitions must be at least the entire source partition count; replay DLT partitions
must be at least replay count. The original partition must actually exist on the source.
The eventual publisher must preserve that partition; no fallback is authorized.

The hard cap is ten selected coordinates, counted before deduplication. Duplicate DLT
copies of one origin/event/key/raw value select the lowest DLT coordinate deterministically.
Conflicting hashes/keys/event IDs fail. Different origin offsets with the same eventId
require manual review. Globally unique audit constraints also reject a second plan for
an already planned origin or eventId in the same target group; they never silently resend.
There is no cancel/replan or exception-to-dedupe command in v1.

Plan JSON includes immutable route, actor/reason/limit, source/DLT coordinates, raw SHA/length,
key SHA/null marker and exact ordered-header fingerprint; the raw body/key are not stored.
Items are canonically sorted and JSON/digest verified again on approval/status. DB triggers
protect immutable plans/items and append-only PLAN/APPROVAL actions. All inserts/approval
and their audit actions commit atomically. Provision separate migration and restricted
operating roles; database superusers/owners can bypass these protections. This is not a
tamper-proof external ledger. Audit text may itself contain operator-supplied sensitive data;
retention/access control remain operator responsibilities.

## Topology, ACLs and remaining implementation

Future opt-in topology is **eight** explicitly provisioned topics: two sources, two source
DLTs, two replay topics and two replay DLTs, listed in `routes.example.json`.
No existing Cell configuration or subscription is modified by this partial delivery.
Current M26.8 preflight splits CSV but the listener annotation does not: the resolved
Spring `resolveTopics/resolveAsString` treats a property String as one topic. That mismatch
must be fixed and tested before enabling multi-topic replay subscription. Use existing
group, transactional dispatcher/Inbox and reliable DLT policy, never a bypass listener.

Operators need Describe on relevant topics and Read on source DLTs; this phase has no
group-offset access requirement at runtime. Audit role needs SELECT/INSERT plus the
constrained approval UPDATE, action sequence usage; the migration role owns DDL. Future
execute additionally needs replay Write/idempotent-producer permissions. Cells need Read
on replay topics using their existing group, and Write to replay DLT. Configure partition
counts, replication/min ISR, retention, encryption and ACL boundaries explicitly. DLT
durability is finite within broker retention/replication policy, not indefinite archival.

**NOT IMPLEMENTED:** broker-confirmed execute, rate/batch execution limits, item claims,
PUBLISHING/PUBLISH_UNKNOWN reconciliation, sanitized replay headers, replay subscriptions,
operator reconciliation command and positive two-Cell replay E2E. Do not approve a rollout
based on this partial result. Before adding execute, implement durable PUBLISHING-before-send,
positive ACK coordinates, unknown/crash blocking without automatic retransmit, exact raw/key
revalidation, failure-cause-resolved attestation, and independent business verification.
PostgreSQL audit and Kafka publish will not be atomic. An ambiguous send must stop for manual
reconciliation; approval alone must never be reported as broker delivery or business success.
