# Operator-controlled DLT replay

Independent Java 21 / Boot and Spring Kafka 4.1.1 CLI. Commands are human-initiated:
**inspect → plan → approve → execute → status/reconcile**. No automatic replay, startup
listener, source-topic publication or consumer-group offset reset exists.
`PUBLISHED_TO_REPLAY_TOPIC` means broker receipt, not business completion.

## Build and isolated tests

From the repository root:

```powershell
mvn -B -ntp -o -f apex-replay-tool/pom.xml clean verify
powershell.exe -NoProfile -ExecutionPolicy Bypass -File inventory-cell/tools/verify-saga.ps1 -Offline -MavenCommand mvn.cmd
```

The tool tests use disposable KRaft/native PostgreSQL and real packaged CLI processes.
The Saga runner clean-installs Platform, verifies Order, verifies/packages the CLI, then
verifies Inventory including the original Saga and bidirectional replay with two packaged
Cell JVMs. No Docker/shared service is needed. Remove offline flags only for downloads.

## Explicit environment and audit database

Provide `REPLAY_AUDIT_DB_URL`, `REPLAY_AUDIT_DB_USERNAME`, `REPLAY_AUDIT_DB_PASSWORD`
through an approved secret mechanism. The audit database must be separate from both Cells.
Kafka commands also require `REPLAY_KAFKA_BOOTSTRAP_SERVERS`; optional
`REPLAY_KAFKA_PROPERTIES_FILE` supplies Java properties for TLS/SASL. No network default
exists. Keep passwords/properties outside Git, URLs, actor/reason values and CLI arguments.
Protect Kafka client logs, which can include connection metadata. Raw payloads and nested
exceptions are suppressed by the CLI; inspect always redacts exception message text.

```powershell
java -jar apex-replay-tool/target/apex-replay-tool.jar init-audit
```

Only this explicit command installs/upgrades tool-local `db/replay` V1/V2. Applied V1 is
unchanged; existing plans/approvals survive V2. Other commands never migrate implicitly.
Unknown public tables cause refusal, but operators remain responsible for choosing a
separately provisioned audit DB. Use distinct migration and restricted runtime DB roles.

## Eight-topic topology and opt-in subscriptions

`routes.example.json` is the explicit whitelist. Provision these existing source/DLT pairs:
`apex.order.events.v1` / `apex.order.events.v1-dlt` and
`apex.inventory.events.v1` / `apex.inventory.events.v1-dlt`.

| Target / existing group | Dedicated replay / replay DLT |
|---|---|
| Inventory / `inventory-cell-order-placed-v1` | `apex.inventory.order-placed.replay.v1` / `apex.inventory.order-placed.replay.v1-dlt` |
| Order / `order-cell-inventory-outcome-v1` | `apex.order.inventory-outcome.replay.v1` / `apex.order.inventory-outcome.replay.v1-dlt` |

Topics are never auto-created by the tool. Source DLT >= source partitions, replay >=
source partitions, replay DLT >= replay partitions. Preserve original partition, including
nonzero partitions; no fallback. Replay topics must be distinct and target-specific.

In separately authorized disposable/manual environments, provide existing Cell DB/Kafka
variables and launch in separate terminals:

```powershell
java -jar order-cell/target/order-cell.jar --spring.profiles.active=saga,replay
java -jar inventory-cell/target/inventory-cell.jar --spring.profiles.active=saga,replay
```

Both profiles are required for replay configuration. Base YAML and group IDs are unchanged.
Optional `APEX_ORDER_REPLAY_TOPIC` / `APEX_INVENTORY_REPLAY_TOPIC` and existing source topic
variables must match the route file. The same APEX listener/registry/transactional Inbox/
handler/reliable DLT policy processes both topics. A shared canonical parser validates and
trims exact CSV names for listener and preflight; empty/invalid/duplicate names fail startup.
No wildcard/bypass listener exists. ACLs must restrict replay subscriptions to intended groups;
a whitelist alone cannot prove external deployment/ACL correctness.

### M29 full three-Cell recovery

`three-cell-routes.example.json` adds explicit Payment-request, Payment-outcome and
Inventory-release-request routes and permits INVENTORY_RELEASED on the existing
Inventory-outcome route. Only the listed V1 contract families are accepted. Old
two-Cell routes and already-approved immutable plans retain their original contract sets.
One group may own several sources; each `(originTopic,targetGroup)` route and each replay
destination must still be unique. A plan selects **both** target group and replay topic.
An origin DLT's group header must match; it is never broadcast to other groups.

Use `saga,three-cell-saga,replay` for Order/Inventory and
`three-cell-saga,simulation,replay` for Payment. Provision every subscribed replay topic
and its `-dlt` first, with source-compatible partition capacity. No default profile or
shared broker is modified. See the [M29 operational guide](../inventory-cell/tools/OPERATIONAL_RECOVERY.md)
for the five-route map, protected stalled view, owner-specific checks and complete
inspect/plan/approve/execute/reconcile commands. The existing CLI approval, digest,
broker-confirmation and UNKNOWN/no-resend rules below are unchanged.

## Tested command syntax

All shown options are mandatory; unknown/duplicate options refuse. Every Kafka command
requires `--allow-kafka-interaction`. Example coordinates below require substitution from
an authorized inspection; they are not shared-infrastructure operations.

```powershell
java -jar apex-replay-tool/target/apex-replay-tool.jar inspect --routes apex-replay-tool/routes.example.json --dlt-topic apex.order.events.v1-dlt --partition 1 --offset 7 --allow-kafka-interaction
```

Inspect uses manual assignment/seek without group membership or commits. Copy its eventId
and raw SHA-256 into `out/replay-selector.json`:

```json
[{"dlt":{"topic":"apex.order.events.v1-dlt","partition":1,"offset":7},"eventId":"<inspection UUID>","rawSha256":"<inspection lowercase SHA-256>"}]
```

```powershell
java -jar apex-replay-tool/target/apex-replay-tool.jar plan --routes apex-replay-tool/routes.example.json --selector out/replay-selector.json --target-group inventory-cell-order-placed-v1 --replay-topic apex.inventory.order-placed.replay.v1 --actor operator-id --reason "Reviewed technical failure" --limit 10 --allow-kafka-interaction
java -jar apex-replay-tool/target/apex-replay-tool.jar approve --plan-id <plan-UUID> --confirm-digest <digest> --actor approver-id --reason "Explicit plan approval"
java -jar apex-replay-tool/target/apex-replay-tool.jar execute --plan-id <plan-UUID> --confirm-digest <digest> --routes apex-replay-tool/routes.example.json --target-group inventory-cell-order-placed-v1 --replay-topic apex.inventory.order-placed.replay.v1 --actor operator-id --reason "Recover selected event" --failure-resolved "Technical cause removed and checked" --max-records 10 --rate 10 --attempts 1 --allow-kafka-interaction
java -jar apex-replay-tool/target/apex-replay-tool.jar status --plan-id <plan-UUID>
java -jar apex-replay-tool/target/apex-replay-tool.jar reconcile --plan-id <plan-UUID> --confirm-digest <digest> --routes apex-replay-tool/routes.example.json --target-group inventory-cell-order-placed-v1 --replay-topic apex.inventory.order-placed.replay.v1 --actor operator-id --reason "Positive broker evidence review" --from-offset 0 --max-scan 1000 --allow-kafka-interaction
```

For Order, substitute its group/replay topic. Actor values are **unverified identity claims**,
not authenticated RBAC. Separate approval does not enforce two-person control. Failure-resolved
is an operator attestation, not machine proof; an unresolved fault can quarantine the replay.
External authorization and actual root-cause/handler deployment checks remain mandatory.

## Validation, bounds and durable audit

Plans bind immutable source/DLT coordinates, routes, eventId/type/version, raw SHA/length,
key SHA/null and ordered-header SHA. Raw body/key are not stored. Missing/conflicting origin
headers, mismatched topic/group/hash/key/eventId, unknown/V99, malformed metadata, expired
DLT coordinates, missing/undersized topics or unavailable audit refuse sends. Mandatory DLT
headers are `kafka_dlt-original-topic`, `-original-consumer-group` (UTF-8),
`-original-partition` (4-byte big-endian) and `-original-offset` (8-byte big-endian).
Only original-source DLTs and approved Saga V1 contract sets are supported. This allowlist
is not proof of business DTO validity or deployed handler coverage.

Selection hard cap is ten before dedupe. Duplicate DLT copies dedupe deterministically;
conflicting identities and same eventId at different offsets require manual review. Unique
audit constraints reject another plan for an already planned origin/eventId/group. No
cancel/replan/override command exists. Execute requires the whole plan fit max-records
(1–10 and <= approved limit), rate 1–100 records/second and attempts exactly 1. Producer
internal idempotent retries are time-bounded; they are not automatic application replay.

V1 plans/items/approvals remain immutable. V2 adds unique durable run claims, deliveries
and append-only execution/receipt actions. Execute revalidates the entire batch before
claim, and each source again before send. PUBLISHING intent commits first; exact original
bytes/key and original partition are sent with acks=all/idempotence and bounded timeout.
Business headers are copied; `kafka_dlt-*` and prior `apex-replay-*` headers are stripped.
New safe provenance headers: `apex-replay-marker`, `-plan-id`, `-event-id`, `-origin`, `-dlt`,
`-target-group`, `-attempt`. The stable marker binds plan/event/origin. No stack trace is copied.
Only positive broker metadata can mark PUBLISHED and append the receipt atomically.

Send exceptions conservatively become PUBLISH_UNKNOWN. Crash/audit outage may leave
PUBLISHING. Both block automatic resend. Completely published execute is a no-op. A durable
run claim is never reclaimed/expired automatically, including interrupted batches with
unsent items. Such cases require engineering review; no unsafe resume/reset command exists.
PostgreSQL audit and Kafka are not atomic, and producer idempotence does not provide
exactly-once cross-process replay. Inbox/business keys protect duplicate effects. Owners/
superusers can bypass audit triggers; this is not a tamper-proof external ledger.

Reconcile independently scans the approved replay partition from the requested offset,
bounded to 1–10000 offsets and ten seconds. Exact marker + raw SHA/length + key SHA/null
must match. Positive evidence persists coordinates with RECONCILED action; repeats are
idempotent. No match leaves UNKNOWN with NO_EVIDENCE: absence/retention expiry is never
proof of non-delivery. Widen a justified search or escalate; never blindly resend, delete
audit rows or reset source offsets. Inspect/status do not mutate the audit database.

## Business verification and operations

After broker publication, independently verify the owning Cell Inbox/group/eventId,
reservation/projection/outcome, Order HTTP status and replay DLT. Tool status deliberately
keeps business processing NOT_VERIFIED. A handler failure follows M26.8 retries into the
replay topic's own DLT before recovered offset commit; no forwarding/replay loop exists.

Operators need Describe on route topics, Read on DLT/replay evidence, Write and appropriate
idempotent-producer permission on replay. Cells need replay Read/group permission and replay
DLT Write. Audit runtime needs restricted SELECT/INSERT, approval/delivery UPDATE and action
sequence usage; migration role owns DDL. Explicitly configure TLS/ACLs, replication/min ISR,
retention and finite DLT durability. Production identity/RBAC, live ACL-denial/TLS/failover
qualification and production readiness are outside this milestone. No shared infrastructure
operation is authorized by these examples.
