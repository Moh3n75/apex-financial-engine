# M29: explicit three-Cell operational recovery

Age is an observation, not a business fact. UNKNOWN or a missing response is never
DECLINED. No age-based cancellation, release, reauthorization or automatic replay exists.
Payment remains an explicit per-order **simulation**, with no prices, currency, PSP,
charge, capture, refund or ledger operation.

## Isolated acceptance runner

From the repository root, select Java 21 and your installed/cached Maven launcher:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File inventory-cell/tools/verify-operational-recovery.ps1 `
  -Offline -MavenCommand mvn.cmd
```

Omit `-Offline` only if dependencies must be downloaded. The runner invokes the
unchanged M22/M26/M27/M28 workflows before the new `-Poperational-recovery verify`.
It builds current Platform/Starters, independent Customer/Reporting/Order/Replay/
Inventory/Payment artifacts and checks packaged artifact receipts. Nothing deploys
to shared infrastructure. Tests use native PostgreSQL and embedded KRaft, not Docker.

M29 starts three actual packaged Cell JVMs and a separate packaged CLI JVM per operator
command, with **four** native PostgreSQL servers: Order, Inventory, Payment and the
operator audit database. Only the first three are business Cell databases. Twenty-two
explicit source/DLT/replay/replay-DLT topics include the unused legacy Order stream.
Broker auto-creation is disabled. Source/replay identities, PIDs, SQL/HTTP results,
receipts and offsets are generated in `inventory-cell/target/m29-operational-recovery-evidence.json`.
All resources are task-owned and closed even when assertions fail.

## Opt-in deployment configuration (operator-owned environments only)

Start from the [three-Cell manual](THREE_CELL_SAGA.md), with separate owning DB variables
and `KAFKA_BOOTSTRAP_SERVERS`. Preserve existing groups; replay is not a new logical
business consumer. Provision source topics and `<source>-dlt`, plus every replay pair:

| Source / V1 family | Intended existing target group | Dedicated replay |
|---|---|---|
| `apex.order.events.v1` / ORDER_PLACED | `inventory-cell-order-placed-v1` | `apex.inventory.order-placed.replay.v1` |
| `apex.inventory.events.v1` / INVENTORY_RESERVED, INVENTORY_RESERVATION_REJECTED, INVENTORY_RELEASED | `order-cell-inventory-outcome-v1` | `apex.order.inventory-outcome.replay.v1` |
| `apex.order.payment-requests.v1` / ORDER_READY_FOR_PAYMENT | `payment-cell-authorization-v1` | `apex.payment.authorization-request.replay.v1` |
| `apex.payment.events.v1` / PAYMENT_AUTHORIZED, PAYMENT_DECLINED | `order-cell-inventory-outcome-v1` | `apex.order.payment-outcome.replay.v1` |
| `apex.order.inventory-release-requests.v1` / ORDER_CANCELLATION_REQUESTED | `inventory-cell-order-placed-v1` | `apex.inventory.release-request.replay.v1` |

Each replay has its own `<replay>-dlt`. Source DLT partitions >= source; replay
partitions >= source; replay DLT partitions >= replay. Original partition is preserved;
there is no fallback. Set ACLs/retention/replication explicitly. Do not enable broker
auto-creation or execute provisioning commands on shared resources without separate
authorization. Neither Cell startup nor the CLI provisions topics.

The explicit example whitelist is `apex-replay-tool/three-cell-routes.example.json`.
If any group/topic environment override differs, create a reviewed matching route file
outside deployment secrets. Do not alter an existing approved plan or reuse an unrelated
group. Optional replay topic overrides are `APEX_ORDER_REPLAY_TOPIC`,
`APEX_ORDER_PAYMENT_REPLAY_TOPIC`, `APEX_INVENTORY_REPLAY_TOPIC`,
`APEX_INVENTORY_RELEASE_REPLAY_TOPIC`, `APEX_PAYMENT_REQUEST_REPLAY_TOPIC`.
No per-event remote registry or new router is introduced.

```powershell
java -jar order-cell/target/order-cell.jar --spring.profiles.active=saga,three-cell-saga,replay,operations
java -jar inventory-cell/target/inventory-cell.jar --spring.profiles.active=saga,three-cell-saga,replay
java -jar payment-cell/target/payment-cell.jar --spring.profiles.active=three-cell-saga,simulation,replay
```

These are manual operator commands; this document does not launch them. Missing
simulation policy remains a technical failure, never an implicit approval/decline.

## Read-only stalled view

The `operations` profile is off by default. It requires a secret
`APEX_ORDER_OPERATIONS_READ_TOKEN` (24..500 characters, no whitespace/control characters,
no unresolved placeholder). Supply it from an approved secret store, not CLI arguments
or Git. Optional `APEX_ORDER_STALLED_AGE_THRESHOLD` defaults to `15m` and must be 1ms..365d.
Bind loopback or use an authenticated, authorized TLS operator ingress. The bearer read
credential is a minimal shared-secret gate, not per-user identity/RBAC or production
security certification. It exposes no mutation operation.

```powershell
$headers = @{ Authorization = "Bearer $env:APEX_ORDER_OPERATIONS_READ_TOKEN" }
Invoke-RestMethod -Headers $headers `
  'http://localhost:8083/internal/operations/stalled-orders?limit=50&offset=0'
```

Only GET is mapped. Missing/wrong credential returns 401; responses are `no-store`.
Limit is 1..100 and offset 0..10000. The response contains observation time, threshold,
page bounds/hasMore and rows with orderId, status, statusEnteredAt, ageMillis,
inventoryDecision, paymentDecision, releaseAcknowledged, local pending/dead Outbox
counts and safe diagnostic codes. It never returns raw events, customer details,
exception text or secrets. `quarantineInspection=NOT_INSPECTED`: the endpoint does
not infer DLT contents or query another Cell/Kafka.

Order V2001 adds nullable `status_entered_at` and local indexes. New inserts/actual
guarded transitions write PostgreSQL `clock_timestamp()` atomically with business
facts/Inbox/Outbox; equivalent duplicates leave it unchanged. This is transition-write
wall time, not a claimed exact COMMIT timestamp. Failed transactions roll it back.
Existing rows keep NULL instead of guessing from `created_at`. Such non-terminal rows
appear as `STATE_ENTRY_TIME_UNKNOWN`, with no invented age. Known rows older than the
threshold are `POTENTIALLY_STALLED`, never definitively failed. Terminal orders disappear.
Pages are bounded live observations, not a durable snapshot across concurrent requests.
No applied migration, Flyway history or outOfOrder flag is changed.

## Diagnose before approving recovery

Use separate authorized credentials for each owning DB. These queries belong to
operator/test tooling; production Cells never query each other's databases.

```sql
-- Order DB only
SELECT id,status,status_entered_at,inventory_decision,payment_decision,release_acknowledged
FROM orders WHERE id='<order-id>';
SELECT event_id,event_type,aggregate_id,published,dead_letter,retry_count,next_retry_at,correlation_id,causation_id
FROM outbox_event WHERE aggregate_id='<order-id>' ORDER BY id;
-- Payment DB only
SELECT order_id,status,created_at FROM payment_authorizations WHERE order_id='<order-id>';
SELECT event_id,event_type,published,dead_letter,correlation_id,causation_id FROM outbox_event WHERE aggregate_id='<order-id>';
-- Inventory DB only
SELECT * FROM inventory_reservations WHERE order_id='<order-id>';
SELECT * FROM inventory_reservation_items WHERE order_id='<order-id>';
SELECT product_id,available_quantity,reserved_quantity FROM inventory_stock WHERE product_id='<product-id>';
-- Separately in the relevant target DB
SELECT consumer_group,event_id,event_type,topic,partition_id,offset_value,processed_at
FROM processed_event WHERE event_id='<original-event-id>';
```

| Observation | Safe next investigation; no automatic business transition |
|---|---|
| Order Outbox pending publication | Inspect publisher health/lease, connectivity, configured route, topic and ACL. Keep eventId/raw metadata; ordinary retry may recover. It is not a consumer DLT event yet. |
| Producer Outbox dead | Explicit engineering/owner review is required; this increment does not add an Outbox reset/requeue API. Never fabricate a Kafka DLT record or mark it published. |
| Consumer fault / source DLT | Inspect exact coordinates and target group; repair the technical cause or deploy the required compatible handler, then approve only that target replay. |
| Payment down or outcome unknown | Inspect Payment's own decision/Outbox and group/DLT. Restart same DB/group when appropriate. AUTHORIZED may already exist; do not authorize again or release stock because Order is waiting. |
| CANCELLING, release request failed, Inventory still RESERVED | Verify trusted Order payment DECLINED fact and original reservation/items; fix technical fault and replay cancellation request to Inventory. |
| CANCELLING, Inventory already RELEASED, acknowledgement failed | Replay original INVENTORY_RELEASED outcome to Order; do not mutate stock again. |
| Trusted terminal DECLINED outcome not yet applied to Order | Recover that original outcome to Order. Only its guarded transaction decides CANCELLING and durably requests release. |
| Unknown/missing/rejected reservation, contradictory terminal outcome, V99 or broken provenance | Stop, reconcile with owners. Do not rewrite payloads, invent facts, broaden versions or blindly replay conflicts. |

For Kafka inspection use authorized read-only topic/group describe commands. Never
reset offsets. Committed source offset after DLT means quarantine recovery, not business
success. A replay ACK/receipt also is not business success: independently check owning
Inbox, authorization/reservation/Order HTTP state, outcome Outbox and replay DLT.

## Actual operator command sequence

Set `REPLAY_AUDIT_DB_URL`, `REPLAY_AUDIT_DB_USERNAME`, `REPLAY_AUDIT_DB_PASSWORD` for a
**separate** operator database and `REPLAY_KAFKA_BOOTSTRAP_SERVERS` explicitly. Optional
`REPLAY_KAFKA_PROPERTIES_FILE` supplies authorized TLS/SASL configuration outside Git.
Run `init-audit` explicitly only for that database. Existing V1/V2 audit schema is reused;
M29 adds no audit or Platform migration. Every Kafka command requires deliberate opt-in.

Example Payment request, substituting inspected coordinates/IDs/digest:

```powershell
java -jar apex-replay-tool/target/apex-replay-tool.jar init-audit
java -jar apex-replay-tool/target/apex-replay-tool.jar inspect --routes apex-replay-tool/three-cell-routes.example.json --dlt-topic apex.order.payment-requests.v1-dlt --partition 0 --offset <dlt-offset> --allow-kafka-interaction
```

Create a small selector outside committed source, with exact inspection values:

```json
[{"dlt":{"topic":"apex.order.payment-requests.v1-dlt","partition":0,"offset":0},"eventId":"<original-uuid>","rawSha256":"<original-lowercase-sha256>"}]
```

```powershell
java -jar apex-replay-tool/target/apex-replay-tool.jar plan --routes apex-replay-tool/three-cell-routes.example.json --selector out/approved-selection.json --target-group payment-cell-authorization-v1 --replay-topic apex.payment.authorization-request.replay.v1 --actor <operator> --reason "Reviewed technical cause" --limit 10 --allow-kafka-interaction
java -jar apex-replay-tool/target/apex-replay-tool.jar approve --plan-id <plan-id> --confirm-digest <digest> --actor <approver> --reason "Explicit immutable plan approval"
java -jar apex-replay-tool/target/apex-replay-tool.jar execute --plan-id <plan-id> --confirm-digest <digest> --routes apex-replay-tool/three-cell-routes.example.json --target-group payment-cell-authorization-v1 --replay-topic apex.payment.authorization-request.replay.v1 --actor <operator> --reason "Recover repaired technical failure" --failure-resolved "Cause removed and owning state verified" --max-records 10 --rate 10 --attempts 1 --allow-kafka-interaction
java -jar apex-replay-tool/target/apex-replay-tool.jar status --plan-id <plan-id>
java -jar apex-replay-tool/target/apex-replay-tool.jar reconcile --plan-id <plan-id> --confirm-digest <digest> --routes apex-replay-tool/three-cell-routes.example.json --target-group payment-cell-authorization-v1 --replay-topic apex.payment.authorization-request.replay.v1 --actor <operator> --reason "Positive publication evidence review" --from-offset 0 --max-scan 1000 --allow-kafka-interaction
```

For other directions substitute **both** group and dedicated replay destination from
the table, and select that source DLT. An approved plan is immutable; changing its route
file cannot retarget it. Exact raw/key/eventId/correlation/causation are revalidated,
preserved and broker-confirmed. Source topic end offsets and source group offsets are
never reset by the tool. Multiple routes for Order/Inventory do not broadcast a record.

PUBLISHED means only `PUBLISHED_TO_REPLAY_TOPIC`; business state stays independently
NOT_VERIFIED in CLI status. UNKNOWN/PUBLISHING after timeout/crash must not be resent.
Reconcile requires positive marker+raw/key evidence; absence/retention expiry is not
proof of non-delivery. Actor and failure-resolution values are operator claims, not
authenticated RBAC or machine proof. Repeated execute is not a retry escape hatch.
No automatic DLT forwarding occurs on restart; failed replay uses its own DLT with
the same reliable policy. Planning another loop from replay DLT is not implemented.

Production TLS/ACL enforcement, multi-broker failover, authenticated identity/two-person
authorization, long-term archival, live PSP reconciliation and historical business-fact
backfill remain separate work. Disposable plaintext tests do not establish production
readiness or exactly-once Kafka delivery.
