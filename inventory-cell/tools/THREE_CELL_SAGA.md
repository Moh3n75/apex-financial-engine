# M27/M28 three-process simulated-payment Saga

This is an executable business demonstration, not a payment system. No server-owned
prices, currency or monetary amount exist in these Order contracts. Real PSP
authorization/capture/refund, pricing, timeout/reconciliation and financial controls
require separate work. Never run simulation policies against real payment operations.

## Repeatable isolated verification

From the repository root, with Java 21 and Maven installed/cached:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File inventory-cell/tools/verify-three-cell-saga.ps1 `
  -Offline -MavenCommand mvn.cmd
```

Omit `-Offline` if dependencies need downloading. The runner invokes the existing
M22 and M26/replay runners, builds/verifies Payment, records its JAR hash, then runs
Inventory with `-Pthree-cell verify`. Platform/Starter and independently packaged
Cell artifacts are rebuilt. Existing Order source/JAR receipts remain enforced.
No test skips, shared service dependencies, Git staging or deployment are performed.

The fixture starts real packaged Order, Inventory and Payment child JVMs, three
separate native PostgreSQL servers, and a disposable KRaft broker. It explicitly
creates twelve source/DLT topics with broker auto-creation disabled. It shuts them
down in `finally`. Existing bidirectional replay regression separately uses two Cell
DBs plus a third operator audit DB; that audit DB is not a fourth business Cell DB.

Evidence is generated under `inventory-cell/target/three-cell-evidence.json`,
`three-*.log`, `three-cell-reports/`; existing `saga-e2e-reports/` and replay evidence
remain separate. Do not commit generated target/log files.

## Operator-owned local demonstration

Provision three distinct empty, disposable PostgreSQL databases/roles and an owned
Kafka broker. Do not use existing development or production data. Configure Kafka
with auto topic creation disabled. Provision these topics explicitly before startup:

| Stream | Writer | Subscribed Cell / stable default group |
|---|---|---|
| `apex.order.events.v1` | Order: ORDER_PLACED | Inventory / `inventory-cell-order-placed-v1` |
| `apex.order.payment-requests.v1` | Order: ORDER_READY_FOR_PAYMENT | Payment / `payment-cell-authorization-v1` |
| `apex.order.inventory-release-requests.v1` | Order: ORDER_CANCELLATION_REQUESTED | Inventory / same group |
| `apex.inventory.events.v1` | Inventory: RESERVED, REJECTED, RELEASED | Order / `order-cell-inventory-outcome-v1` |
| `apex.payment.events.v1` | Payment: AUTHORIZED, DECLINED | Order / same group |
| `apex.order.legacy.events.v1` | Order: legacy ORDER_CREATED in full Saga profile | No Saga consumer |

For each topic also provision `<topic>-dlt` with at least the source partition count.
One partition and replication factor one are sufficient **only for a disposable
single-broker demo**. Set production retention/replication/ACLs separately. Example
for one topic, after explicitly selecting your owned broker and Kafka tooling:

```powershell
& "$env:KAFKA_HOME/bin/windows/kafka-topics.bat" --bootstrap-server $env:KAFKA_BOOTSTRAP_SERVERS `
  --create --topic apex.order.payment-requests.v1 --partitions 1 --replication-factor 1
& "$env:KAFKA_HOME/bin/windows/kafka-topics.bat" --bootstrap-server $env:KAFKA_BOOTSTRAP_SERVERS `
  --create --topic apex.order.payment-requests.v1-dlt --partitions 1 --replication-factor 1
```

Repeat for the table's other streams. These manual provisioning commands are not
executed by application startup. Reliable consumer preflight refuses missing subscribed
topics/DLTs. Missing producer destinations fail Outbox publication; they are not marked
published. Do not change routing while undelivered Outbox rows exist: routing is resolved
from configuration at each send, not snapshotted into each row. Drain or explicitly
review a pending-row migration before renaming any route or stream.

Supply only your own values in the appropriate process environment:

| Process | Required variables | Profiles / default port |
|---|---|---|
| Order | `ORDER_DB_URL`, `ORDER_DB_USERNAME`, `ORDER_DB_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS` | `saga,three-cell-saga` / 8083 |
| Inventory | `INVENTORY_DB_URL`, `INVENTORY_DB_USERNAME`, `INVENTORY_DB_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS` | `saga,three-cell-saga` / 8084 |
| Payment | `PAYMENT_DB_URL`, `PAYMENT_DB_USERNAME`, `PAYMENT_DB_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS` | `three-cell-saga,simulation` / no HTTP |

Optional stable group overrides: `APEX_ORDER_CONSUMER_GROUP`,
`APEX_INVENTORY_CONSUMER_GROUP`, `APEX_PAYMENT_CONSUMER_GROUP`. Do not change existing
group identities or reset offsets to perform recovery. Use CLI `--server.port` if needed.
No committed credential or workstation endpoint is needed. In three separate terminals:

```powershell
java -jar order-cell/target/order-cell.jar --spring.profiles.active=saga,three-cell-saga
java -jar inventory-cell/target/inventory-cell.jar --spring.profiles.active=saga,three-cell-saga
# Start Payment only after seeding the explicit per-order decision described below.
java -jar payment-cell/target/payment-cell.jar --spring.profiles.active=three-cell-saga,simulation
```

Flyway installs local Platform tables in each owning database. Inventory needs no new
migration. Order adds V2000 decision facts; do not automatically enable full Saga on old
in-flight rows whose new facts are NULL. Operator migration/reconciliation is required;
the implementation does not guess whether an old CANCELLED order was reserved/released.
No applied migration checksum or Flyway history should be changed.

Deploy this as a coordinated opt-in feature. Do not roll a single Cell back to legacy
handlers while new payment/release requests or outcomes remain pending/retained for
its group: a legacy listener correctly quarantines unsupported contracts. Pause,
drain/reconcile the workflow and review routes/offset ownership first. Adding nullable
columns is schema-compatible; it does not make mixed old/new orchestration behavior safe.

## Submit and inspect

Start Payment once to install its schema, stop it gracefully, then seed Inventory in its
own disposable database using your PostgreSQL client (connection secrets stay local):

```sql
INSERT INTO inventory_stock(product_id, available_quantity, reserved_quantity)
VALUES ('11111111-1111-4111-8111-111111111111', 10, 0);
```

With Order and Inventory running, submit:

```powershell
$body = @{
  externalReference = [guid]::NewGuid().ToString()
  description = 'isolated simulated-payment demo'
  items = @(@{ productId = '11111111-1111-4111-8111-111111111111'; quantity = 2 })
} | ConvertTo-Json -Depth 4
$placed = Invoke-RestMethod -Method Post -Uri http://localhost:8083/api/v1/order-placements `
  -ContentType 'application/json; charset=utf-8' -Body $body
$placed
```

HTTP 201 returns `orderId` and `PENDING_INVENTORY`; it does not prove completion. In
Payment's own DB seed `payment_simulation_policy(order_id,decision)` for this exact
returned ID as `AUTHORIZED`, then start Payment. Poll with a bounded timeout:

```powershell
$deadline = (Get-Date).AddSeconds(45)
do {
  $current = Invoke-RestMethod "http://localhost:8083/api/v1/order-placements/$($placed.orderId)"
  if ($current.status -in @('CONFIRMED','CANCELLED')) { break }
  Start-Sleep -Milliseconds 200
} while ((Get-Date) -lt $deadline)
$current
if ($current.status -notin @('CONFIRMED','CANCELLED')) { throw 'Saga has not converged; inspect owned DBs and DLTs' }
```

SQL inspection (run each query only against its owning DB):

```sql
-- Order
SELECT id,status,inventory_decision,payment_decision,release_acknowledged FROM orders WHERE id='<order-id>';
-- Inventory
SELECT * FROM inventory_stock;
SELECT * FROM inventory_reservations WHERE order_id='<order-id>';
SELECT * FROM inventory_reservation_items WHERE order_id='<order-id>';
-- Payment
SELECT order_id,status,created_at FROM payment_authorizations WHERE order_id='<order-id>';
-- Run separately in each Cell DB to inspect its own technical records.
SELECT event_id,event_type,aggregate_id,correlation_id,causation_id,published FROM outbox_event WHERE aggregate_id='<order-id>';
SELECT event_id,event_type,consumer_group FROM processed_event;
```

For decline, use a fresh product with 10 units, request 3, and seed `DECLINED` for the
new Order ID while Payment is stopped. Observe `CANCELLING` before the release ACK;
eventually Order is CANCELLED, reservation RELEASED, stock restored to 10/0. This
intermediate state may be brief; the isolated test holds a real deferred COMMIT failure
to prove the wait. For insufficient stock request more than available: Order CANCELLED,
no Payment request/decision and no release; stock unchanged.

## Recovery and limits

Technical failure rolls back Inbox/business/Outbox and uses existing bounded retry,
confirmed DLT publication and commit-after-DB semantics. Business DECLINED is a normal
durable outcome. Do not force terminal states manually to hide a technical failure.
Same event ID uses Inbox; another event ID with the same orderId uses business guards.
Payment emits its first decision only. Inventory release of an already RELEASED
reservation may emit another release acknowledgement for liveness, never restore stock
twice. Contradictory/early outcomes fail closed; a CONFIRMED order cannot regress.

Existing `saga,replay` remains compatible. Explicit `saga,three-cell-saga,replay` adds
the dedicated target replay topics to the full subscriptions. They are not enabled globally.
M29 extends operator-controlled replay to Payment requests/outcomes and release requests/
acknowledgements. See [OPERATIONAL_RECOVERY.md](OPERATIONAL_RECOVERY.md) for explicit
provisioning, the expanded whitelist, protected stalled view and the additional packaged
verification runner. No recovery may bypass approval or reset source offsets.

No timeouts, compensation for technical outages, release after payment authorization,
Payment replay subscription, production RBAC, TLS/ACL/failover validation, or real
financial integration is claimed. Transport is at least once; DB Inbox and business
keys/conditional transitions protect effects. Kafka and PostgreSQL are not atomic.
