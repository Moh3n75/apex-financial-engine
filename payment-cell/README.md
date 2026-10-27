# Payment Cell: explicit authorization simulation

This independent Java 21 / Boot 4.1.1 application consumes `ORDER_READY_FOR_PAYMENT` V1
and publishes `PAYMENT_AUTHORIZED` or `PAYMENT_DECLINED` V1. Each payload is
`{"orderId":"<uuid>"}`. There is no HTTP endpoint, money amount, pricing, currency,
charge, PSP call, capture, settlement, transfer, ledger posting or refund.

Its only APEX dependency is `apex-starter-cell-messaging:1.0-SNAPSHOT`. JDBC, Kafka,
Context, Core and Events arrive transitively. Flyway and its PostgreSQL support are
application dependencies; embedded Kafka/PostgreSQL are test dependencies only.
It is outside the root Maven reactor and builds an executable `target/payment-cell.jar`.

Required environment: `PAYMENT_DB_URL`, `PAYMENT_DB_USERNAME`, `PAYMENT_DB_PASSWORD`,
`KAFKA_BOOTSTRAP_SERVERS`. There are no connection defaults. Optional
`APEX_PAYMENT_CONSUMER_GROUP` defaults to `payment-cell-authorization-v1`.
Use a separately provisioned, Payment-owned PostgreSQL database. Flyway installs
Platform V1001/V1002 and Payment V2000; no applied migrations are rewritten.

The base listener is paused. `three-cell-saga` enables it; `simulation` supplies the
explicit seeded policy. Without a policy Bean startup fails. Without a per-order policy
row processing fails technically, rolls back, retries and eventually goes to DLT;
it never implicitly approves or converts missing setup into a business decline.

Only in an operator-owned disposable demo database, seed a known Order ID:

```sql
INSERT INTO payment_simulation_policy(order_id, decision)
VALUES ('<actual-order-uuid>', 'AUTHORIZED'); -- or DECLINED
```

Keep Payment stopped while submitting the Order and inserting this decision, then
start it. This avoids racing the bounded retry window. There is no seed/admin API
and no client-controlled approval field. The simulation profile and database writes
are an explicit demo opt-in, not production payment authorization.

```powershell
# Set required variables in this process; do not put credentials in committed files.
java -jar payment-cell/target/payment-cell.jar --spring.profiles.active=three-cell-saga,simulation
```

The unique `payment_authorizations.order_id` claims the operation. Inbox, decision
and its first outcome Outbox row commit together. Different event IDs for the same
order preserve the first decision and do not emit another outcome; the original
durable Outbox remains the delivery path. A changed simulation policy does not
override an existing decision. Aggregate identity mismatch fails closed.

Full disposable verification and all three process instructions:
[three-cell guide](../inventory-cell/tools/THREE_CELL_SAGA.md).
M29 adds opt-in targeted authorization-request replay via the same Payment group/Inbox;
see the [operational recovery guide](../inventory-cell/tools/OPERATIONAL_RECOVERY.md).
Tests can also run independently after current Platform artifacts are installed:
`mvn -f payment-cell/pom.xml clean verify`. They never use the supplied deployment
environment; they create their own native PostgreSQL and KRaft broker.
