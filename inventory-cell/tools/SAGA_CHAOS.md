# Deterministic Saga COMMIT chaos

Run from the repository root with Java 21 and Maven available:

```powershell
./inventory-cell/tools/verify-saga-chaos.ps1 -Seed 20261109 -Orders 100 -FailureRate 0.10
```

Use `-MavenCommand <path-to-mvn.cmd>` and optionally `-Offline` with a populated Maven cache.
The runner clean-builds the packaged applications and their Platform dependencies, writes the
existing build receipts, runs `SagaChaosIT`, then runs
`OperationalRecoverySagaIT,SagaPerformanceIT,OrderInventorySagaIT`. Packaging skips Cell unit
tests; the named integration suites run explicitly. A failed regression makes the runner fail
even when chaos succeeds. Command argument arrays, exit codes, assertion failures, XML reports,
and logs are retained under `out/saga-chaos-<timestamp>/`.

No shared database, Kafka endpoint, or Eureka server is used. The fixture starts four native
disposable PostgreSQL servers, one embedded KRaft broker, and packaged Cell JVMs. Every resource
closed by the fixture was created by it. Child application environment overrides are sanitized.
Database durability settings (`fsync`, `synchronous_commit`, `full_page_writes`) must all be on.
The applications use unique test consumer groups and explicitly subscribe to approved replay
topics. Production configuration, migrations, application classes, and public APIs are unchanged.
One instance per Cell is exercised; this suite does not establish Eureka failover coverage.

`java.util.Random(seed)` generates quantities and Bernoulli checkpoint selections in a fixed
order. Product IDs are derived from the seed and sample index. Order/event IDs remain generated
by the applications and are recorded in the evidence. Every fourth order has an explicitly
seeded business decline. With 100 orders there are 325 eligible combinations: reservation and
Order payment-outcome for all orders, authorization for the 75 unseeded orders, and release and
release-acknowledgement for the 25 seeded declines. A small requested sample/rate can miss a
checkpoint; the suite fails rather than claim complete coverage.

The current Payment implementation always authorizes and no longer reads its simulation-policy
table. Accordingly, seeded declines are created as a test-only atomic Payment decision + Outbox
transaction while Payment is stopped, after the corresponding Order payment request exists.
The packaged publisher delivers the trusted outcome. Those seeded decisions are excluded from
authorization-failure eligibility. This proves compensation handling of a durable explicit
decline; it does not prove a production decline-policy implementation.

Each disposable owner database gets test-only `chaos_*` objects. A business-row trigger captures
the order ID in transaction-local state. An `AFTER INSERT` constraint trigger on the Inbox is
`DEFERRABLE INITIALLY DEFERRED`. It raises SQLSTATE 23514 during PostgreSQL **COMMIT**, after
the business changes and causal Outbox writes have executed. PostgreSQL server logs record
`STATEMENT: COMMIT`. A separate `dblink` connection persists the transaction ID, event ID,
backend PID, group/topic/offset, and local Inbox/Outbox observations outside the failed
transaction. Per-selection PostgreSQL sequences survive rollback and limit transient injection
to one attempt. These features require PostgreSQL's `dblink` extension and `pg_xact_status`;
missing support is a test failure.

The retry waits on an advisory lock held by a separate test connection. The observer requires
`pg_xact_status(...) = 'aborted'`, verifies the committed business predecessor, absent Inbox,
absent causal Outbox, and source offset not acknowledged, then unlocks the successful retry.
Final queries against each owner prove exact decisions, reservation/release states, stock
conservation, unique effects, durable delivery of every expected event, and empty pending/dead
Outbox. Exact-event and new-event-ID redeliveries exercise business and Inbox idempotency.

The separate permanent Payment case creates one additional order, injects exactly three COMMIT
failures, checks source DLT provenance and stalled `PENDING_PAYMENT`, then disables only that
fault. The existing operator CLI performs plan/approval/execute/reconcile with its audit database.
Replay identity, repeated-execute publication idempotency, unchanged source topic, and duplicate
delivery are checked. No offsets are reset and no DLT is automatically replayed.

`inventory-cell/target/saga-chaos-evidence.json` contains the chaos result and failed assertions,
selection, observed transaction rollbacks and recovery, instance PIDs, business outcomes, stock,
and replay evidence. `actualGeneratedOrderCount` counts only randomized orders;
`totalOrdersIncludingPermanentCase` includes the additional permanent case. Final outcome and
stock totals include both. A PASS is written only after all chaos assertions succeed; inspect
the runner's `execution.json` independently for regression results.
