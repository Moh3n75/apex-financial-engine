# Accounting

A high-throughput financial transaction service built with Java, Spring Boot, and Oracle.

The goal of this project is not simply to implement money transfers, but to explore
the concurrency, database, connection-pool, and scalability challenges that appear
in real-world financial systems.

The project is developed incrementally. Each stage introduces a real-world
engineering challenge, evaluates multiple solutions, and validates the final
solution through load testing and observability.

---

## Architecture

Current architecture:

```text
                    ┌───────────────┐
                    │     k6        │
                    │ Load Testing  │
                    └───────┬───────┘
                            │
                            ▼
                    ┌───────────────┐
                    │ Transaction   │
                    │    Service    │
                    │ Spring Boot   │
                    └───────┬───────┘
                            │
                    ┌───────▼───────┐
                    │    Oracle     │
                    │   Database    │
                    └───────────────┘

                    Observability
                         │
             ┌───────────┴───────────┐
             ▼                       ▼
        Prometheus                Grafana
        
``` 

# Engineering Challenges

This project is intentionally built around real engineering problems. Each challenge is investigated independently and validated through benchmarks.

---

## Challenge 1 — Concurrent Account Updates

### Problem

The first implementation used the traditional JPA pattern:

```java
Account source = accountRepository
        .findById(transaction.sourceAccount())
        .orElseThrow();
Account destination = accountRepository
        .findById(transaction.destinationAccount())
        .orElseThrow();

source.setAvailableAmount(
        source.getAvailableAmount()
              .subtract(transaction.amount())
);
accountRepository.save(source);
```

This approach follows the classic **Read → Modify → Write** pattern. Under high concurrency, multiple transactions can read the same account balance before another transaction commits its update.

**Example:**

```text
Initial balance = 1000

Transaction A reads 1000
Transaction B reads 1000
A writes 900
B writes 800

Expected: 1000 - 100 - 200 = 700
Possible result: 800
```

This creates a **lost-update** problem.

---

### Solution 1 — Optimistic Locking

The first alternative investigated was optimistic locking. The account entity was protected using a `version` field.

**Conceptually:**

```text
Transaction A              Transaction B
    ↓                           ↓
Read version = 10           Read version = 10
    ↓                           ↓
Update                      Update
    ↓                           ↓
version = 11                OptimisticLockException
```

**Result:**  
Optimistic locking correctly detects concurrent modifications. However, for a high-throughput transaction engine it introduces another requirement: under heavy contention, retrying can significantly increase database traffic and application work.

**Decision:**  
Optimistic locking was **not selected** as the primary solution for the current implementation because it requires a retry strategy under contention.

---

### Solution 2 — Pessimistic Locking

The second approach used a database row lock:

```sql
SELECT ... FROM account WHERE id = ? FOR UPDATE
```

The transaction becomes:

```text
Transaction A              Transaction B
    ↓                           ↓
Lock Account                Wait
    ↓                           ↓
Update                      Acquire Lock
    ↓                           ↓
Commit                      Update
    ↓                           ↓
Release Lock                Commit
```

**Result:**  
This approach correctly handles concurrent updates and provides strong consistency. However, row-level locking introduces waiting and lock management overhead. Under high concurrency, multiple transactions can become serialized on the same account row.

**Decision:**  
Pessimistic locking was correct from a consistency perspective, but its locking overhead made it less attractive for the high-throughput implementation.

---

### Solution 3 — Atomic Native SQL Update

The selected approach was to let Oracle perform the arithmetic atomically.

**Instead of:**  
`Read balance → Modify balance in Java → Write balance`

**We execute:**

```sql
UPDATE account 
SET available_amount = available_amount - :amount,
    total_amount = total_amount - :amount 
WHERE id = :accountId
```

And for the destination:

```sql
UPDATE account 
SET available_amount = available_amount + :amount,
    total_amount = total_amount + :amount 
WHERE id = :accountId
```

**Result:**  
The database performs the arithmetic directly on the current row value. This eliminates the lost-update problem of the original JPA implementation without requiring application-level optimistic-lock retries.

**Current implementation:**

```java
accountRepository.decreaseBalance(
    transactionModel.sourceAccount(), 
    transactionModel.amount()
);
accountRepository.increaseBalance(
    transactionModel.destinationAccount(), 
    transactionModel.amount()
);
```

**Decision:**  
For the current benchmark, atomic native SQL updates provide the best balance between correctness and throughput among the tested approaches.

---

## Challenge 2 — Connection Pool Saturation

After solving the account concurrency problem, load testing revealed another bottleneck.

The initial HikariCP configuration used a maximum pool size of 10. At approximately 250 TPS:

```text
Active connections  = 10
Pending connections = 140
Idle connections    = 0
```

The application CPU was not saturated, but many application threads were waiting for database connections:

```text
More requests
    ↓
Connection pool exhausted
    ↓
Threads wait
    ↓
Latency increases
    ↓
P95 increases
```

### HikariCP Investigation

The pool size was progressively increased while keeping the load test constant:

| Pool Size | Dropped Iterations | P95 (ms) | P50 (ms) | Actual TPS |
|-----------|-------------------|----------|----------|------------|
| 10        | 382               | 858.02   | 97.56    | 241.49     |
| 20        | 226               | 793.91   | 26.51    | 246.19     |
| 30        | 86                | 629.57   | 26.33    | 248.47     |
| 40        | 58                | 596.01   | 16.39    | 248.99     |

The results showed a clear improvement as the connection pool increased. At pool size 40:

```text
Active connections = 15
Pending connections = 0
Idle connections = 25
```

### Important Finding — The Connection Pool Was Not the Final Bottleneck

Increasing the pool size from 30 to 40 resulted in `Pending: 117 → 0`. However, `P95: 629 ms → 596 ms`. The improvement became relatively small. This indicated that another bottleneck existed below the application layer. The next investigation therefore moves toward database contention.

---

## Challenge 3 — Hot Row Contention

The benchmark intentionally uses:

```text
sourceAccount      = 1
destinationAccount = 2
```

This creates a highly contended workload. At approximately 250 TPS, many transactions attempt to modify the same database rows concurrently. This is particularly important for real financial systems. A common example is a shared fee account:

```text
Transaction 1 ──┐
Transaction 2 ──┤
Transaction 3 ──┤
Transaction 4 ──┼──> Fee Account
Transaction N ──┘
```

Even when using an atomic native SQL update, the database still has to serialize concurrent modifications to the same physical row. Therefore:

```text
Atomic UPDATE
    ↓
Lost Update solved
    ↓
Row contention remains
```

This distinction is one of the key findings of this project.

### Benchmark Results

- **Target TPS:** ~250
- **Actual TPS:** 248.99
- **Average:** 114.68 ms
- **P50:** 16.39 ms
- **P90:** 477.95 ms
- **P95:** 596.01 ms
- **Maximum:** 1.50 s
- **HTTP errors:** 0%
- **Successful:** 100%
- **Dropped iterations:** 58
- **Total transactions:** 14,943

### Why P50 Is Low but P95 Is High

One interesting observation is the difference between median and tail latency. This means a large portion of transactions complete quickly, while a smaller portion experiences significant waiting. The current hypothesis is database-level contention on hot account rows. This will be investigated in future iterations.

---

## Observability

The project includes:

- **Prometheus**
- **Grafana**
- **Micrometer**
- **k6**

**Current metrics include:**

- `transactions_total`
- `transaction_processing_seconds_count`
- `transaction_processing_seconds_bucket`
- HikariCP metrics:
    - `connections_active`
    - `connections_pending`
    - `connections_idle`

**Example PromQL query for P95:**

```promql
histogram_quantile(
    0.99,
    sum(
        rate(transaction_processing_seconds_bucket[1m])
    ) by (le)
)
```

---

## Load Testing

Load testing is performed using **k6**.

**Example workload:**

- **Target TPS:** ~250
- **Duration:** 60 seconds

The benchmark collects:

- TPS
- Average latency
- P50, P90, P95, P99
- Maximum latency
- Error rate
- Dropped iterations
- Virtual users
- CPU
- Thread count
- Database connections

### Benchmarking Methodology

```text
1. Establish baseline
        ↓
2. Identify bottleneck
        ↓
3. Implement one change
        ↓
4. Run the same workload
        ↓
5. Compare metrics
        ↓
6. Keep or reject the change
```

---

## Current Architecture Decision

The current transaction implementation is:

```text
HTTP Request
    ↓
Spring Boot
    ↓
@Transactional
    ↓
Atomic Native SQL
    ↓
Oracle
```

With:

- **HikariCP**
- **Prometheus**
- **Grafana**
- **k6**

### HikariCP Configuration

The current benchmark configuration uses `Hikari maximumPoolSize = 40`. However, the pool size is considered a benchmark parameter rather than a universally optimal production value. The optimal value depends on database capacity and workload characteristics.

---

## Current Bottlenecks

The investigation has identified the following:

### Solved

- Lost updates caused by JPA read-modify-write
- Need for optimistic-lock retry under contention
- Excessive row-lock waiting from pessimistic locking
- HikariCP connection starvation

### Currently Under Investigation

- Database row contention
- Hot account scalability
- Tail latency under high concurrency
- Scaling shared financial accounts

---

## Future Work

The next stages of the project will introduce additional real-world challenges.

### Hot Account Sharding

Instead of one shared account, investigate distributing writes across multiple rows:

```text
Fee Account 1
Fee Account 2
Fee Account 3
...
Fee Account N
```

### Ledger-Based Accounting

Investigate replacing direct updates of hot aggregate rows with an append-only ledger:

```text
Transaction
    ↓
Ledger Entry
    ↓
Async Aggregation
    ↓
Account Balance
```

### Authentication

Introduce a dedicated authentication service. Measure the overhead of authentication at high throughput:

```text
Client
    ↓
Authentication
    ↓
Transaction
```

### Authorization

Introduce role and permission checks while avoiding a synchronous authorization bottleneck.

### Order Service

Introduce order processing:

```text
Order
    ↓
Inventory
    ↓
Transaction
```

Investigate distributed transaction problems.

### Inventory

Introduce concurrent inventory reservation and investigate hot-product contention.

### Payment and Fee Processing

Introduce real transaction fees and investigate the scalability of shared financial accounts.

### Event-Driven Architecture

Introduce Kafka and asynchronous event processing:

```text
Transaction
    ↓
Event
    ↓
Kafka
    ├── Payment
    ├── Inventory
    ├── Notification
    └── Analytics
```

### Outbox Pattern

Guarantee reliable event publication together with database transactions.

### Saga

Introduce distributed transaction management for multi-service workflows:

```text
Order
    ↓
Reserve Inventory
    ↓
Payment
    ↓
Transaction
```

With compensating actions when a step fails.

### Distributed Tracing

Introduce distributed tracing to understand end-to-end latency:

```text
Gateway
    ↓
Auth
    ↓
Order
    ↓
Inventory
    ↓
Transaction
    ↓
Oracle
```

---

## Long-Term Goal

The long-term goal of this project is to evolve the current transaction service into a distributed, horizontally scalable financial platform capable of handling extremely high throughput.

The target is not to achieve 1M TPS on a single machine. Instead, the goal is to investigate how a system can scale toward 1,000,000 TPS through:

```text
         │ Load Balancer │
         ┌───────────────┼───────────────┐
         ↓               ↓               ↓
 Service Node    Service Node    Service Node
         │               │               │
 Partition 1     Partition 2     Partition N
         │               │               │
        DB 1            DB 2            DB N
```

- Horizontal scaling
- Partitioning/Sharding
- Asynchronous processing
- Event-driven architecture
- Database optimization
- Concurrency control
- Backpressure
- Observability

---

## Engineering Philosophy

This project follows one simple principle:

> **Measure first. Optimize second.**

Every architectural decision should answer three questions:

1. What problem are we solving?
2. Why does this solution solve it?
3. What does the benchmark prove?

The goal is not to build the most complicated architecture. The goal is to understand why a particular architecture is required at a particular scale.

---

## Status

**Current Stage:**

- [x] Transaction Service
- [x] Concurrent account update investigation
- [x] Optimistic locking investigation
- [x] Pessimistic locking investigation
- [x] Atomic native SQL update
- [x] HikariCP bottleneck investigation
- [x] Prometheus
- [x] Grafana
- [x] k6 load testing
- [x] Baseline benchmark
- [ ] Hot account sharding
- [ ] Ledger architecture
- [ ] Authentication
- [ ] Authorization
- [ ] Order Service
- [ ] Inventory Service
- [ ] Payment Service
- [ ] Kafka
- [ ] Outbox Pattern
- [ ] Saga
- [ ] API Gateway
- [ ] Distributed Tracing
- [ ] Distributed Load Testing
- [ ] Horizontal Scaling
- [ ] 1M TPS experiment

---

## Tech Stack

- Java
- Spring Boot
- Spring Data JPA
- Oracle Database
- HikariCP
- Micrometer
- Prometheus
- Grafana
- k6
- Docker

---

## Disclaimer

This project is an engineering and benchmarking laboratory. Benchmark results depend on:

- Hardware
- Oracle configuration
- JVM configuration
- Docker configuration
- Network
- Database schema
- Workload distribution
- Connection pool configuration
- Number of application instances

Therefore, benchmark numbers should be interpreted as measurements of a specific environment rather than universal performance guarantees.