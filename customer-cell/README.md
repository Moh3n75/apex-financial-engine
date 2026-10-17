# customer-cell

Independent Customer Cell. M21 REST + M22 Outbox publication of `CUSTOMER_CREATED` v1.

## Event

- Type: `CUSTOMER_CREATED` / version `1`
- Topic (configurable): `apex.customer.events.v1`
- Atomic with Customer insert via Platform `OutboxAppender`

## Run

```powershell
mvn clean verify
mvn spring-boot:run
```

Requires PostgreSQL (`customer_db`) and Kafka for live publishing.
