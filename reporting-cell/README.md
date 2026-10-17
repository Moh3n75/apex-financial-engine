# reporting-cell

Independent Reporting Cell (M22). Consumes `CUSTOMER_CREATED` v1 from Kafka via APEX Platform messaging and projects into **its own** `reporting_db`.

## APIs

- `GET /api/v1/reports/customers/{customerId}`
- `GET /api/v1/reports/customers/count`
- `GET /actuator/health`

## Config

| Variable | Default |
|----------|---------|
| `CELL_DB_URL` | `jdbc:postgresql://localhost:5435/reporting_db` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `APEX_CUSTOMER_EVENTS_TOPIC` | `apex.customer.events.v1` |
| `SERVER_PORT` | `8082` |

Does **not** connect to `customer_db`.
