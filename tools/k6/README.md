# APEX k6 — WebFlux Gateway / Eureka / Three-cell Saga

Run these commands in PowerShell from the extracted folder.

## Smoke test (1 order/s for 15 seconds)

```powershell
k6 run `
  -e MODE=smoke `
  -e BASE_URL=http://127.0.0.1:8080 `
  -e ORDER_PATH=/order-cell/api/v1/order-placements `
  -e PRODUCT_ID=11111111-1111-4111-8111-111111111111 `
  -e RATE=1 `
  -e DURATION=15s `
  -e VERIFY_PERCENT=100 `
  .\APEX_k6_Gateway_WebFlux.js
```

## Load test (ramp up to 50 new orders/s)

```powershell
k6 run `
  -e MODE=load `
  -e BASE_URL=http://127.0.0.1:8080 `
  -e ORDER_PATH=/order-cell/api/v1/order-placements `
  -e PRODUCT_ID=11111111-1111-4111-8111-111111111111 `
  -e RATE=50 `
  -e PRE_VUS=250 `
  -e VERIFY_PERCENT=5 `
  --summary-export=summary.json `
  .\APEX_k6_Gateway_WebFlux.js
```

## Important

- Use only against local/disposable test data. This script creates actual orders.
- Update PRODUCT_ID to an existing SKU and seed enough stock for all orders.
- Ensure Eureka advertises reachable IPs/ports and all Order replicas use the same Order database.
- Ensure Inventory and Payment consumers are running with appropriate Kafka partitions and groups.
- `ORDER_PATH` defaults to `/order-cell/api/v1/order-placements` in the script.
- With `VERIFY_PERCENT=5`, end-to-end Saga metrics represent a sampled subset, not all orders.
- Do not automatically retry POSTs that time out: they may have committed.
- A failed health check at `/actuator/health` aborts the run; set `HEALTH_PATH` if different.
