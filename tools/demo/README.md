# APEX local interactive demo

From the repository root, in PowerShell (the execution policy applies only to this invocation):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\demo\start-apex-demo.ps1 -Mode Preview -OpenBrowser
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\demo\status-apex-demo.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\demo\create-demo-order.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\demo\stop-apex-demo.ps1
```

Open **http://127.0.0.1:3000**. Preview is explicitly **NO M36 AUTH**, local disposable synthetic inputs only. The data path is real Next BFF → packaged Java Console → workload mTLS/contract ACL → independent Order/Inventory/Payment JVMs, each with its own native PostgreSQL; events flow over an owned KRaft broker. Payment is an authorization **simulation**, never a bank/PSP. SecurePreview/Identity login is not supplied by this launcher.

Java 21, Maven 3.9, Node 20, pnpm 9 and already installed web dependencies are required. The launcher detects this machine's installed tools, snapshots current nonignored source, and rebuilds current APEX dependencies into a private Maven cache under ignored `out/demo`. No full regression runs on startup. It does not overwrite application YAML, `.env.local`, workspace targets, or existing Maven APEX jars. Ports **3000 and 9190** must be free; owner HTTP/gRPC, PostgreSQL and Kafka use additional allocated ports shown by status. A collision fails without killing the holder.

The launcher returns after its real browser smoke; the detached Java supervisor stays alive. Closing the shell/Codex response does not intentionally stop it. Stop explicitly with the command above. Logs, source hashes, PID/start-time/command fingerprints, state proofs and browser smoke evidence are in the run directory printed by status; `out/demo/demo-readiness.json` records the smoke verdict. These files contain local synthetic identifiers, not credentials. Do not treat raw framework logs as a public diagnostic endpoint. No scripts print TLS passwords, keys or bearer tokens.

Kafka uses the **same 4.2.1 test dependency already present in Inventory**, bootstrapped with its actual StorageTool/Kafka CLI as an owned JVM. The embedded TestKit used by the acceptance harness prebinds wildcard sockets even when listeners specify localhost; the interactive supervisor therefore uses literal `127.0.0.1` broker/controller listeners instead. No Kafka version, business ACK policy or SDK dependency was changed. The stop command removes only the verified run's `kafka-data` after its recorded children exit; PG and certificates use their existing owned fixture cleanup.

## Explore

- **Recent Events** (`/events`): environment `test`, owner `order-cell` (or `inventory-cell` / `payment-cell`), scope `owner`. Click **Last 15m** (or adjust UTC From/To to include the order), then **Search**. New UUIDs are discovered without reconfiguring Console. Event IDs printed by the create command are cross-checked against disposable owner Outbox rows.
- **Workflows** (`/workflows`): click Search separately in **Order workflows**, **Inventory reservations**, and **Payment authorization simulation**. Happy path: `CONFIRMED`, `RESERVED`, `AUTHORIZED`. The same order UUID is the actual workflow key in these existing owner schemas; it does not imply an event/request identity relation.
- **Known-Group Inbox** (`/inbox`): owner `inventory-cell`, scope `inventory-three-demo` (also printed by status/create), Search. This proves only that group's committed handler row; other consumers remain unknown.
- **Cells & Sources** (`/cells`): owner READABLE is a DB read probe, not full readiness or Kafka progress. Active Operations is bounded, ephemeral and may legitimately be empty after requests finish.

Search is user initiated: at most one-hour lookback, 16 candidates/page, 8 pages. Wait a few seconds after `SHED/BACKOFF`, then Search again. Use a new current UTC window when exploring later. Results explicitly remain partial; timestamps are not commit coordinates. There is **no durable Operations projection/history**, authoritative broker ACK view, full timeline, ETA, live replica census, production authorization/tenant isolation or control plane. No Replay/Retry/Pause action exists. Restarting creates a new disposable stack; old data is not promised to survive.

Only the happy-path seed and create interaction are offered. Decline/compensation and stock rejection are **NOT AVAILABLE IN THIS DEMO launcher**; existing acceptance tests remain separate. No core business logic or test assertions were changed. Interactive smoke is not FULL_REGRESSION or PRODUCTION_READY; M36B's reported combined full regression remains FAIL.

## Diagnose / recheck

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\demo\status-apex-demo.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\demo\status-apex-demo.ps1 -Logs
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\demo\smoke-apex-demo.ps1
```

Status checks PID **and start time**, prints owned ports/log paths, and does not equate a foreign listener with success. The supervisor monitors its own children. A real startup/child failure triggers owned-resource cleanup; a browser smoke failure leaves the stack running for diagnosis, marks readiness NO, and never switches to synthetic responses. Stop sends a local owner-controlled flag, then waits for the supervisor to stop only its recorded Process handles and dispose its PG/KRaft/certificate resources. Cleanup receipts remain on disk; no user directories are deleted.

For an independent **design preview only**, with no backend claim:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\demo\start-ui-preview.ps1
```

This foreground server displays **SYNTHETIC DEMO** and uses existing fixture JSON/security-off flags. Ctrl+C stops it. It needs port 3000 free and cannot run alongside the real demo on that port.

Source changes for review: only `tools/demo/*` and `inventory-cell/src/test/java/com/apex/inventory/ApexDemoSupervisor.java`. No files are automatically staged or committed.
