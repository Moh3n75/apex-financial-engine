# APEX Operations Web (OPS-04 / OPS-04B preview)

Local-only, read-only Next.js UI for the restricted APEX Operations Console.

**Not production. Not a general Event Explorer. No mutations.**
Optional **M36A** local OIDC (Identity → BFF session → Console JWT) is opt-in; see `tools/m36a/SECURE_RUN.md`.

## Requirements

- Node.js ≥ 20.9
- pnpm 9.15.x (lockfile committed)
- Java 21 + Maven 3.9.11 for live OPS-04B acceptance
- Optional manual: Java Operations Console on loopback with OPS-03D search enabled

## Secure topology

- Browser → Next.js (`127.0.0.1:3000`) only
- Next.js server → Java Console (loopback origin only; default `http://127.0.0.1:9190`)
- `APEX_OPERATIONS_API_ORIGIN` is **server-only** (never `NEXT_PUBLIC_*`)
- `pnpm dev` / `pnpm start` bind **literal `127.0.0.1`**
- Do not publish, tunnel, Docker-expose, or set `0.0.0.0`

## Setup (Windows PowerShell) — interactive

```powershell
cd apps\operations-web
Copy-Item .env.example .env.local
# APEX_OPERATIONS_API_ORIGIN must remain a loopback origin (port 1024–65535)
pnpm install --frozen-lockfile
# Live Console on :9190 (fixture mode MUST stay unset/false):
Remove-Item Env:APEX_OPS_UI_FIXTURE_MODE -ErrorAction SilentlyContinue
$env:APEX_OPERATIONS_API_ORIGIN='http://127.0.0.1:9190'
pnpm dev
```

Open `http://127.0.0.1:3000`.

### M36A authenticated mode (local Identity)

```powershell
# After offline-provisioning apex-ops-web (tools/m36a/provision-ops-web-client.md)
# and enabling apex.ops.m36a on the Java Console:
$env:APEX_OPS_M36A_ENABLED='true'
# Set APEX_OIDC_* + APEX_OPS_SESSION_SECRET in .env.local (see .env.example)
Remove-Item Env:APEX_OPS_UI_FIXTURE_MODE -ErrorAction SilentlyContinue
pnpm dev
```

Fixture mode **forces M36A off**. Do not treat fixture Playwright as authenticated live.

### Fixture mode (UI-only; not live qualification)

```powershell
$env:APEX_OPS_UI_FIXTURE_MODE='true'
pnpm dev
```

Shows `SYNTHETIC DEMO`. Never count fixture Playwright as LIVE_E2E.

## OPS-04B live acceptance (real Java stack)

Repeatable command (never starts fixture mode):

```powershell
$env:JAVA_HOME='C:\Program Files\Java\jdk-21'
$env:Path="$env:JAVA_HOME\bin;$env:USERPROFILE\.local-node;$env:USERPROFILE\.local-node\bin;$env:Path"
Remove-Item Env:APEX_OPS_UI_FIXTURE_MODE -ErrorAction SilentlyContinue
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools\ops04b\verify-ops04b-live.ps1
```

What it does:

1. Fails if `:9190` or `:3000` are already listening (will not kill foreign PIDs)
2. `pnpm install --frozen-lockfile` + `pnpm build` for `apps/operations-web`
3. Packages Order / Inventory / Payment / Operations Console JARs
4. Runs `Ops04bLiveUiBridgeIT` which owns disposable PostgreSQL + KRaft + mTLS cells + Console on `127.0.0.1:9190`, creates a real order, starts Next with `OPS04B_REQUIRE_LIVE=true` / fixture OFF, then Playwright `live-happy` + `live-stale`
5. Writes receipts under `out/ops04b/` (ignored runtime output)

Owner indexes: fixture applies each cell’s `db/ops03d/recent-indexes.sql` in an owned autocommit session. Production index creation remains an owner action (`apex.ops03d.indexes-reviewed`, scopes, `QUERY:OPS_OWNER_RECENT_SEARCH:1`).

## Scripts

| Command | Purpose |
|---|---|
| `pnpm dev` / `pnpm start` | Next on 127.0.0.1:3000 |
| `pnpm build` | Production build (`--webpack`) |
| `pnpm lint` / `pnpm typecheck` / `pnpm test` | Frontend gates |
| `pnpm test:e2e` / `test:e2e:fixture` | Fixture Playwright (separate from live) |
| `pnpm test:e2e:live` | Live Playwright only (requires running stack + handshake) |
| `pnpm verify` | lint + typecheck + unit + build |

## Pages

Overview · Cells & Sources · Recent Events · Known-Group Inbox · Workflows · Requests · Active Operations · Diagnostics

## Deployment restrictions

No Vercel/cloud manifest, no public Docker publish, no wildcard CORS, no operator-supplied upstream URL. OPS-03D FULL regression remains failed/open — this UI does not relabel it. Owner approval required before any commit/push.
