# APEX Operations Web (OPS-04 preview)

Local-only, read-only Next.js UI for the restricted APEX Operations Console.

**Not production. Not a general Event Explorer. No M36 auth. No mutations.**

## Requirements

- Node.js ≥ 20.9
- pnpm 9.15.x (lockfile committed)
- Optional: Java Operations Console on `http://127.0.0.1:9190` with OPS-03D search enabled

## Secure topology

- Browser → Next.js (`127.0.0.1:3000`) only
- Next.js server → Java Console (`127.0.0.1:9190`) only
- `APEX_OPERATIONS_API_ORIGIN` is **server-only** (never `NEXT_PUBLIC_*`)
- `pnpm dev` / `pnpm start` bind **literal `127.0.0.1`**
- Do not publish, tunnel, Docker-expose, or set `0.0.0.0`

## Setup (Windows PowerShell)

```powershell
cd apps\operations-web
Copy-Item .env.example .env.local
# edit APEX_OPERATIONS_API_ORIGIN if needed (must remain loopback allowlist)
pnpm install --frozen-lockfile
pnpm dev
```

Open `http://127.0.0.1:3000`.

### Fixture mode (no Java Console)

For Playwright / offline UI smoke:

```powershell
$env:APEX_OPS_UI_FIXTURE_MODE='true'
pnpm dev
```

Synthetic rows are clearly fixture-backed; do not mix claims with live Console evidence.

## Scripts

| Command | Purpose |
|---|---|
| `pnpm dev` | Next dev on 127.0.0.1:3000 |
| `pnpm build` | Production build |
| `pnpm start` | Next start on 127.0.0.1:3000 |
| `pnpm lint` | ESLint (not part of `next build`) |
| `pnpm typecheck` | `tsc --noEmit` |
| `pnpm test` | Vitest unit tests |
| `pnpm test:e2e` | Playwright browser smoke |
| `pnpm verify` | lint + typecheck + unit + build |

## Pages

Overview · Cells & Sources · Recent Events · Known-Group Inbox · Workflows · Requests · Active Operations · Diagnostics

All surfaces preserve freshness/availability/coverage/uncertainty semantics from the Java API. Replay/Retry/Pause/Resume are not implemented.

## Deployment restrictions

No Vercel/cloud manifest, no public Docker publish, no wildcard CORS, no operator-supplied upstream URL. Owner approval required before any commit/push of this frontend phase.
