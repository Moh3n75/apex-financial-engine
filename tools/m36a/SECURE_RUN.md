# M36A secure local run (not production)

Browser → Next.js BFF (`127.0.0.1:3000`) → Java Console (`127.0.0.1:9190`) → owner gRPC mTLS.

## Prerequisites

1. Offline-provision Identity user + `apex-ops-web` client (`provision-ops-web-client.md`).
2. Identity listening on loopback with reviewed key file.
3. Operations Console with `apex.ops.m36a.enabled=true` and matching issuer/audience/tenant.
4. Owner cells unchanged (mTLS + query ACL).

## Next.js

```powershell
cd apps\operations-web
Copy-Item .env.example .env.local
# Fill APEX_OIDC_* and APEX_OPS_SESSION_SECRET (≥32 chars)
$env:APEX_OPS_M36A_ENABLED='true'
Remove-Item Env:APEX_OPS_UI_FIXTURE_MODE -ErrorAction SilentlyContinue
pnpm install --frozen-lockfile
pnpm dev
```

Open `http://127.0.0.1:3000` → sign-in → Identity → callback → Overview.

## Hard rules

- No `NEXT_PUBLIC_` Console origin/secrets.
- Fixture mode disables M36A and must never be counted as authenticated live.
- Direct browser calls to `:9190`, Kafka, or owner Postgres are out of scope and must fail closed if attempted.
- Turning M36A off restores legacy anonymous loopback for pre-M36 OPS-04B ITs only.
