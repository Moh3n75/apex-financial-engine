# M36B Decision Record (source-backed, before security edits)

**Baseline HEAD:** `3997c0a79eeb92b1efbe706856dc332740acbfba`  
**Branch:** `credit-engine-event-sourcing`  
**Date:** 2026-11-04  

## 1. Human principal and tenant

| Source | Claim / binding |
|---|---|
| Identity access JWT | `sub` (user UUID), `tenant_id`, `principal_type=user`, `scope` incl. `ops.console.read`, `aud=apex-operations-console`, `client_id=apex-ops-web` |
| Console M36A | Validates iss/aud/exp/nbf/principal_type/tenant; authority today = `SCOPE_ops.console.read` only |
| Next session | Sealed cookie holds access/refresh/id tokens + `sub`/`tenantId`/`scopes`; BFF forwards Bearer only |
| Owner gRPC | Console service peer (`m32-client` SPIFFE) + exact contract ACL — **not** the human JWT |

**Decision:** Effective human principal = JWT `sub`. Deployment tenant = configured `apex.ops.m36a.tenant-id` matching claim. `ops.console.read` is necessary but **not** sufficient for per-owner reads under M36B.

## 2. Resource grants vs tenant row isolation

Owner recent-search SQL for outbox/workflows/inbox is **not** generally tenant-predicate-filtered across kinds (order receipts have `tenant_id` in one migration; reservations/outbox do not provide a trustworthy cross-kind tenant column for Console reads).

**Decision:**
- Supported model = **deployment-scoped operator grants**: explicit tuples `(subject|*, environment, owner, kind, scope)`.
- Cross-tenant / user-granular **row** isolation on owner SQL = **TENANT_ROW_ISOLATION=BLOCKED** (DENY/UNSUPPORTED if requested; never filter-after-fetch fiction).
- UI/API labels remain deployment operator view, not “tenant isolated.”

## 3. Cross-instance session continuity

Next seals sessions with `APEX_OPS_SESSION_SECRET` (AES-GCM via jose). Cookies are host-scoped (`127.0.0.1`), port-agnostic.

**Decision:** Least-complex safe design = **shared external session sealing keyring** (same `APEX_OPS_SESSION_SECRET` across two Next processes). No shared server-side session DB in M36B.
- Wrong/missing secret ⇒ decrypt fails ⇒ unauthenticated (fail closed).
- TTL = cookie maxAge 8h and access-token `exp` (refresh when within 15s of expiry).
- **Global logout across instances** = clear cookie only; Identity end_session best-effort. Immediate revoke of already-issued JWT = **NOT_SUPPORTED**.

## 4. Credential lifetimes & revocation

| Credential | Lifetime (fixture) | Revocation |
|---|---|---|
| Access JWT | ~2–5 minutes (Identity token settings) | Expiry only |
| Refresh | ~1 hour, rotate | Logout drops refresh from cookie; no introspection product |
| Session cookie | 8h maxAge | Clear on logout; seal key rotation invalidates all |

**Contract:** Maximum stale-permission exposure ≈ access JWT remaining TTL after grant removal. Document as such; do not claim immediate JWT kill.

## 5. JWKS rotation policy

- Console uses `JwtDecoders.fromIssuerLocation` (remote JWKS).
- Overlap: Identity publishes **both** keys in JWKS before switching `active-kid`.
- New kid accepted after bounded Console JWKS refresh (awaitility poll ≤60s).
- Retired key: remove private use; after overlap window, signatures from retired-only key rejected.
- JWKS outage / unknown kid = fail closed (no disable-validation).
- No fabricated zero-delay guarantee.

## 6. Ingress / TLS edge

**LOCAL_RESTRICTED_ONLY.** Default binds remain `127.0.0.1:3000` / `:9190`. Disposable TLS reverse-proxy for `__Host-`/Secure cookies = **NOT_VERIFIED** this milestone (loopback HTTP retained). Not public production ingress.

## 7. Confused-deputy bound

Human JWT authorizes the **exact** requested environment/owner/kind/scope grant first. Only then may Console use its fixed mTLS destinations / service peer to call that owner. No client-supplied destination, reflection, or arbitrary owner SQL.

## 8. Audit

Bounded allow/deny audit at Java Console: time, hashed `sub`, tenant id, environment/owner/kind/scope, decision, safe reason code, correlation id. In-memory + optional file sink for tests. **DURABLE_AUDIT=BLOCKED** (no Business Cell TX writes). Failed sink must not break owner business paths; policy = best-effort log + continue after authorization decision recorded in-process.
