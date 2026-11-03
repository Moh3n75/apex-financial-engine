# M36A-1 Security decision table

Local-only authenticated read path. Not full M36 / multi-tenant / public ingress.

| Topic | Decision |
|---|---|
| Human vs service | Browser operators use **Authorization Code + PKCE** (`principal_type=user`). Owner Cells remain **mTLS service ACL** only. Machine CC tokens are **not** accepted for Console HTTP in M36A. |
| Issuer | Existing `apex-identity-server` (`apex.identity.issuer`). Console/Next validate that exact issuer. |
| Audience | Access token `aud` = **`apex-operations-console`** (per-client `identity_client_policy.audience`). |
| Scopes | Require **`ops.console.read`** plus OIDC `openid`/`profile` for login. Existing `api.read` is **not** sufficient for Console. |
| Roles | **None in JWT** (Identity policy). Operator authority is scope + Console grant inventory + tenant match. |
| Environment / owner grants | Console keeps explicit `apex.ops03*` / `ops03d.sources` inventory. JWT does **not** expand owner scopes. Optional `apex.ops.m36a.environment` must match if set. |
| Tenant | JWT `tenant_id` must equal `apex.ops.m36a.tenant-id`. No universal row-level tenant filter (metadata lacks it). Unsupported cross-tenant → **403**. |
| Cookie / session | Next: encrypted HttpOnly session cookie, `SameSite=Lax`, `Secure` when not loopback-HTTP test; short access TTL (≤5m Identity); refresh via Identity refresh_token for confidential client. |
| CSRF / PKCE / state / nonce | Required on authorize; callback validates state+PKCE; nonce on id_token when present. |
| Local callback | `http://127.0.0.1:3000/api/auth/callback` only (loopback allowlist). |
| Log redaction | Never log access/refresh/id tokens, codes, verifiers, passwords. BFF errors stay redacted. |
| Key rotation | Console `NimbusJwtDecoder` / JWKS from `{issuer}/oauth2/jwks`; Identity manual kid ring. |
| Console auth switch | `apex.ops.m36a.enabled=true` → JWT required on `/ops/v1/**`. `false` → legacy anonymous loopback for pre-M36 ITs only. |
| Next auth switch | `APEX_OPS_M36A_ENABLED=true` → session required (except fixture mode). Fixture remains synthetic and cannot satisfy authenticated live. |
| Owner gRPC | Unchanged mTLS + `QUERY:OPS_*` contracts. No JWT in gRPC/Kafka/outbox. |

Library choices: Next uses **`openid-client`** (OIDC) + **`jose`** (sealed cookie); Console uses **`spring-boot-starter-oauth2-resource-server`** (Boot 4.1.1 managed).
