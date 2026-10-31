# APEX Identity Server (M35)

Independent Boot 4.1.1 / Spring Security 7.1.1 application, no Cell dependencies. Read
[ARCHITECTURE.md](ARCHITECTURE.md) for the binding single-tenant, issuer, grant and key contract.
It does not authorize the existing business APIs or change M34 Gateway exposure (M36 work).

## Build and isolated acceptance

Java 21, Maven 3.9.x. From the repository root:

```powershell
$env:JAVA_HOME = '<Java 21 JDK>'
$env:Path = "$env:JAVA_HOME/bin;$env:Path"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/m35/verify-m35.ps1 -MavenCommand '<mvn.cmd>'
# Add -Offline once dependencies/plugins/native PG binaries are cached.
# -Phase Identity runs only the complete new unit + packaged two-JVM acceptance.
# -Phase Regression runs the unchanged complete M34/M32/prior chain.
```

`mvn -f apex-identity-server/pom.xml clean verify` runs unit tests, packages the executable JAR,
then Failsafe launches **two actual java -jar processes**, fixed test-only issuer proxy and one
native owned PostgreSQL. It uses real cookies, CSRF, login, consent, PKCE/code exchange, UserInfo,
cross-replica code/refresh races/restarts, independent JWT/JWKS validation, revocation and key rollout.
Fixtures alone create random passwords/secrets/keys; private files are removed during teardown.
Evidence under `target/identity-evidence`/Failsafe XML is ignored; never commit tokens/keys/logs.
The test proxy is not a deployment ingress. Real TLS/ingress qualification is a separate control.

## Operator-owned deployment

Provision **only a separate Identity PostgreSQL logical DB** and a restricted owner account.
Migrations V1/V2 live solely in this application's history: standard Security 7.1.1 JDBC schema
(PostgreSQL text/timestamptz adaptation), Session 4.1.1 PostgreSQL schema, tenant/user/client policy
constraints and indexes. No Platform/Cell migration history is used. Flyway applies them on startup;
for first onboarding apply them using the standard Flyway operator workflow before starting an app
that requires users/clients. Back up identity/auth/client/consent/session state as sensitive data.
Opaque credentials and JWT authorizations are stored in the DB; use encrypted storage/backups and
restrict DB access. Passwords use the standard delegating encoder's salted bcrypt hashes.

Required externally supplied settings (no endpoint or credential fallback):

| Setting | Policy |
|---|---|
| `IDENTITY_DB_URL`, `IDENTITY_DB_USER`, `IDENTITY_DB_PASSWORD` | Owning PostgreSQL only |
| `apex.identity.issuer` | Stable externally reachable HTTPS origin, same on every replica |
| `apex.identity.tenant-id` | One immutable UUID tenant per issuer/deployment |
| `apex.identity.key-file`, `apex.identity.active-kid` | Externally protected JWK set / active RS256 key ID |
| `server.ssl.enabled` + Boot SSL bundle, OR `apex.identity.trusted-https-ingress=true` | Actual TLS listener or explicit operator attestation of protected HTTPS ingress; attestation is not authentication |
| `server.address` | Defaults loopback; widen only with separately authorized secure ingress/network policy |
| `IDENTITY_PORT` | Defaults 9000; separate replica HTTP ports behind the same stable issuer |

No forwarded/user/tenant headers establish identity. The request origin is pinned to the configured
issuer for external login/consent redirects, not derived from untrusted forwarding headers.
Cookies are Secure/HttpOnly/SameSite=Lax; session fixation/CSRF use standard Security behavior;
default form logout requires CSRF. Sessions and the Security SessionRegistry use Spring Session JDBC.
Access logs are off and error details are excluded. Protect ingress logs from code/token/query leakage.

Run each replica with its own process configuration but the **same tenant, issuer, DB and public
key ring**. Example, after offline provisioning and reviewed external configuration:

```powershell
java -jar apex-identity-server/target/apex-identity-server.jar --spring.config.additional-location=file:<protected-replica-1.properties>
# Separate terminal, distinct IDENTITY_PORT or configuration:
java -jar apex-identity-server/target/apex-identity-server.jar --spring.config.additional-location=file:<protected-replica-2.properties>
```

Normal mode fails without explicit issuer/keys/clients/users. Only an explicit `isolated-test`
profile + `apex.identity.allow-loopback-http=true`, issuer host localhost/127.0.0.1 and bind127.0.0.1
permits HTTP for the disposable fixture; it does not seed anything. Do not use it on a real deployment.

## Reviewed offline onboarding, not an admin endpoint

Use Spring's `JdbcRegisteredClientRepository` and `PasswordEncoderFactories.createDelegatingPasswordEncoder()`
in a reviewed one-time owner-DB transaction, or an equivalent migration/change-management tool.
Supply secrets interactively/from an authorized secret manager, never literals, argv or SQL logs.
`IdentityReplicasIT` demonstrates the exact standard builder/persistence APIs with disposable values.

1. Insert an immutable `identity_tenant` UUID; provision a random immutable `identity_user.user_id`,
   tenant-local `login_name`, bcrypt-prefixed hash and explicit enabled status. A DB trigger prohibits
   changing userId/tenant. No signup, email, production account or default administrator is provided.
2. Persist a `RegisteredClient` with explicit ID/clientId/name, exact redirect URIs, approved scopes,
   authentication method and allowed grants. Then insert its `identity_client_policy` tenant and
   explicit audience. The application cannot dynamically register clients; its repository save API
   refuses runtime onboarding. Invalid persisted grant/TTL/tenant policy fails startup/use closed.
3. Public browser: authentication NONE, authorization_code only, requireProofKey=true,
   requireAuthorizationConsent=true. Confidential web/BFF: client_secret_basic, authorization_code
   plus refresh_token, PKCE+consent. Machine: client_secret_basic and client_credentials only, no
   openid scope. Client secrets are encoded, not plaintext. No password/implicit/device/custom grant.
4. Set access TTL <=5 min, code TTL <=2 min, refresh TTL <=8 h, reuseRefreshTokens=false; ID tokens
   are capped at5 min. Every browser redirect must match exactly, even a loopback port variation.
   No wildcard redirects/CORS registration. Consent is persisted by client+immutable subject.

## Browser/client protocol

Use `/.well-known/openid-configuration` rather than constructing endpoints. Standard endpoints:
`/oauth2/authorize`, `/oauth2/token`, `/oauth2/jwks`, `/userinfo`, `/oauth2/introspect`, `/oauth2/revoke`.
OIDC RP logout metadata is supplied by the standard server; the acceptance exercises CSRF form
logout and shared sessions, not every RP-initiated logout/OP front/back-channel variant.

For code flow, a client generates cryptographically random state, nonce and a43..128-character
verifier; send base64url(SHA256(verifier)) with code_challenge_method=S256. Keep state/nonce/verifier
bound to that client's browser session; check returned state **before** exchange and signed ID-token
nonce afterwards. The server echoes supplied state; it cannot validate the client's hidden expected
state. Repeated consent scopes are distinct form fields, not one concatenated checkbox value.
Public clients do **not** receive refresh tokens under the standard server policy. Confidential
clients explicitly authorized for refresh rotate them. Codes and old refresh tokens are one-use;
concurrent redemption across replicas is serialized by DB transaction locks on the authorization.
Grant transactions explicitly select READ_COMMITTED, regardless of the database default; the
fixture deliberately uses a REPEATABLE_READ database default to verify this reload boundary.
Wrong verifier, code replay, unauthorized scope/client/method/grant/redirect fail without tokens.

Token replies are buffered until actual owning DB COMMIT. A DB/commit failure returns sanitized503
temporarily_unavailable, with no successful token body. A network loss after a successful commit
is ambiguous: do not blindly retry an old refresh token or claim a failed issuance; reauthenticate
if the new rotating credential was lost. No distributed exactly-once or arbitrary grant retry claim.

JWT access claims include issuer, immutable human sub or machine client sub, explicit API audience,
time/jti, approved scope, client_id, tenant_id and principal_type=user|service. ID token audience is
the relying client. No inferred roles or password/PII claims. Resource Servers must independently
validate signature/kid, issuer, audience and timestamps (test verifier uses standard60 s skew).
Never forward tokens in Kafka/Outbox/replay or trust X-User/X-Tenant as an authentication source.

Disable a user to prevent login, code/refresh redemption and stored-token lookup/introspection.
Revocation/introspection across replicas uses the JDBC authorizations. **An already-issued signed
JWT may remain valid offline until exp + accepted skew**; revoking it is not instant revocation at
all Resource Servers. Use short TTL and authorized introspection when immediate state is required.

## Key lifecycle

Provision external RS256 RSA >=2048-bit JWKs through an authorized key-management workflow.
Exactly one active private key; <=4 unique stable kids; all other keys public verification-only.
No random startup key or repository private key. Restrict file/secret permissions to the process,
mount read-only, prohibit source/log/backup disclosure. Windows ACL/POSIX permissions and secret
delivery are operator responsibilities; this milestone does not certify a KMS or automatic rotation.

1. Publish next **public** key alongside active A on all replicas/JWKS, while signing A.
2. Replace the protected files on a controlled rollout: B private active, A public retained. During
   rollout both replicas publish the same public ring and verify both, although active kids differ.
3. After all issued A access/ID tokens' maximum TTL + clock skew have elapsed, retire A by an
   explicit reviewed rollout. Keep durable authorizations/consents; do not truncate/reset history.

Unknown kid, wrong signature/issuer/audience, expiry/not-before fail validation. Missing stable
signing config refuses restart. JWKS never exports private parameters. JWTs and key content are
not included in acceptance evidence; only kid/public thumbprints and sanitized HTTP/SQL receipts.

## Discovery and follow-up

Issuer URL is stable regardless of backend replica addresses. A future explicit Eureka or
Kubernetes deployment may route that issuer to replicas sharing this DB; this application currently
has **no discovery dependency or automatic registration**. Provision HTTPS ingress, service/probes,
secrets and policies separately; no Identity Eureka/Kubernetes deployment was qualified here.
Do not register token endpoints as business gRPC handlers or make Cells access Identity DB.
M36 must explicitly integrate Gateway/Cell JWT audience/tenant/resource authorization and service
mTLS vs user delegation; M35 leaves M34 security boundaries untouched. MFA, recovery, operational
RBAC, production TLS/key rotation/failover and real financial integration remain outside this scope.

### Deployment topology examples (not executed deployments)

For VMs, keep one issuer such as `https://identity.example.net` behind a **separately authorized
HTTPS ingress**, with two backend endpoints/ports and the shared Identity DB/key ring. No M34
Gateway public exposure is enabled. An operator's existing Eureka registration agent may publish
HTTP-only `APEX-IDENTITY-SERVER` entries with unique `identity-1`/`identity-2` IDs, backend HTTP/TLS
ports and readiness/expiry appropriate to that ingress. This JAR has no Eureka client: setting
`eureka.*` alone does not register it, and the M34 Cell adapter's mandatory gRPC metadata is not
an Identity registration model. HTTP-only registration/secure ingress integration remains unverified.

For Kubernetes, use a two-replica `Deployment` and selector-backed `Service` for
`apex-identity-server`, one separately provisioned Identity DB Secret, per-workload external key/TLS
Secret mounts (read-only), and the same HTTPS issuer in both Pods. Server-only Pods require no
Kubernetes API token. Route only the configured issuer host through explicitly authorized HTTPS
ingress; validate backend TLS/host trust and DB-aware readiness before exposure. Roll out staged
public/active key sets as described above, retaining old verification keys. Neither native cluster
admission, probes, Pod failover nor this issuer ingress was tested here. Do not substitute Service
DNS or an arbitrary backend address for the issuer and do not add Eureka in this mode.
