# M35 Identity contract

`apex-identity-server` is an independent Spring MVC Authorization Server, not a Cell. It owns one
PostgreSQL database, shared only by its replicas. No Kafka, discovery, gRPC or business dependency.
The configured issuer is one stable externally reachable HTTPS URL per **single tenant** deployment.
Tenant UUID is deployment policy; user UUID is immutable OIDC `sub`. Login names are tenant-local,
not token subjects. No signup, production seed, default client, admin API or user impersonation.

Use Spring Security 7.1.1 as managed by Boot 4.1.1, its standard Authorization Code, PKCE S256,
OIDC discovery/JWKS/ID token/UserInfo, token, consent, introspection and revocation endpoints.
No password/implicit/device/custom grants. Public clients get no refresh token (standard server
policy); an explicitly registered confidential PKCE web client may use rotating refresh tokens.
A separate service client uses client_credentials only. State/nonce originate and are checked by
the client; the server echoes state and signs the supplied nonce, rather than inventing a client
session verifier. Exact redirect registrations and no wildcard CORS. Default login/logout retain CSRF.

Users/passwords (standard adaptive PasswordEncoder), registered clients, authorizations, consents
and HTTP sessions are JDBC-backed. Client onboarding is an offline, reviewed owning-DB operation.
Client policy pins tenant, API audience, grants and scope. No unrestricted dynamic registration.
Token response writes must be buffered until DB commit. Cross-replica code/refresh redemption and
revocation serialize using PostgreSQL transaction advisory locks over the presented credential;
there is no JVM-local correctness lock. Issuance failure never means business rejection/success.
Grant transactions explicitly use READ_COMMITTED (reload after the lock) and a10-second transaction
timeout; waiting locks are bounded at3 seconds. User-row share locks serialize issuance against
account disable. A DB default with older repeatable snapshots must not bypass one-time redemption.

Access JWT TTL <=5 minutes, code <=2 minutes, ID token <=5 minutes, refresh <=8 hours, refresh reuse
disabled. Claims: iss/sub/aud/iat/exp/jti plus controlled scope/client_id/tenant_id/principal_type;
no passwords, PII, role inference or client-controlled tenant. User and service principals remain
distinct. Disabled users cannot log in or redeem stored grants; stored-token lookup/introspection
fails closed. Already issued JWTs can still pass offline validation until exp: no instant global
revocation claim. Resource Servers must enforce issuer, audience, time and signature independently.

Signing: externally provisioned RSA >=2048-bit RS256 JWK set, stable unique kid; exactly one active
private signing key and public verification-only predecessors/next keys. No startup key generation.
JWKS contains public material only. Rotation is explicit file rollout/restart: publish next public,
then activate its private key on replicas; retain prior public keys for maximum issued token TTL +
clock skew. Protect file permissions, backups and DB/audit access externally. No KMS/RBAC claim.

Normal startup requires issuer HTTPS, HTTPS listener or explicit trusted TLS-terminating ingress
attestation, keys, owning DB and configured clients/users. A fixture-only explicit loopback profile
allows HTTP on 127.0.0.1; it never seeds accounts/keys. Forwarded headers are not trusted by default.
External load balancer must route one fixed issuer to both replicas, and TLS/host policy must be
qualified separately. M34 Gateway remains loopback-only and is not made a Resource Server here.

M36 will define validated end-user vs mTLS service identity, tenant/resource authorization and
explicit HTTP→gRPC delegation. No tokens/credentials enter Kafka/Outbox/replay or business DTOs.
MFA, self-service recovery, production ingress/PKI rotation/failover and real financial integration
are outside this increment.
