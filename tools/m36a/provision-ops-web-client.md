# Provision `apex-ops-web` (offline, reviewed)

Identity does **not** auto-register clients. Use the same offline JDBC onboarding as `apex-identity-server/README.md`.

## Required registration

| Field | Value |
|---|---|
| `client_id` / id | `apex-ops-web` |
| Auth | `CLIENT_SECRET_BASIC` + bcrypt secret |
| Grants | `authorization_code`, `refresh_token` |
| PKCE | required |
| Consent | required |
| Redirect | `http://127.0.0.1:3000/api/auth/callback` only |
| Scopes | `openid`, `profile`, `ops.console.read` |
| Policy audience | `apex-operations-console` |
| Policy tenant | deployment `apex.identity.tenant-id` |

`IdentityReplicasIT` seeds this client for automated checks (`m36aOpsWebClientIssuesConsoleAudienceAndOpsScope`).

## Console pairing

```
apex.ops.m36a.enabled=true
apex.ops.m36a.issuer=<same issuer>
apex.ops.m36a.audience=apex-operations-console
apex.ops.m36a.required-scope=ops.console.read
apex.ops.m36a.tenant-id=<same tenant UUID>
```
