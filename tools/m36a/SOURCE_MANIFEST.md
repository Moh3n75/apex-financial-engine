# M36A commit-ready source-only manifest

Stage **only** reviewed source below. Do **not** stage: secrets, `.env.local`, `out/`, `.next/`, `node_modules/`, protected application YAMLs, generated acceptance reports, or unrelated dirty work.

## Include

### Decision / runbooks
- `tools/m36a/M36A_DECISION_TABLE.md`
- `tools/m36a/SECURE_RUN.md`
- `tools/m36a/provision-ops-web-client.md`
- `tools/m36a/console-m36a.example.properties`
- `tools/m36a/SOURCE_MANIFEST.md`

### Next.js (`apps/operations-web`)
- `package.json` / `pnpm-lock.yaml` (openid-client, jose pins)
- `.env.example`
- `README.md`
- `src/middleware.ts`
- `src/server/auth/**`
- `src/server/ops-client.ts`
- `src/app/api/auth/**`
- `src/app/api/ops/**` (assertOpsBffAuth)
- `src/app/login/page.tsx`
- `src/components/shell.tsx`
- `src/components/app-shell.tsx`
- `src/lib/m36a-fence.test.ts`
- `e2e/m36a-unauth.spec.ts`
- `playwright.m36a.config.ts`
- `src/lib/m36a-flags.ts`

### Java Console (`apex-operations-console`)
- `pom.xml` (oauth2-resource-server, spring-security-test)
- `src/main/java/com/apex/operations/console/ConsoleApplication.java`
- `src/main/java/com/apex/operations/console/security/OpsM36a*.java`
- `src/test/java/com/apex/operations/console/security/OpsM36a*.java`

### Identity (`apex-identity-server`)
- `src/test/java/com/apex/identity/IdentityReplicasIT.java` (apex-ops-web fixture + m36a test only)

## Exclude from Git (operator upload only)
- `APEX_M36A_SECURITY_ACCEPTANCE.md` (and other generated reports)
- Evidence under `out/`, `target/*-evidence/`
