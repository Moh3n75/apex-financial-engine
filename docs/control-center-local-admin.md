# Local Apex Operations Admin Panel

The existing Operations Web app is reused. The Console can add service-presence evidence from its existing EurekaClient cache without owner pollers, gRPC, contract discovery or identity infrastructure. This is a local inventory capability, not production qualification.

## Local setup

Keep the existing Eureka server and Cells running. Use Java 21, Maven 3.9.11, Node >=20.9 and pnpm 9.15.9 on PATH. Rebuild the Console and its dependencies:

```powershell
mvn -pl apex-operations-console -am install -Dmaven.test.skip=true
```

That command prepares artifacts; it deliberately skips tests and is not a CI qualification command. Run the targeted tests below separately. Stop your previous Console before restarting it. Use the existing IDE ConsoleApplication run point or the built Console JAR, with these arguments:

```text
--spring.profiles.active=local
--server.address=127.0.0.1
--server.port=9190
--apex.ops.m36a.enabled=false
--eureka.client.enabled=true
--eureka.client.fetch-registry=true
--eureka.client.service-url.defaultZone=http://127.0.0.1:8761/eureka/
```

The existing auth-off switch is deliberate for this loopback development session. The new local profile does not change security configuration. For authenticated integration, retain the existing M36A/M36B configuration and omit the auth-off argument. Do not activate the local inventory profile for non-local registries. The provider also requires a local environment name and literal loopback Console binding; remote or credential-bearing registry URLs are rejected.

For the web app, copy `.env.example` to `.env.local` if no local file exists, then set:

```dotenv
APEX_OPERATIONS_API_ORIGIN=http://127.0.0.1:9190
APEX_OPS_M36A_ENABLED=false
APEX_OPS_UI_FIXTURE_MODE=false
OPS04B_REQUIRE_LIVE=true
```

Restart Next.js after changing environment settings. Alternatively, from a PowerShell session that permits local scripts, run `tools/control-center/start-local-web.ps1 -NoAuth`. It sets process-only settings and invokes the existing `pnpm dev` command on 127.0.0.1:3000; it does not overwrite `.env.local`. Omit `-NoAuth` to preserve the session's existing auth setting. No origin uses `NEXT_PUBLIC_*`.

Missing or invalid origin configuration returns HTTP 500 `CONFIGURATION_ERROR`, including safe setup guidance. A configured Console that cannot be reached returns 503 `SOURCE_UNAVAILABLE`. The existing exchange timeout, response-byte cap, concurrency budget, no-store policy and redirect rejection still apply. HTTPS origins retain HTTPS when normalized.

## Semantics and boundaries

- Only schemaVersion=2 gains the opt-in `eureka-local-inventory` provider. Version 1 is unchanged.
- UP registrations are normalized to SERVICE nodes, for example `local:service:order-cell`. Multiple UP replicas aggregate into one service. Only service names leave the provider; endpoints, host names, instance metadata and message payloads do not.
- Evidence is `OBSERVED / EUREKA / SERVICE`, explicitly an observation of registry presence. Health remains `UNKNOWN`; no traffic edges, UUID epochs, instance nodes or authoritative membership records are manufactured.
- `lastSeen` is the confirmed registry-cache refresh time, not a health-check time or individual heartbeat. Until Eureka confirms its first scheduled refresh, coverage is `SOURCE_UNAVAILABLE`. The local profile polls every five seconds; server-side response caching, replication and lease eviction add delay.
- A stale but previously confirmed cache yields `STALE` nodes/provider coverage. No confirmed registry refresh yields an empty unavailable inventory. A successfully refreshed empty registry yields an empty available inventory. Other configured sources remain intact.
- The environment is an explicit local registry scope (`local` or `local-dev`). Tagged cross-environment registrations are excluded. Untagged entries belong to that local scope; they are not independently environment-attested. Owner visibility is applied before aggregation.
- Contract and runtime membership projections run first. Matching environment/service IDs merge evidence without changing contract freshness, verified membership, existing health evidence or existing edges. Graph/evidence bounds remain enforced; truncation reports `PARTIAL`.
- Browsers contact only Next.js. The BFF contacts only the fixed loopback Console origin. The Console reads its existing EurekaClient cache; no parallel registry or browser-to-Eureka route was added. Existing OAuth, mTLS, owner authorization and mutation-denial implementations were not edited.

## Repeatable verification

```powershell
mvn -o -pl apex-operations-console -am test '-Dtest=LocalEurekaTopology*Test,TopologyApiTest,ContractTopologyTest,RuntimeMembershipPollerTest,OpsM36aHttpSecurityIT' -Dsurefire.failIfNoSpecifiedTests=false
cd apps/operations-web
pnpm test
pnpm lint
pnpm typecheck
pnpm build
$env:APEX_LOCAL_INVENTORY_LIVE='true'
pnpm exec playwright test -c playwright.live.config.ts e2e/live-local-inventory.spec.ts
```

Stop the dev server before building into the same Next output directory. `playwright.live.config.ts` reuses the running app and never starts fixtures. The new browser test is explicitly opt-in; an unenabled/skipped invocation is not live acceptance.

With Console, Eureka and Next running, `tools/control-center/verify-local-inventory.ps1` reads the real registry server-side and compares its UP names with Java, BFF and rendered-page results. It writes a redacted API receipt to `out/local-admin-live.json`. Use `-Samples 20 -IntervalSeconds 5` while gracefully stopping/restarting a local Cell using its normal launcher. Refresh `/topology` after registry/cache convergence. A crash can remain registered until lease eviction; Eureka self-preservation may delay removal. The UI must not call such a registration healthy. No runtime actions are exposed by the panel.

To exercise actual registration/removal without stopping existing Cells, run the live integration test:

```powershell
mvn -o -pl apex-operations-console -am test -Dtest=LocalEurekaInventoryLiveIT -Dsurefire.failIfNoSpecifiedTests=false -Dapex.local.inventory.live=true '-Dapex.local.inventory.receipt=E:/drive e/projectOnGit/apex-financial-engine/out/local-admin-lifecycle.json'
```

This starts one actual Console context with the distinct service name `apex-operations-console-local-probe`, registers it through the normal Eureka client, waits for the existing Console's topology to include it, closes the context gracefully, and waits for its removal. It requires the real Console/Eureka infrastructure; it cannot substitute a mock. The HTTP and unit tests use mocked registry caches and are labeled accordingly.

## Live acceptance, 2026-11-10

Java Console 9190, BFF 3000 topology, BFF overview and `/topology` returned HTTP 200. The real browser test passed. Independently reading Eureka matched the five rendered registrations:

| Service | Registry presence | Topology health |
| --- | --- | --- |
| apex-api-gateway | UP | UNKNOWN |
| apex-operations-console | UP | UNKNOWN |
| inventory-cell | UP | UNKNOWN |
| order-cell | UP | UNKNOWN |
| payment-cell | UP | UNKNOWN |

The resulting topology has 5 SERVICE nodes, 0 connections and 0 memberships. The actual JSON and live lifecycle receipts are in [the evidence directory](evidence/control-center-local-admin/). Customer and Reporting were not registered in this environment and were not invented. No existing Cell was stopped for this run; real registration/removal convergence was tested with the isolated Console process.

Validation: 32 frontend unit tests passed; frontend lint/typecheck/build passed, with existing lint/deprecation warnings. The targeted backend topology, authorization, HTTP and membership tests passed (24 tests); the two selected registry-freshness tests passed as well. The live integration test and one real Chrome test passed. The two existing registry-freshness tests also passed. The full repository CI suite was not qualified.

A broader adapter test run also exposed `EurekaMetadataTest.realAutoAllocatedServerPublishesActualPortThroughLifecycleToEurekaInstanceInfo`: it expected a non-null Eureka instance ID at line 43 and failed. This path was not changed by the inventory provider; the adapter change only makes the existing freshness utility public. The failure is retained and is not represented as a passing full suite. No Order test sources or unrelated configuration edits were reverted.

Not implemented by this slice: live traffic/OTel ingestion, Kafka Admin metadata, independently confirmed health, untagged registration environment attestation, authoritative instance epochs/duplicate detection from plain registrations, non-local registry inventory, runtime controls, or production readiness. Existing secure contract/membership providers remain available when separately configured.

## Files changed for this task

- `apex-discovery-eureka/.../EurekaCellAutoConfiguration.java`: share the existing confirmed-cache freshness reader.
- `apex-operations-contracts/.../TopologySnapshot.java` and `apps/operations-web/src/lib/topology.ts`: reject registry-presence evidence as observed traffic, without changing the JSON shape.
- `apex-operations-console/.../TopologyApi.java`, new `LocalEurekaTopologyInventory.java`, new `application-local.properties`.
- New Java tests: `LocalEurekaTopologyInventoryTest`, `LocalEurekaTopologyHttpTest`, `LocalEurekaInventoryLiveIT`.
- `apps/operations-web/src/lib/origin-policy.ts` and its test; `src/server/ops-client.ts` and new `ops-client-origin.test.ts`.
- `apps/operations-web/src/components/topology-view.tsx`, `src/app/diagnostics/page.tsx`, new `src/lib/eureka-topology.test.ts`, new `e2e/live-local-inventory.spec.ts`.
- New `tools/control-center/start-local-web.ps1`, `verify-local-inventory.ps1`, this report and redacted evidence files.
- Local ignored `.env.local` was created for the running development app. Existing unrelated working-tree changes, including Console `application.properties` and Cell YAML changes, were preserved. No commit or push was made.
