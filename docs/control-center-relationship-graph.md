# Interactive Service Relationship Graph

Implemented incrementally on `credit-engine-event-sourcing`. The existing Console v2 endpoint, BFF, page, auth implementation and unrelated working-tree changes are retained. No commit or push was made.

## Audit and reuse decision

| Existing capability | Evidence and decision |
| --- | --- |
| Operations Web | Next 16 / React 19 / Tailwind, shared Panel, Badge and MetaRow, server-only ops client, Zod validation and existing auth/BFF routes. Reuse all of them; add a client-side graph component to the existing TopologyView. |
| Topology presentation | Expandable text details and provider metadata already represented all v2 facts. Keep them as an accessible evidence inventory; make the interactive graph the primary presentation. |
| Eureka inventory | The Console already reads its existing EurekaClient cache, filters visibility and environment before aggregation, and distinguishes cache freshness from health. Extend it with optional, explicitly unverified registry instance records. |
| Gateway | The running `apex-api-gateway` already exposes the read-only `GET /actuator/gateway/routes` endpoint on loopback 8080. It reports actual registered `route_id` / logical `lb://` destinations. Read those records server-side; do not infer edges from discovery membership. No Gateway source or security changes are necessary. |
| ContractTopology | Existing exact environment/cluster/topic/type/version joins generate explicit Kafka topic/group nodes and connections. Registered gRPC handlers are descriptors, not proof of callers. Reuse this projection unchanged. |
| Runtime membership | Existing epoch-bearing INSTANCE nodes and memberships retain their secure verification and authorization. Local registry records do not enter that membership array. |
| Schema and BFF | TopologySnapshot v2 already has the needed node kinds, protocols, provenance and provider states. Keep its JSON shape; narrowly permit epochless INSTANCE nodes only with explicit registry-only provenance. Keep schemaVersion 1 behavior. |

The graph uses React Flow 12.12.1 and Dagre 3.1.3. [React Flow's Dagre example](https://reactflow.dev/examples/layout/dagre) documents the layout integration; [its server-rendering guide](https://reactflow.dev/learn/advanced-use/ssr-ssg-configuration) describes supplying node dimensions and handle positions. The implementation supplies those dimensions, renders custom node kinds and preserves the API's IDs. The layout adapter does not create domain facts.

## Delivered behavior

- Pan, scroll zoom, zoom controls, fit-to-view, automatic layout, service expansion/collapse, explicit refresh, node/edge selection and a responsive details sidebar.
- SERVICE, INSTANCE, KAFKA_TOPIC, CONSUMER_GROUP and existing CELL nodes have distinct shapes, accents, labels and icons. Registry instances are labeled `registry only`; they have unknown health and null epochs.
- HTTP, gRPC and Kafka filters remove the relevant connections. Kafka topic/group nodes disappear when Kafka is disabled. Disconnected services remain visible because missing relationship evidence does not mean a service is absent.
- Dashed edges indicate declarations. Connection details show source, target, protocol, route/contract, environment, evidence, collection time, provider coverage and health/traffic uncertainty. Registered handlers remain visible as capabilities in node details.
- Provider status badges remain visible, with full capability/coverage details expandable. The original text inventory remains available for accessible inspection.
- A readiness overlay prevents interaction until initialization and layout fitting complete. Selecting/collapsing instances or changing filters triggers a fresh fit before the canvas is interactive again.
- No node deletion, connection editing, route mutation or runtime actions. Existing local no-auth switches continue to work; no OAuth or mTLS prerequisite was introduced for this local graph.

## HTTP route evidence

`GatewayRouteTopology` is an opt-in local-profile provider merged after the existing contract/runtime and Eureka projections. It reads the existing Gateway endpoint with a fixed server-only loopback origin, no redirects and GET only. It does not forward browser cookies or credentials. [Spring's Actuator documentation](https://docs.spring.io/spring-cloud-gateway/reference/spring-cloud-gateway-server-webflux/actuator-api.html) describes registered route introspection and the existing read-only access setting.

The reader enforces a 1.5-second deadline across headers and body, cancels oversized bodies incrementally at 64 KiB, accepts at most 128 route records, and caches results for five seconds. Gateway endpoints are not exposed to the browser. Raw destination URLs, predicates, filters, headers and metadata are not included in topology responses.

An HTTP edge requires a registered route with a logical destination and visible SERVICE endpoints in the same environment. Its source is the explicitly configured Gateway service. Evidence is `DECLARED / SPRING_CLOUD_GATEWAY`, its contract is `ROUTE:<actual route_id>`, health is UNKNOWN and traffic lastSeen is null. The Gateway's real self-route is retained because it is an actual declaration; it is not proof that a request successfully traversed it.

Unknown or non-logical destinations, malformed routes, conflicting route IDs and graph limits yield partial/unresolved coverage. Denied targets and their route IDs are removed before aggregation. Denied Gateway visibility avoids polling altogether. Transport failure, denied access, malformed responses, missing source service and disabled configuration have explicit provider states. Other providers' facts remain intact.

## Registry instance compatibility

The local profile enables `apex.ops.local-inventory.include-instances=true`. Each authorized UP registry entry can add an INSTANCE node, keyed by a stable environment/service-scoped hash alias. Raw Eureka IDs may contain hosts and ports and are therefore not exposed. The alias is an evidence-record identifier, not a claimed application instance identity.

These records use scope INSTANCE, OBSERVED/EUREKA evidence with reference `UP_REGISTRATION_NO_EPOCH`, uncertainty `REGISTRY_INSTANCE_IDENTITY_UNVERIFIED_NO_EPOCH`, UNKNOWN health and a null epoch. Java and Zod reject generic epochless instances without this provenance; Zod still requires non-null epochs for verified memberships. They are only valid in v2 under the matching service/environment. Known membership registry IDs are not added again as unverified aliases.

Expansion lays out existing instance nodes beside their service. Parentage is used only for positioning; it never creates an HTTP, gRPC, Kafka or other connection. Hash aliases cannot establish restart epochs or duplicate-instance claims. Registry cache timestamps do not establish process health.

## Local setup and repeatable checks

Use the existing setup in [control-center-local-admin.md](control-center-local-admin.md). Rebuild the changed modules, restart the local Console, and retain the normal local profile. It now adds:

```properties
apex.ops.local-inventory.include-instances=true
apex.ops.gateway-routes.enabled=true
apex.ops.gateway-routes.origin=http://127.0.0.1:8080
apex.ops.gateway-routes.service=apex-api-gateway
```

These are server-side local-profile properties. The reader rejects non-loopback origins and non-local environment names. The existing no-auth switches remain `APEX_OPS_M36A_ENABLED=false` and `apex.ops.m36a.enabled=false`; the auth implementation was not edited. Do not enable fixture mode.

The local `pnpm dev` command now explicitly uses `next dev --webpack`, matching the existing production build engine. With the same graph and services, this installed Next/Turbopack development combination repeatedly reloaded the page during initialization, making interactions unreliable; Webpack development mode passed the live interaction tests. The underlying Turbopack reload behavior was not patched. Initial Webpack development compilation can take longer.

```powershell
# Java 21 / Maven 3.9.11, repository root
mvn -o -pl apex-operations-console -am test '-Dtest=com.apex.operations.console.**.*Test,com.apex.operations.console.**.*IT' -Dsurefire.failIfNoSpecifiedTests=false
mvn -o -f apex-api-gateway/pom.xml test -Dtest=GatewaySafetyTest

cd apps/operations-web
pnpm test
pnpm typecheck
pnpm lint
# Stop the dev server before building into the same output directory.
pnpm build
pnpm dev
# In another shell, with the real local services running:
$env:APEX_LOCAL_INVENTORY_LIVE='true'
pnpm exec playwright test -c playwright.live.config.ts e2e/live-relationship-graph.spec.ts e2e/live-local-inventory.spec.ts
```

The existing `verify-local-inventory.ps1` script now compares SERVICE nodes, allowing additional INSTANCE records. It still reads Eureka only from PowerShell and verifies the real Java/BFF/page outputs. The live Java registration/removal test remains opt-in and is documented in the local setup guide. It now permits separately evidenced Gateway routes to the real probe service while continuing to reject traffic claims.

## Actual validation

The running Java Console on 9190 and BFF on 3000 returned valid v2 responses. The real Chrome tests exercised node/edge selection, instance expansion, evidence details, protocol filtering, zoom and fit-to-view, with no browser-to-Gateway/Eureka requests or browser errors. The final graph contains:

| Fact | Actual result |
| --- | --- |
| Discovered services | 5: apex-api-gateway, apex-operations-console, order-cell, inventory-cell, payment-cell |
| Registry instance evidence records | 5, all null epoch / UNKNOWN health |
| HTTP edges | 5, matched against the actual Gateway route IDs and destinations |
| Kafka / gRPC edges | 0 / 0 in this local runtime |
| Verified memberships | 0; registry aliases are not memberships |

The live Java lifecycle test also passed with the real isolated Console probe: 5 → 6 → 5 service registrations, or 10 → 12 → 10 total evidence nodes. Existing Cells were left running.

Tests: 65 Console tests passed; the opt-in live test was skipped in that suite and passed in a separate enabled invocation. All 4 Gateway safety tests passed. All 37 frontend unit tests passed, including layout and actual server rendering of the five-cell repository Kafka declarations. Two live Chrome tests passed. Lint, typecheck and build passed; the existing anonymous-export lint warning and Next middleware deprecation remain.

Receipts, exact topology JSON, verification counts and a screenshot are in [evidence/control-center-relationship-graph](evidence/control-center-relationship-graph/). Unit HTTP tests use a synthetic Actuator server; repository manifests remain STATIC_DECLARATION tests, not live Kafka observations. No full repository CI claim is made. The previously reported unrelated Eureka metadata integration failure remains outside this Console/Gateway test scope; unrelated Order/configuration work was not reverted.

## Remaining gaps

- The running environment does not publish Kafka contract manifests to this Console, so no live Kafka links were claimed. Repository contract tests verify 28 existing declared Kafka links and their topic/group rendering. Configure the existing ContractDiscovery/runtime membership path to supply live effective declarations.
- gRPC handlers alone do not identify callers. Only separately configured caller declarations/owner queries can supply gRPC edges; no callers were inferred from registered servers.
- No OTel traffic ingestion, independent runtime health checks or verified epochs/duplicate detection from unverified registry entries were added. Existing secure membership remains separate.
- Route discovery currently covers one explicitly configured local Gateway origin and its aggregate route table. Per-replica Gateway coverage and non-local authenticated route collection are not implemented. Physical destination URLs are unresolved rather than guessed into service IDs.
- Node/edge/provider and HTTP byte budgets remain in force. There is no graph search, saved layout, trace overlay or runtime-control functionality. This is local development functionality, not production readiness.

## Exact files changed in this phase

```text
apex-operations-console/src/main/java/com/apex/operations/console/GatewayRouteTopology.java
apex-operations-console/src/main/java/com/apex/operations/console/LocalEurekaTopologyInventory.java
apex-operations-console/src/main/java/com/apex/operations/console/TopologyApi.java
apex-operations-console/src/main/resources/application-local.properties
apex-operations-console/src/test/java/com/apex/operations/console/GatewayRouteTopologyTest.java
apex-operations-console/src/test/java/com/apex/operations/console/LocalEurekaTopologyInventoryTest.java
apex-operations-console/src/test/java/com/apex/operations/console/LocalEurekaInventoryLiveIT.java
apex-operations-contracts/src/main/java/com/apex/operations/contracts/TopologySnapshot.java
apps/operations-web/package.json
apps/operations-web/pnpm-lock.yaml
apps/operations-web/src/app/globals.css
apps/operations-web/src/components/topology-view.tsx
apps/operations-web/src/components/topology-graph.tsx
apps/operations-web/src/components/topology-details.tsx
apps/operations-web/src/lib/topology.ts
apps/operations-web/src/lib/topology-layout.ts
apps/operations-web/src/lib/topology-layout.test.ts
apps/operations-web/src/lib/topology-rendering.test.ts
apps/operations-web/e2e/live-local-inventory.spec.ts
apps/operations-web/e2e/live-relationship-graph.spec.ts
tools/control-center/verify-local-inventory.ps1
docs/control-center-relationship-graph.md
docs/evidence/control-center-relationship-graph/topology.json
docs/evidence/control-center-relationship-graph/live-acceptance.json
docs/evidence/control-center-relationship-graph/lifecycle.json
docs/evidence/control-center-relationship-graph/verification.json
docs/evidence/control-center-relationship-graph/topology.png
```

Existing Cell YAML edits, Console application.properties edits and the earlier untracked acceptance evidence were preserved.
