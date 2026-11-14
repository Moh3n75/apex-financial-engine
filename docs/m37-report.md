# M37 SDK consolidation review

Status: implementation is uncommitted; full platform acceptance FAILED at Order regression. No commits or pushes were made. SDK, external consumers and Customer/Reporting qualification passed; this is not a full product PASS.

## Artifact and ownership audit

The audited SDK footprint falls from **18 to 13 Maven library artifacts (28% fewer)**. Executable applications, Order contracts, Financial modules and business Cells are excluded from both counts. All 208 existing SDK source/resource/test files were copied and checked before removing old paths. The removal manifest contains 231 original paths, their replacement paths and original hashes; it includes POMs and documentation, not unrelated business files. Ignored historical build outputs remain on disk and are not reactor modules.

The full before graph, including dependency scopes and profile declarations in external consumers, is [m37-audit.json](m37-audit.json). The after graph, runtime closures and 16 unique auto-configuration registrations are [m37-after.json](m37-after.json). [m37-removed-files.json](m37-removed-files.json) lists every removed path. Reproduce structural checks with `python tools/m37/audit-sdk.py`.

| Before | After | Reason |
| --- | --- | --- |
| platform-core, platform-events, platform-grpc-api, operations-contracts | sdk-api | Cohesive public contracts; no Spring/transport runtime. Existing Operations Jackson annotations are retained. |
| spring-boot-starter | sdk-spring-boot | Default context integration and API entry point. |
| platform-messaging, starter-cell-messaging | sdk-messaging | Wrapper had no production sources or independent activation. Both test suites retained. |
| platform-grpc / discovery-grpc | sdk-grpc / sdk-discovery-grpc | Runtime and explicitly selected discovered channels remain separate to preserve static-channel behavior. |
| starter-grpc-client / starter-grpc-server | sdk-grpc-client / sdk-grpc-server | Retained as meaningful role selectors: independent Boot client/server starter graphs. |
| platform-discovery | sdk-discovery | Shared abstractions must remain independent of providers and RPC. |
| discovery-eureka / discovery-kubernetes | sdk-discovery-eureka / sdk-discovery-kubernetes | Separate optional providers avoid pulling Eureka into Kubernetes applications. |
| starter-webmvc / starter-observability | sdk-webmvc / sdk-observability | Keep servlet and diagnostics opt-in. |
| platform-contract-discovery, operations-owner-jdbc | sdk-operations | Unified operational adapters. JDBC, messaging, RPC and WebMVC remain optional. |

The candidate nine-artifact layout was adjusted to retain client/server role selectors, a separate Eureka provider and explicit discovered-channel integration. Combining them would undermine dependency isolation or change activation for static RPC consumers. No duplicate auto-configuration implementations were deleted: the classes perform distinct jobs. Registration lists were merged and checked for duplicate entries and missing classes. No business implementation or allegedly unused public source was deleted. The one production-free messaging wrapper was removed, along with obsolete publication boundaries.

## Public API and dependency changes

All Java package names, public type signatures, protobuf contracts, SQL migrations, event metadata, Cell identities and configuration keys are retained. Maven coordinates change according to the audited mapping. SDK modules share version `1.0-SNAPSHOT`; independent consumers use `apex.sdk.version`. There are no legacy relocation JARs. See [recommended dependency usage](apex-sdk.md).

The default Boot graph contains API and context only. API adds only the existing Operations contract Jackson annotations, with no Kafka, database, RPC transport, servlet or discovery runtime. Operations no longer transitively installs messaging, RPC or JDBC; consumers select those capabilities explicitly. Missing messaging/RPC classes cause metadata auto-configuration to back off. Directory-based RPC retains a separate SDK capability and has a client-classpath guard. No combined client/server starter is introduced. Eureka explicitly includes Boot's non-servlet `spring-boot-web-server` event API, required by its auto-registration implementation even for non-web consumers; WebMVC remains absent.

The reactor, standalone POMs, verification commands, artifact path assertions, receipt fingerprints and current READMEs were updated. Historical milestone reports remain historical. Financial modules and applications remain independent; their dependency declarations changed only where required. The supplied Operations Console application properties were not changed.

## Verification evidence

Java: `C:/Program Files/Java/jdk-21`. Maven: locally available 3.9.11. Initial offline generation lacked `spring-security-crypto:7.0.7`; an online build resolved it successfully.

- All 13 final SDK modules built and installed successfully. Final affected RPC, discovery and Console build: `out/m37/final-rpc-console.log`. Additional SDK evidence: `out/m37/sdk-build-online.log`, `sdk-remaining.log`, `eureka-final.log` and `customer-reporting-isolated.log`. Messaging verifies Kafka publish/consume, transactional Inbox/Outbox, retry/DLT/replay. RPC suites exercise request/query, authorization, deadlines and discovered channels. Discovery provider tests cover Eureka lifecycle and Kubernetes adapter compatibility. Operations covers contract discovery, HTTP metadata and bounded JDBC owner evidence. HTTP propagation and observability suites passed. Kubernetes was tested through its existing provider test suite, not against a live cluster.
- Seven external installed-consumer modes passed outside this repository: default, HTTP, capabilities, observability, RPC client, RPC server and Operations. Each includes dependency-tree checks; non-capability modes additionally boot packaged JARs. Capabilities exercises owned PostgreSQL/Kafka, real HTTP/2 RPC and an owned Eureka registry. Final-layout receipt: `out/m35-6/external/20261112095423/receipt.json`; log: `out/m37/external-final-boundaries.log`.
- Standalone Customer, sample gRPC client, Gateway and Operations Console `verify` passed. Console tests include existing topology/communication graph checks. Results: `out/m37/standalone-results.json` and per-application logs.
- Order `verify` failed (42 tests, 7 failures, 7 errors), including missing `EventHandlerRegistry` in `SagaSubscriptionPreflight`. A focused run of `OrderGrpcLocalTest` using the original committed POM and old installed dependencies reproduced the same two startup errors: `out/m37/order-baseline.log`.
- Inventory `verify` failed (36 tests, one failure): base Kafka listener is running when the smoke test expects paused. The original committed POM reproduced the same failure: `out/m37/inventory-baseline.log`.
- Payment `verify` failed (19 tests, three failures): simulation decline/no-policy expectations differ from existing runtime behavior. The original committed POM reproduced the same three failures: `out/m37/payment-baseline.log`.
- Reporting direct `verify` initially failed its clean-build receipt prerequisite (21 tests, one error). The final isolated `tools/m22/verify-cells.ps1` run passed: clean SDK build/install, Customer verify, valid source-and-artifact receipt, Reporting verify and final source-fingerprint check. Log: `out/m37/customer-reporting-isolated.log`; result: `out/m37/customer-reporting-isolated-result.json`.

The Eureka lifecycle test now explicitly enables registration. External tests now validate the current explicit provider policy, wait through initial stale snapshots, and disable native Eureka registration for the read-only consumer. These fixture changes preserve runtime behavior and keep actual transport/registry assertions. An Operations filtered-classloader test and three external isolation modes verify the newly optional boundaries.

Full acceptance initially stalled while the archive step recursively searched nonexistent target paths. A directory-existence check fixes that. A later attempt failed because a generated SDK JAR was temporarily locked; the retry proceeded after the lock cleared and FAILED at Order (42 tests, six failures and eight errors under the discovery profile). Log: `out/m37/platform-retry.log`. Identity and SDK/discovery prerequisites passed. Later full-chain stages, Financial runtime qualification and full packaged cross-Cell acceptance were not reached. Console topology tests passed; a separate live Operations deployment was not qualified.

A later Customer/Reporting chain attempt encountered one Kafka quarantine-topic metadata provisioning failure. Its XML is retained in `out/m37/topic-provisioning-retry-failure.xml`; the isolated full chain subsequently passed without test or runtime changes. An earlier chain's results were invalidated correctly when the final discovery boundary changed the source fingerprint; only the final isolated run qualifies Customer/Reporting.

Final SDK reports total **246 tests, zero failures/errors/skips**. A final clean rebuild of RPC, discovery integration and Console passed (`out/m37/final-clean-rpc-console.log`), eliminating compiled classes left from the intermediate layout. The packaged SDK class ownership audit found no duplicate Apex classes across the 13 JARs.

Machine-readable final JUnit totals, failing cases, external JAR receipts and verification status are in [m37-verification.json](m37-verification.json). Standalone JUnit directories may contain prior-run reports; command results and the explicitly named logs establish each run's result. PowerShell parsing passed for all 20 changed verification scripts; SDK structural audit and `git diff --check` passed. Historical frozen OPS evidence and selective-staging guides remain historical rather than being rewritten to pretend they qualify M37.
