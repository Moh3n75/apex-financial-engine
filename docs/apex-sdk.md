# Apex SDK

The SDK is one coordinated product at **1.0-SNAPSHOT**, built with Java 21 and Spring Boot 4.1.1. Each Cell remains independently built and deployed with its own database and identity. SDK ownership covers execution context, contracts, transports, discovery and operational adapters; business contracts and persistence remain with their owning Cells.

Standalone consumers define `<apex.sdk.version>1.0-SNAPSHOT</apex.sdk.version>` and use that version for every `com.apex:apex-sdk-*` dependency. SDK modules inherit the same root version. Upgrade the complete selected set together; old artifact coordinates are removed, with no forwarding wrappers. Existing Java imports and `apex.*` configuration keys remain valid.

| Artifact | Use and dependency boundary |
| --- | --- |
| apex-sdk-api | Core, Events, typed RPC and Operations contracts. No Spring or transport runtime; Jackson annotations support the existing Operations wire contracts. |
| apex-sdk-spring-boot | Recommended default entry point: API and shared execution context, with no network, database or servlet runtime. |
| apex-sdk-messaging | Kafka, transactional Outbox/Inbox, JDBC adapters, PostgreSQL driver, consumer reliability, retry/DLT/replay. Select only for messaging Cells. |
| apex-sdk-grpc | RPC runtime; Boot client/server integrations are optional. No server starter transitively. |
| apex-sdk-discovery-grpc | Explicit opt-in for directory-based RPC channels; includes the client role and discovery abstractions. Retains the original activation boundary. |
| apex-sdk-grpc-client | RPC plus Boot client starter; select for remote callers. Does not include the server starter. |
| apex-sdk-grpc-server | RPC plus Boot server starter; select for responders. Does not include the client starter. Select both roles for both capabilities. |
| apex-sdk-discovery | Provider-neutral directory, endpoint validation and Spring Cloud discovery abstractions. |
| apex-sdk-discovery-eureka | Optional Eureka provider, registration metadata and freshness. Does not pull in RPC runtime. |
| apex-sdk-discovery-kubernetes | Optional Fabric8 provider. Does not pull in Eureka or RPC runtime. |
| apex-sdk-webmvc | Optional servlet integration and HTTP execution-context filter. |
| apex-sdk-observability | Optional bounded operational diagnostics and Micrometer metrics; no exporter or Console dependency. |
| apex-sdk-operations | Contract discovery and owner-side JDBC evidence. Messaging, RPC, JDBC and WebMVC dependencies are optional. Select required adapters explicitly. |

For a plain Boot application:

```xml
<dependency>
  <groupId>com.apex</groupId>
  <artifactId>apex-sdk-spring-boot</artifactId>
  <version>${apex.sdk.version}</version>
</dependency>
```

Add `apex-sdk-webmvc` for HTTP, `apex-sdk-messaging` for durable events, the relevant RPC role(s), and exactly the discovery provider the application needs. Provider configuration remains `apex.discovery.provider=eureka` or `kubernetes`. RPC roles remain explicitly activated by `apex.grpc.client.enabled` / `apex.grpc.server.enabled`; merely installing contracts or discovery starts no RPC server. Existing TLS, peer authorization and loopback-only test restrictions remain in force.

Operations contracts are available through API. Owner evidence users add `spring-jdbc` and their own datasource/driver (or obtain them from selected messaging integration). Contract discovery currently introspects both messaging and RPC registries: select `apex-sdk-messaging` and `apex-sdk-grpc` and enable `apex.contract-discovery.enabled`. Without those classes, its auto-configuration backs off. Local HTTP metadata additionally requires WebMVC and the existing explicit local profile/property constraints. Operations never owns another Cell's database.

Build the SDK with `mvn -B -ntp -pl apex-sdk-api,apex-sdk-spring-boot,apex-sdk-messaging,apex-sdk-grpc-client,apex-sdk-grpc-server,apex-sdk-discovery-grpc,apex-sdk-discovery-eureka,apex-sdk-discovery-kubernetes,apex-sdk-webmvc,apex-sdk-observability,apex-sdk-operations -am install`. Run `tools/m35-6/verify-platform-product.ps1` for the existing full regression chain and external installed-consumer acceptance. Fast mode does not establish full acceptance.

The complete coordinate migration, pre-change dependency graph (including profiles), source hashes and removed-file manifest are in [m37-audit.json](m37-audit.json) and [m37-removed-files.json](m37-removed-files.json). See [m37-report.md](m37-report.md) for actual verification outcomes.
