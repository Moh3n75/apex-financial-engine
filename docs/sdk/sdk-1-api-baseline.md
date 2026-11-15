# SDK-1 public Java API baseline

Checkpoint: `8a2f822` on `credit-engine-event-sourcing` (Java 21). SDK-0 and SDK-0.1 are committed in that checkpoint. SDK-1 is uncommitted; no package, event model, existing public signature or authentication contract has been removed or renamed.

## Supported surface and implementation boundaries

The complete machine inventory is [api-inventory-v1.tsv](../../tools/sdk-1/baseline/api-inventory-v1.tsv); the public/protected signatures and JVM descriptors are in [public-api-v1.txt](../../tools/sdk-1/baseline/public-api-v1.txt). They cover all 13 SDK artifacts, including the two dependency-only role artifacts. The inventory includes named package-private hierarchy support for a deliberately conservative guard; a `jvmPublic=false` row does not become a supported application API merely because its module is `apex-sdk-api`.

Inventory totals: **252 named classes, 220 JVM-public entries**, including nested/generated types; **64 public API record/enum/annotation types** have a separate semantic-shape fixture. Role artifacts have no Java declarations. The class inventory is not a count of supported services or runtime features.

| Artifact | Supported API / SPI / configuration family | Boundary |
| --- | --- | --- |
| apex-sdk-api | `com.apex.platform.core.context`, `events`, `events.api`, `events.saga`, `grpc.api`, `com.apex.operations.contracts` | Public Java models, annotations, facades, gateways, options/results and owner SPIs; no Spring/Kafka/network runtime |
| apex-sdk-spring-boot | Context provider auto-configuration and bean replacement | Default capability; no transport activation or mandatory identity settings |
| apex-sdk-webmvc | `com.apex.platform.webmvc` servlet filter and auto-configuration | Servlet only; existing root-context behavior is preserved for SDK-2 to extend |
| apex-sdk-messaging | `consumer`, `kafka`, `outbox`, `jdbc` public adapters and policies | Typed events and selected JDBC/Kafka adapters; owner supplies DB/migrations and Kafka policy |
| apex-sdk-grpc | `grpc.api`, `grpc.security`, selected public integration helpers | Java-model request/query/collection; role artifacts select dependencies, flags activate runtime |
| apex-sdk-grpc-client | No Java declarations | Dependency-only client role; no server starter |
| apex-sdk-grpc-server | No Java declarations | Dependency-only server role; no client starter |
| apex-sdk-discovery | `com.apex.platform.discovery` directory/endpoint/membership interfaces and policy | Provider-neutral; no implicit registration |
| apex-sdk-discovery-eureka | Eureka auto-configuration | Explicit provider plus separate native Eureka lifecycle flags |
| apex-sdk-discovery-kubernetes | EndpointSlice discovery adapter and auto-configuration | Explicit provider; live-cluster qualification remains deferred |
| apex-sdk-discovery-grpc | Resolver/bridge integration | Explicit directory plus enabled gRPC client; no mutation retry |
| apex-sdk-observability | `com.apex.observability` observer/exporter/metrics contracts | Optional bounded diagnostics; no tracing/security promise |
| apex-sdk-operations | `com.apex.operations.owner`, `com.apex.platform.contracts` adapters | Owner metadata reads/declarations; no automatic public endpoint or authorization policy |

`internal` is an implementation designation, **not permission to break an already linked class**. Operations references `HandlerRegistry` and `Contracts`; the discovery bridge references `DiscoveredCellChannelFactory`. `RpcCodec` is also publicly constructible and used by compatibility fixtures. Their public nested types and public/protected members remain in the guard. `consumer.internal` handler discovery and generated `grpc.internal.wire` classes remain implementation surfaces; generated wire descriptors are additionally frozen by protocol fixtures. Applications should choose the public facades/SPIs above for new code. There is no deprecation/removal migration in SDK-1.

## Machine gate and its limits

Run after compiling all SDK modules:

```powershell
& "$env:JAVA_HOME/bin/java" tools/sdk-1/SdkApiBaseline.java check .
& "$env:JAVA_HOME/bin/java" tools/sdk-1/SdkApiBaseline.java self-test .
```

The Java 21-only tool invokes `javap -protected -s -constants` without loading SDK classes or resolving their optional runtimes. It inventories named class files across all modules (anonymous bodies excluded), retaining type modifiers, generic declarations, superclasses/interfaces, constructors, public/protected fields/methods, constants and JVM descriptors, including compiler bridge methods. Private implementation bodies are absent. The baseline is an **exact, conservative declaration snapshot**: it rejects binary breaks and also some compatible additions/source-contract changes. It is not an implementation of every directional compatibility rule in the JLS. This stricter review policy is intentional. [Java 21 binary compatibility rules](https://docs.oracle.com/javase/specs/jls/se21/html/jls-13.html) distinguish binary compatibility from source and behavioral compatibility; neither this tool nor a JAR checksum proves all behavior.

Self-tests compile valid old/new classes and demonstrate rejection of parameter/return descriptor changes, static/instance changes, finalization, reduced protected visibility, removed methods, superclass changes and inlined constant changes. A method-body-only change passes. The checker never automatically updates an existing baseline. API changes require an explicit reviewed baseline version and migration/compatibility explanation; preserve old fixtures and adapt the gate to the next version rather than regenerating v1 to make a failure disappear. Raw Maven tests run contract fixtures; `tools/sdk-0/verify-sdk.ps1 -Sdk1` is the complete API/config/protocol verification entrypoint.

The separately retained [checkpoint-api.txt](../../tools/sdk-1/baseline/checkpoint-api.txt) and [checkpoint-inventory.tsv](../../tools/sdk-1/baseline/checkpoint-inventory.tsv) were captured before SDK-1 production edits. Comparison added only the package-private `DiscoveryPolicyAutoConfiguration` declaration; existing declarations were unchanged. All new preflight methods are package-private. The final v1 snapshot explicitly includes that new implementation type.

## Old binary consumer and dependency alignment

[FrozenConsumerV1.java](../../tools/sdk-1/baseline/FrozenConsumerV1.java) was compiled against the installed checkpoint API. Its frozen class bytes are [versioned separately](../../tools/sdk-1/baseline/frozen-consumer-v1.class.b64), with the original compiler input JAR SHA256 retained. Installed-consumer verification executes those bytes against the newly installed API without recompiling them. It exercises context scopes, event envelopes/schema declarations, request/query options and an Operations query. This is sampled positive linkage evidence, complemented by the exhaustive declaration guard; it is not exhaustive invocation coverage of every method.

The existing `apex-financial-engine` parent/importable POM now manages all 13 SDK versions and Spring Cloud 2025.1.3. Boot 4.1.1 remains the inherited dependency alignment; the metadata processor uses the matching `apex.spring-boot.version`. SDK child dependencies no longer repeat `${project.version}` and three provider POMs no longer repeat the Cloud BOM. No new module or BOM artifact was introduced. Dependency management imports versions; it does not add transport dependencies.

SDK-only compiler processor paths use that centrally pinned version. They are not installed into the shared root compiler path: the non-SDK `apex-domain` module uses Lombok. A focused parent/domain compile was retained to verify existing processor discovery after this scoping correction; it is not a business runtime acceptance test.

An independent Boot consumer may import the existing parent POM in `dependencyManagement` and select only the capabilities it needs:

```xml
<dependencyManagement><dependencies><dependency>
  <groupId>com.apex</groupId><artifactId>apex-financial-engine</artifactId>
  <version>1.0-SNAPSHOT</version><type>pom</type><scope>import</scope>
</dependency></dependencies></dependencyManagement>
<dependencies><dependency>
  <groupId>com.apex</groupId><artifactId>apex-sdk-spring-boot</artifactId>
</dependency></dependencies>
```

The standalone fixture has its own Boot parent and a deliberately different project version, `0.0.1-EXTERNAL`. It has no reactor parent/source-directory dependency. Optional capability dependencies remain optional. Check actual installed-consumer results in [verification](sdk-1-verification.md), not a historical receipt. Java 21/Boot 4.1.1/Cloud 2025.1.3 is the tested alignment, not a claim of upstream enterprise support or arbitrary mixed-version support. Immutable remote publication, an empty-cache release consumer, version-skew policy and legacy coordinate relocation remain release-gate work.
