# SDK-1 compatibility matrix

Reference checkpoint `8a2f822`. Fixtures live under `apex-sdk-operations/src/test/resources/sdk-1/v1/` and `tools/sdk-1/baseline/`. Their `v1` identifies the fixture set; named application contract v2 cases are intentional within it. No product event schema or public Java package was redesigned.

| Boundary | Frozen evidence and positive cases | Incompatible/negative cases | Limits |
| --- | --- | --- | --- |
| Java binary API, all 13 artifacts | Public/protected declaration/descriptors/constants snapshot; checkpoint snapshot; old compiled consumer executed with current installed API | Eight compiled mutation controls; exact baseline rejects any declaration difference | Conservative exact gate can reject compatible additions; no universal behavioral-equivalence proof |
| All API records/enums/runtime annotations | `public-contract-shapes.txt` enumerates all public API model components/types, enum values, effective runtime accessor/field annotations and annotation defaults | Shape/annotation/default changes fail without versioned review | Reactor enumeration; arbitrary custom application DTOs remain owner contracts |
| Kafka metadata/envelopes | Frozen v1/v2 JSON decoded through actual SDK types/dispatcher; exact type/version routing to corresponding payload; preserved IDs/provenance/time and nullable causation | Unknown event version fails reliable dispatch; invalid declaration version rejected | schemaId describes declaration, is **not serialized** in EventMetadata or a secondary routing key; no schema registry or generalized decoder hardening added |
| gRPC request/query Java models | Separate request/query annotations, explicit version 1/2, typed payload/response codec fixtures | Wrong/unknown JSON fields, duplicate keys and missing required v2 response data rejected | No automatic old/new DTO coercion; owners must version breaking changes |
| Response/receipt bytes | Frozen v1 response and current same-version stored response round-trip; v2 has explicit currency field and contract version | v2 response → v1 reader and v1 stored response → required v2 reader both reject | No universal durable owner receipt store or principal/version-key migration supplied |
| Generated protobuf | Frozen `.proto`, generated field names/numbers and service/method identity; frozen binary containing unknown field 99 preserved on round-trip | Field type/number/service/source changes require explicit protocol review; mutated int32→int64 control rejected | Exact source freeze is stricter than protobuf's additive wire compatibility; generated code namespace remains internal |
| Operations | Frozen OwnerSearch/OwnerRecentSearch query, manifest schema v1/v2, all public model shapes/constants; immutable lists and strict round-trips | Manifest v3 rejected; old constructor and absent grpcDependencies semantics preserved | Declarations are not traffic/commit proof; one-transport manifest capability remains the existing G15 limitation |
| SQL | Immutable V1001/V1002 text; real PostgreSQL columns/types/order, composite Inbox PK and check/duplicate constraints | Mutated applied CHECK constraint rejected; invalid version and duplicate rows rejected by PostgreSQL | New migrations require reviewed versioned fixtures; no automatic destructive migration or production upgrade qualification |
| Configuration | All generated metadata compared with catalogue; typed defaults checked against bound objects; identity precedence and disabled/invalid cases | Mutated metadata payload default rejected; invalid provider, refresh, payload/deadline, concurrency/interval rejected | Only supported Apex/native prerequisites documented; no silent redefinition of native settings |
| Dependencies | Existing parent/importable POM aligns 13 SDK versions, Boot/Cloud; independent consumer has different parent/project version | Runtime dependency tree rejects unselected transports, executable/business artifacts and role contamination | Same-version local installed artifacts; arbitrary dependency overrides/mixed releases unsupported until qualified |

`RpcCodec` remains strict: adding a response field at the same contract version is not automatically backward compatible. Event payload compatibility is version/handler-specific and uses the configured messaging mapper; SDK-1 does not claim a new universal strict event decoder. Changing schemaId alone cannot upgrade an event's routing version. Existing Outbox/Inbox IDs, Java model constructors, protobuf field tags, and applied SQL must remain compatible or have an explicit migration.

## Installed combinations

| Mode | Capability behavior | Dependency isolation expected |
| --- | --- | --- |
| default | Context/custom bean replacement and packaged boot | No Kafka/JDBC/gRPC/Eureka/Fabric8/servlet runtime |
| http | Servlet context filter and packaged HTTP boot | No messaging, RPC or discovery |
| messaging | Provisioned source/DLT, transactional Outbox → handler effect + Inbox | Kafka/JDBC present; no gRPC/Eureka/Fabric8/MVC |
| rpc-client | Role isolation, SDK runtime off until selected, packaged boot | Client starter only; no server starter/DB/Kafka/provider |
| rpc-server | Role isolation and packaged boot | Server starter only; no client starter/DB/Kafka/provider |
| discovery-eureka | Provider installed, SDK none/backoff and native Eureka disabled for isolation | No K8s/Kafka/JDBC/gRPC/MVC |
| discovery-kubernetes | Provider installed, SDK none/backoff | No Eureka/Kafka/JDBC/gRPC/MVC |
| operations | Optional adapter/declaration backoff and packaged boot | No optional transport/JDBC activation merely from installation |
| observability | Diagnostics/export fixture and packaged boot | No transport/provider activation |
| capabilities | Actual installed Messaging, local/remote gRPC + collection, Operations SQL deadline, owned Eureka read/registration-off | Explicit combined graph; source-consistent registry fixture |

Read actual outcomes and artifact fingerprints in [verification](sdk-1-verification.md). Provider-isolation modes do not establish a live Kubernetes deployment; real API/TLS/RBAC/token-rotation qualification remains SDK-6. The combined fixture uses an owned, rebuilt registry JAR; other business applications are not launched. No SDK test result implies full product acceptance.
