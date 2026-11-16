# AOR-01 compatibility matrix

| Boundary | Before | After / evidence requirement |
|---|---|---|
| Legacy Maven coordinate | apex-sdk-messaging:1.0-SNAPSHOT | Preserved with its PostgreSQL driver and Boot JDBC starter; not database-neutral |
| Neutral coordinate | No orchestration entry point without manual exclusions | apex-sdk-messaging-core:1.0-SNAPSHOT, driver-free tested composition; legacy classes/resources remain transitive |
| Public Java/JVM API | SDK-1 public-api-v1 and artifact inventory | Exact original types stay in original artifact; constructors/descriptors unchanged. New internal declarations checked separately in public-api-aor-01-v2; no v1 edits |
| Configuration v1 | Frozen catalogue/defaults | Exact unchanged comparison for all 73 v1 module/property entries; new storage selection property checked separately in configuration-aor-01-v2 |
| Default selection | Implicit PostgreSQL factories | Legacy still defaults PostgreSQL. New neutral composition defaults custom via marker. Explicit unsupported provider fails; no Oracle implementation |
| Publisher/consumer defaults | Publisher true inside selected messaging, consumer false | Preserved; all four combinations tested for legacy and neutral custom storage |
| Custom beans | ConditionalOnMissingBean per adapter | Preserved for appender/store/Inbox. Multiple ambiguous storage candidates or missing selected custom implementation fail preflight |
| Default PostgreSQL creation | In Cell configuration | Separate internal configuration imported only through selected Cell configuration; no duplicate defaults |
| Datasource/transactions | Owner JdbcTemplate/manager and native transactional proxies | Same ownership/proxies, no XA; new deferred product validation performs no construction-time connection |
| SQL/migrations | V1001/V1002 and existing tables/keys | Exact bytes/checksums and PostgreSQL SQL strings/mappings preserved |
| Claims/recovery | Bounded SKIP LOCKED, lease/fence transitions | Same implementation; new real-PG race/retry/fence/rollback checks |
| Crash window | ACK before mark can duplicate | Preserved and demonstrated using real PG+Kafka with simulated loss before mark, lease expiry and new publisher; not an OS-kill qualification |
| Inbox | Composite group/event key, transaction with handler | Same registration SQL and executor; concurrent duplicate and handler rollback checked against real PostgreSQL |
| Kafka retry/DLT/topic provisioning | Existing capability flags and readiness rules | No implementation changes; original reliability/provisioning/routing suites retained |
| Event/protobuf/response formats | SDK-1 frozen protocol/fixtures | Unchanged; exact fixture guards and frozen consumer remain required |
| Installed PostgreSQL consumer | Existing coordinate/config, native owner datasource | Original external fixture unchanged; real durable Outbox→Kafka→Inbox/effect verification against installed jars |
| Installed neutral consumer | Unavailable | Own Boot parent, installed core coordinate, no driver/datasource activation, compiled/packaged boot, explicit negative selections |

The API checker now partitions actual declarations using the unchanged v1 inventory: every old class must remain in its original artifact and every old declaration must match exactly. Additions must match the separate v2 extension exactly; unexpected module/class additions also fail. The configuration checker similarly compares its single explicit additive entry separately before running the unchanged exact v1 catalogue comparison. This is a guard extension, not regeneration of v1 evidence.

Read aor-01-verification.md for actual runs, counts, failures and limits. This matrix describes contract intent and does not itself constitute runtime evidence. Full physical JAR separation remains deferred under the exact v1 artifact constraint.
