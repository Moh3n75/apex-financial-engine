# M32: explicit read-only multi-responder gRPC

APEX uses **MultiResponderGateway** for a bounded set of Java queries. It composes
the existing QueryGateway; it is not a bus or a collector for Kafka subscribers.
The Platform's existing internal unary protobuf service is unchanged. Teams still
write Java records and ordinary Spring handler methods, with no business `.proto`,
generated stub, shared database or other Cell implementation dependency.

## Choose the operation

| Need | API / semantics |
|---|---|
| Create one Order and return its committed orderId | RequestReplyGateway; explicit target + persistent owning-DB idempotency |
| Read one owning Cell | QueryGateway; explicit remote gRPC or configured local route/queryLocal |
| Read several explicitly known Cells | MultiResponderGateway; concurrent independent read perspectives, partial/unknown outcomes |
| Publish durable domain facts / advance a Saga | CellEvents + local Outbox + Kafka; asynchronous, at least once, Inbox/business idempotency |

Kafka does not identify a finite set of responders or guarantee a synchronous reply.
Do not interpret a collected read or broker ACK as completed Payment, authorization,
compensation or global transaction. Different owner perspectives can legitimately
reflect different instants of an eventually consistent workflow.

## Java contract and handlers

`apex-order-contracts` remains a lightweight versioned DTO/API artifact:

```java
@CellQuery(type = "GET_ORDER_PERSPECTIVE", version = 1, response = OrderPerspective.class)
public record GetOrderPerspective(UUID orderId) {}
public record OrderPerspective(UUID orderId, String owner, String state) {}
```

Each independently packaged Cell depends on the contract artifact and
`apex-starter-grpc-server`, and registers its own public method:

```java
@Component
@Profile("grpc")
class InventoryPerspectiveHandler {
    private final JdbcTemplate jdbc; // Inventory's own DataSource only
    @CellQueryHandler(type = "GET_ORDER_PERSPECTIVE", version = 1)
    @Transactional(readOnly = true, timeout = 5)
    public OrderPerspective perspective(GetOrderPerspective query) {
        // SELECT status FROM inventory_reservations WHERE order_id=?
        // Missing local fact -> GatewayException(NOT_FOUND, false).
    }
}
```

Real implementations:

- Order: `order-cell/.../modules/order/grpc/OrderPerspectiveHandler.java`, existing OrderRepository.
- Inventory: `inventory-cell/.../modules/inventory/grpc/InventoryPerspectiveHandler.java`, local reservation table.
- Payment: `payment-cell/.../grpc/PaymentPerspectiveHandler.java`, local payment_authorizations table.

No handler invents a reservation/payment, updates business status, appends an event
or registers an Inbox receipt. Missing local facts remain NOT_FOUND. Inventory and
Payment add an opt-in `grpc` profile; their default/Saga/replay configuration is
unchanged. Payment retains its existing policy/preflight prerequisites: the fixture
uses `simulation,grpc` only to satisfy that dependency, never authorizes a payment
from a query or invents a default decision.

## Collector API

```java
var options = new CollectionOptions(
        List.of("order-cell", "inventory-cell", "payment-cell"),
        Duration.ofSeconds(2), true);
CompletionStage<CollectionResult<OrderPerspective>> pending =
        multiResponderGateway.collectQueries(
                new GetOrderPerspective(orderId), OrderPerspective.class, options);
```

The record constructor sorts the distinct IDs. Results have exactly one slot per
target, in deterministic lexical order, including a unique invocationId per attempt:

| Status | Meaning |
|---|---|
| SUCCESS | Typed owner response, answerUnknown=false |
| NOT_FOUND | Owner has no corresponding local fact, answerUnknown=false |
| TIMED_OUT | Overall/native deadline elapsed; answerUnknown=true |
| UNAVAILABLE | Transport/server unavailable; answerUnknown=true |
| FAILED | Safe Gateway error code, including permission/contract/infrastructure errors; answerUnknown=true |
| CANCELLED | Read was cancelled; answerUnknown=true |

These are read-result semantics, not business approval/decline. `allowPartial=false`
requires every target SUCCESS; it waits for the bounded result set rather than
dropping targets on the first error, then fails with CollectionFailedException
whose result() retains all slots. A legitimate NOT_FOUND is still an incomplete
all-success collection, while remaining distinguishable from transport failure.

`pending.toCompletableFuture().cancel(true)` attempts cancellation of outstanding
gRPC/local futures. CollectionCancelledException.result() preserves completed reads
and marks other slots CANCELLED. It cannot roll back a completed read or guarantee
that remote code has stopped instantaneously. No automatic retries are introduced.
Use async completion handlers for blocking follow-up work, rather than blocking
the gateway's transport/deadline callback threads.

One monotonic total budget includes validation and queue wait; remaining deadlines
are passed to every concurrent unary call. Terminal slots are published before
transport cleanup; callbacks/cancellation never run under the collection state lock.
Cancellation is best effort and independent owner transactions remain bounded.

Validation rejects writes/missing @CellQuery, incorrect response class, duplicate,
empty, malformed or unknown responders before dispatch. Configured limits reject a
whole collection before any calls if capacity is exhausted. Limits default to16
responders/call,16 active collections,64 reserved in-flight attempts,4 launch workers
and16 queued launch tasks. Public max16 responders and1ms..30s deadline are hard caps;
configuration can tighten them. The existing deadline scheduler removes cancelled
tasks promptly. No global mutable response registry is introduced.

## Explicit routing and opt-in

```yaml
apex:
  grpc:
    cell-id: operations-client
    client.enabled: true
    collector:
      enabled: true
      allowed-responders: [order-cell, inventory-cell, payment-cell]
      max-responders: 16
      max-collections: 16
      max-in-flight: 64
    destinations:
      order-cell: {channel: order}
      inventory-cell: {channel: inventory}
      payment-cell: {channel: payment}
spring:
  grpc:
    client:
      channel:
        order: {target: 'static://order.internal:9091', ssl: {enabled: true, bundle: internal}}
        inventory: {target: 'static://inventory.internal:9092', ssl: {enabled: true, bundle: internal}}
        payment: {target: 'static://payment.internal:9093', ssl: {enabled: true, bundle: internal}}
```

Supply the named Boot `internal` SSL bundle through operator-managed configuration,
with client key material and trusted server CA. Never commit private keys/passwords.
There is no implicit network endpoint. Collector defaults OFF; enabling it without
a gateway, valid routes/allowlist and bounded policy fails startup. A local target
requires `destinations.<id>.local=true` and the same collector allowlist. Local calls
are trusted in-process queries, not claimed to be authenticated TLS calls. Remote
collections are rejected inside an active Spring transaction; invoke outside it.

## Authenticated transport and authorization

M31's production mTLS checks remain. The optional static certificate policy reuses
PeerIdentityVerifier and adds InvocationAuthorizer before deserialization/business work:

```yaml
apex:
  grpc:
    server.enabled: true
    insecure-loopback-test-only: false
    security:
      enabled: true
      tenant: alpha
      peers:
        operations:
          uri: spiffe://apex.internal/tenant/alpha/cell/operations-client
          contracts: [QUERY:GET_ORDER_PERSPECTIVE:1]
spring:
  grpc:
    server:
      ssl: {enabled: true, bundle: internal-server, client-auth: require}
```

The transport validates the certificate chain. Policy requires exactly one URI SAN,
an exact configured URI, a matching owning tenant and explicit kind/type/version
grant. The URI supplies the verified caller identity; a caller-name header cannot
authenticate it. The URI profile is SPIFFE-style naming, not a SPIRE/attestation
implementation. One owning tenant is supported per endpoint; this is not generic
multi-tenant row filtering or a production RBAC product. Foreign tenants are rejected
before querying this endpoint's data. Each Cell independently grants its permitted
peers/contracts; an observer may be authorized on Order while denied on Payment.

Custom existing peer verifiers remain supported when the new static policy is OFF.
Do not configure competing verifier/authorizer beans. Security + unverified-loopback
mode is rejected. Untrusted/missing certificates or plaintext fail the real TLS
handshake; valid but unauthorized peers fail afterward with PERMISSION_DENIED;
bad/missing workload SAN and forged origin fail UNAUTHENTICATED. Live identity
provisioning, revocation, certificate rotation and deployment ACL/failover remain
separate operator qualifications. No private material is checked into this repository.

## Write receipt retention and upgrade safety

Queries/collections do not use order_request_receipt. M31 CreateOrder still claims
and persists receipt + business + Outbox in one owning transaction and returns after
commit. Existing keys remain long-lived; there is no expiry/reuse/cleanup job.
Same operation/key returns the original v1 result; changed payload/caller/tenant or
contract version conflicts. A version without a deployed handler is UNIMPLEMENTED.
Cached bytes incompatible with the registered response model now fail
FAILED_PRECONDITION on the server, rather than emitting an apparently successful
reply that fails client decoding. No new business operation is run to replace it.

Keep old handlers/response representations for the supported retry horizon, or
deploy a deliberate tested adapter. Never silently decode a v1 receipt as v2,
delete a receipt or retry under a fresh key to hide an unknown prior outcome.
FAILED_PRECONDITION here requires operator/owner version reconciliation or a safe
owning read; it is not a business decline. There are no migration changes in M32.

## Repeat the acceptance

```powershell
$env:JAVA_HOME = '<Java 21 JDK>'
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File tools/m32/verify-m32.ps1 -Offline -MavenCommand mvn.cmd
```

Use Maven3.9.x with cached dependencies/native PostgreSQL/protoc. Default runner:
installs current Platform/Starters/contracts; executes the complete M31 verifier
with M30 Financial/Saga/Replay/Customer/Reporting regression; verifies source/JAR
SHA-256 receipts; then executes packaged M32 tests. It never uses stale installed
Cells as evidence. `-Focused` is development-only assembly + M32 tests and explicitly
does not qualify the full regression gate. No final acceptance uses skipTests.

The M32 fixture starts real Order/Inventory/Payment JARs and separate client JARs,
three native PostgreSQL instances and runtime-generated CA/server/client certificates.
Independent fixture connections seed/read each owner's facts; this read-only example
does not claim those seeded facts are a completed Kafka Saga. Kafka consumers and
publishers are disabled/stopped; a test-owned loopback connection trap proves no
Kafka connection/publish occurs, so no broker is needed for M32 itself. The unchanged
prior acceptance still exercises real isolated Kafka and packaged Saga/replay.

Faults use owned SQL locks/table rename, actual process stop/restart and certificate
failures, never production endpoints or test hooks in business code. Every owner
table snapshot is compared after each test; query collection must not alter stock,
status, Inbox, Outbox, receipts or payment decisions. All child processes, DB servers
and TLS files are torn down, including startup failure. Sanitized evidence lives
under ignored out/m32-acceptance and order-cell/target/m32-evidence; no raw private
keys, audit credentials or event payloads belong in source control.
