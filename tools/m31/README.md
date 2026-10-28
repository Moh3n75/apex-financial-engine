# Java-first gRPC requests and queries (M31)

Use Java 21 and the repository's Maven 3.9.x launcher. Business applications write
records and methods; only `apex-platform-grpc/src/main/proto/cell_gateway.proto`
is compiled to protobuf/gRPC classes. No business `.proto` or stub import is needed.

## Repeat the isolated acceptance

```powershell
$env:JAVA_HOME = '<Java 21 JDK>'
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File tools/m31/verify-gateways.ps1 -Offline -IncludeM30Regression `
  -MavenCommand mvn.cmd
```

Without `-IncludeM30Regression`, this runs the new Platform tests, builds the
independent sample and runs all Order tests plus the packaged two-process test.
The switch also runs the unchanged M30 workflow: isolated Financial generation,
M22/consumer/DLT/replay, three-Cell recovery and bounded performance fixtures.
Offline requires cached dependencies, protobuf/protoc binaries and the native
Zonky PostgreSQL distribution. Omit `-Offline` only for authorized Maven downloads.
No shared DB, broker, topic, group, offsets or deployment configuration is used.
The older standalone runners build their original Platform module list; install
the new gRPC starters/contracts first (the M31 runner does this) before independently
building the now gRPC-capable Order Cell. Their business assertions remain unchanged.

The packaged fixture owns PostgreSQL (fsync/synchronous_commit/full_page_writes on),
Kafka/KRaft (explicit topics, auto-creation off), an Order JVM and independent
interactive client JVMs. Extra clients test identity isolation and unavailability.
A test-only byte tunnel drops replies for crash/retry testing; production uses
Boot's real Netty HTTP/2 gRPC transport directly. Every child is stopped in finally.
Evidence is copied to ignored `out/m31-acceptance` before the old workflow rebuilds
Order. Test XML remains there; routine logs/target output must not be committed.

## Public Java API

Put intentionally shared DTOs in a versioned contracts-only artifact depending
on `apex-platform-grpc-api` (JDK-only). Do not depend on another Cell's executable JAR.
Supported DTOs are public, nonrecursive records with concrete record/list components,
String, UUID, Instant, BigDecimal, boolean/int/long, their wrappers or enums. No Object,
Map, Java class names on wire, default typing or runtime class loading from input.

```java
@CellRequest(type = "CREATE_ORDER", version = 1, response = CreateOrderResponse.class)
public record CreateOrderRequest(
        String externalReference, String description, List<OrderItem> items) {}
public record CreateOrderResponse(UUID orderId, String status) {}

@CellQuery(type = "GET_ORDER", version = 1, response = OrderView.class)
public record GetOrderQuery(UUID orderId) {}
public record OrderView(UUID orderId, String status) {}
```

This example follows the current Order domain: it has externalReference,
description and product/quantity items; it has no customerId, price or currency.
M31 does not invent those domain fields. Supplied `apex-order-contracts` validates
lengths, item quantities and duplicate products too.

```java
var options = RequestOptions.withIdempotencyKey("caller-operation-123");
var created = requests.request("order-cell", request, CreateOrderResponse.class, options);
CompletionStage<CreateOrderResponse> pending = requests.requestAsync(
        "order-cell", request, CreateOrderResponse.class, options);
var current = queries.query("order-cell", new GetOrderQuery(created.orderId()), OrderView.class);
var local = queries.queryLocal(new GetOrderQuery(created.orderId()),
        OrderView.class, QueryOptions.defaults());
```

Keep the options/requestId/idempotencyKey for retry after an unknown outcome.
`withRequestId(...)` also supports an explicitly new attempt with the same key.
There is no automatic command retry. A deadline, cancellation, unavailable channel
or lost response means **outcome unknown**, never business rejection. Query timeouts
do not mean mutation occurred. Use a finite deadline (default 5s, permitted 1ms..30s,
also constrained by `apex.grpc.max-deadline`). Native gRPC cancellation is propagated.
Remote invocation inside an active Spring transaction is rejected, to avoid hidden
network calls on business/Kafka transaction threads. Call asynchronously outside it.

Register ordinary Spring singleton methods:

```java
@CellRequestHandler(type = "CREATE_ORDER", version = 1)
public CreateOrderResponse create(CreateOrderRequest request, RequestContext context) {
    // Existing owning Cell use case; see OrderRpcHandlers for the real example.
}

@CellQueryHandler(type = "GET_ORDER", version = 1)
@Transactional(readOnly = true, timeout = 5)
public OrderView get(GetOrderQuery query) { /* owning repository only */ }
```

Methods must be public, return the declared concrete record, take its annotated
payload and optionally RequestContext, and remain invocable through Spring proxies.
Duplicates, invalid signatures/types and missing write executor fail startup.
Null/wrong response is a runtime failure, serialized before commit for writes.
Queries must be side-effect-free; readOnly advice is the owner's responsibility.

## Explicit routing and opt-in

Server starter and client starter are independent dependency aggregators.
`apex.grpc.server.enabled` and `apex.grpc.client.enabled` default false. Existing
starters gain neither dependency; Order only activates its new handlers with
the explicit `grpc` profile. There is no Kafka request/reply or event-handler change.

Client configuration must explicitly map a logical Cell to a named Boot channel:

```yaml
apex:
  grpc:
    client.enabled: true
    cell-id: example-client
    destinations:
      order-cell:
        channel: order
spring:
  grpc:
    client:
      channel:
        order:
          target: static://order.internal:9091
          ssl:
            enabled: true
            bundle: internal-client
```

Configure the referenced Boot SSL bundle with operator-managed trust/client keys.
Never commit private material. Targets require explicit host:port, no resolver
guessing, no certificate-validation bypass. Unknown destination -> NOT_FOUND.
For a local named query route use `destinations.order-cell.local=true` explicitly;
local write requests are rejected. There is no implicit central service registry.

Production server startup requires `spring.grpc.server.ssl.bundle`,
`spring.grpc.server.ssl.client-auth=require` and a trusted `PeerIdentityVerifier`
implementation mapping the verified TLS peer/authentication to caller Cell,
tenant and principal. The SPI is a secure delegation point, not a supplied RBAC
product. The platform checks TLS session presence and agreement with the origin
header. Arbitrary tenant/role headers are never trusted as authentication.
Integration-tested here: fail-closed missing-auth policy. Live mTLS/identity/ACL
rollout is a separate operator acceptance; no production security certification.

For **owned disposable loopback tests only**, the server/client may explicitly set
`apex.grpc.insecure-loopback-test-only=true`, false Boot channel SSL, and server
`spring.grpc.server.address=127.0.0.1` plus explicit `apex.grpc.test-clients`.
The caller identity in that mode is an **unverified test claim**, not authentication.
Do not enable it on deployment networks. The acceptance runner supplies it only
to its child processes, never edits base/local application.yml.

## Owning transaction and migration

Generic Platform requires an owning `TransactionalRequestExecutor` for writes.
Order's `JdbcOrderRequestExecutor` requires the same JdbcTemplate DataSource and
JDBC transaction manager, owns the outer transaction, atomically claims the key,
calls the existing PlaceOrderUseCase, and stores the serialized result in that
transaction. PostgreSQL uniqueness serializes concurrent initial requests. No
JVM cache, independent receipt DB or before-commit positive reply is used.

Order migration `V2002__order_request_receipts.sql` follows existing V2001 and
Platform V1001/V1002. Existing migrations are not edited. Apply it through ordinary
owning Flyway startup; do not enable outOfOrder/reset history. The migration is
additive and applies even when gRPC is disabled; downgrade leaves the unused table.

The owner's key namespace is deliberately stricter than a per-client namespace:
reusing an idempotency key with different client/tenant/principal/contract/payload
is ALREADY_EXISTS. Identical payload/client/key returns the persisted initial
orderId/PENDING_INVENTORY, even if the asynchronous Saga later advances. Query to
read current status. New requestId with the same key is allowed; reusing the original
client/requestId with another key conflicts. Receipt retention is indefinite here;
do not delete receipts while clients may retry. Future retention needs an explicit
business/retry horizon and upgrade policy.

Error descriptions contain safe codes only: invalid input/business validation ->
INVALID_ARGUMENT; unknown type/version -> UNIMPLEMENTED; changed idempotency identity
-> ALREADY_EXISTS; missing Order/route -> NOT_FOUND; auth -> UNAUTHENTICATED;
missing strategy/transaction misuse -> FAILED_PRECONDITION; size -> RESOURCE_EXHAUSTED;
transport -> UNAVAILABLE/DEADLINE_EXCEEDED/CANCELLED; unexpected/commit failure -> INTERNAL.
Use GatewayException for deliberate business classifications; never leak DB messages.

Correlation and validated request IDs cross gRPC metadata and the internal envelope.
An owning handler's ExecutionContext retains incoming correlation and uses the
requestId as causation for newly appended events. RequestContext also exposes the
incoming causation and verified caller identity. Scope close restores/clears the
existing APEX Provider; there is no second business context implementation.

Limits: default JSON payload 64KiB (configurable 128B..1MiB), JSON nesting16,
contract record depth8/nonrecursive, <=32 metadata values/8KiB native headers,
<=32 destinations, native receive cap payload+8KiB; server workers8+queue32,
local-query workers4+queue32, one deadline scheduler. Boot manages channel/server
lifecycle; retries disabled on channels. Request/response metrics and real-world
load, TLS rotation, authenticated authorization and failover are not certified.

## Independent manual sample (operator-owned infrastructure only)

After installing Platform/Starters/contracts and packaging both applications,
start Order with its own separately provisioned DB/Kafka and explicit configuration.
Existing environment variables for datasource/Kafka are described by Order README;
M31 adds `--spring.profiles.active=grpc` and independently configurable
`CELL_GRPC_ADDRESS` / `CELL_GRPC_PORT` (loopback /9091). Supply secure server settings
and verifier as above. This guide does not start/provision shared resources.

Start the client with explicit routing, TLS bundle and identity:

```powershell
java -jar sample-grpc-client/target/sample-grpc-client.jar `
  --spring.main.web-application-type=none `
  --apex.grpc.client.enabled=true --apex.grpc.cell-id=example-client `
  --apex.grpc.destinations.order-cell.channel=order `
  --spring.grpc.client.channel.order.target=static://order.internal:9091 `
  --spring.grpc.client.channel.order.ssl.enabled=true `
  --spring.grpc.client.channel.order.ssl.bundle=internal-client
```

After `M31_CLIENT_READY`, send a JSON line on stdin (IDs below are examples;
use fresh product/operation IDs in your own disposable DB):

```json
{"action":"create","key":"demo-unique-1","requestId":"571ee7b2-7f4b-43d5-9ffc-9236f429a01e","externalReference":"demo-unique-1","productId":"11111111-1111-4111-8111-111111111111","quantity":1}
```

`M31_REPLY` returns the committed orderId. Then send
`{"action":"query","orderId":"<returned UUID>"}`. `async` exercises CompletionStage;
`EXIT` closes the client. The sample's stdin/stdout JSON is an operator interface;
its cross-process transport is the Platform-generated unary protobuf gRPC service.

P1 scatter/gather is intentionally not implemented: a future collector must take
an explicit finite expected-responder list, per-target request/query identities and
deadline, return partial/status entries, and never treat timeout as negative business
fact or create automatic Kafka responders. The P0 logical-target unary protocol
does not prevent a caller-side orchestration facade with those explicit semantics.
