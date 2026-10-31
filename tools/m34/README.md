# M34: three independent routing planes

Business code continues to call `queryGateway.query("order-cell", query, OrderView.class)` and
`requestReplyGateway.request("order-cell", request, Response.class, options)`.
Discovery is deployment configuration, not a business contract. No business protobuf or Kafka
request/reply is introduced. Kafka remains the durable Saga/Inbox/Outbox transport.

## Owned native Windows acceptance

Use Java 21 and Maven 3.9.x. The first build needs the Cloud 2025.1.3 dependencies in the Maven
cache; add `-Offline` only after resolving them. From the repository root:

```powershell
$env:JAVA_HOME = '<Java 21 JDK>'
$env:Path = "$env:JAVA_HOME/bin;$env:Path"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/m34/verify-m34.ps1 -MavenCommand '<mvn.cmd>'
```

`-Phase Build` runs actual library/Cell tests, builds provider-specific JARs, creates the legacy
Saga freshness receipt after verified Order/Platform builds, then copies six executable artifacts
to ignored `out/m34-artifacts`. Inputs must remain unchanged during the build. `-Phase VM` checks
that receipt and runs the packaged Eureka/Gateway/client and same-Cell replica fixture.
`All` also runs the existing full M32 wrapper. An old regression failure is a failure, never silently
skipped; separate VM execution permits collecting independent M34 evidence when that gate is blocked.
`-ContinueAfterKnownDeadlineFailure` permits additive assembly only when the sole failed legacy case
is exactly `CollectorDeadlineIntegrationTest.repeatedNativeDeadlinesNeverBlockTheSharedDeadlineScheduler`,
legacy gRPC production inputs are unchanged and previously installed M31 artifacts are available.
The failed XML is archived; no test is disabled and this option never qualifies global regression PASS.
No test uses inherited development Kafka/DB settings. The fixture starts three durable native
PostgreSQL servers, one disposable three-partition KRaft broker and explicitly creates twelve topics.
It owns and stops every child process. Evidence: `order-cell/target/m34-evidence` and
`order-cell/target/m34-reports`; copied acceptance evidence under `out/m34-acceptance` on success.

## VM/Eureka configuration

Build Cells/sample with Maven `-Pdiscovery-eureka`; Gateway with `-Peureka`. This adds only that
provider. Runtime requires `spring.profiles.active=eureka` (plus existing Saga/gRPC profiles),
`apex.discovery.provider=eureka`, and an explicit `eureka.client.service-url.defaultZone`.
Registration is **off** unless `apex.discovery.registration.enabled=true`; fetch-only clients also
disable Spring Cloud auto-registration and do not require an HTTP server. Startup waits at most
`apex.discovery.startup-wait` (default35 s, allowed250 ms..60 s) for a confirmed Netflix cache refresh;
its constructor's initial fetch has no usable refresh clock. Registered instances default their
readiness group to all health indicators, including DB and registry freshness, unless explicitly
configured by the deployment. Liveness stays separate.
Registering servers require matching `spring.application.name` / `apex.cell.id`, a unique
`apex.discovery.instance-id`, `apex.discovery.authority`, explicit HTTP and gRPC ports and a reachable
`eureka.instance.hostname`. `apex.discovery.grpc-advertised-port` supports explicit NAT port mapping.
Never advertise the HTTP port as the gRPC port. Same-Cell replicas share only their owning database.

Clients configure an allowlist, e.g. `apex.discovery.targets.order-cell.authority=orders.example`,
`apex.discovery.grpc.enabled=true`, the existing destination/channel mapping and Boot target
`spring.grpc.client.channel.order.target=dns:///orders.example:443`. The custom per-channel resolver
supplies actual replica ports; this URI does **not** mean connecting to port 443. One reused channel
uses grpc-java `round_robin`; retries are disabled. The authority is pinned configuration and must
match the server certificate DNS SAN even when connecting to an IP. mTLS peer URI/contract ACL remain
the existing M32 policy. Never use `bypass-certificate-validation` or disable hostname verification.
Missing/expired registry data fails new calls closed; in-flight outcomes may be unknown.

The registry and Gateway are deliberately restricted to explicit loopback development mode:
`apex.registry.loopback-development=true` / `apex.gateway.mode=loopback-development`,
`server.address=127.0.0.1`. Their default startup refuses exposure. This is **not** OAuth/JWT security.
Only the Order placement HTTP route is exposed. HTTP LoadBalancer uses the HTTP port; gRPC goes
directly to replicas. Header filtering strips spoofable forwarding/security headers; a correlation
UUID is tracing data, never identity. There is no automatic write retry at either plane.

## Kubernetes deployment (requires an operator-owned disposable cluster)

The standalone [owned-cluster acceptance procedure](kubernetes/ACCEPTANCE.md) contains explicit
context selection, Pod-scale/removal, reused-channel observations and owning receipt checks.
It has not been executed on a real cluster in this environment.

The Kubernetes Maven profile is `discovery-kubernetes` for Cells/sample and `kubernetes` for Gateway.
It contains Fabric8 and no Eureka dependency. The adapter implements Spring Cloud `DiscoveryClient`
using actual namespace-scoped Service GET / EndpointSlice LIST. The resolved stock Spring Cloud
Fabric8 discovery implementation still enumerates legacy Endpoints, so it is not used for this
Pod-address path. No watch, all-namespace scan, Secret read or remote contract registry is needed.

1. Build those provider-specific JARs with normal tests, then build images using the Dockerfile here:
   `docker build -f tools/m34/Dockerfile --build-arg JAR=order-cell/target/order-cell.jar -t apex/order-cell:m34-kubernetes .`
   Repeat for Inventory/Payment. Load images into the owned kind/k3d cluster.
2. Explicitly provision three separate PostgreSQL databases and twelve three-partition Kafka topics
   listed in `DualDiscoveryReplicaIT.SOURCES` plus each `-dlt`. Supply each owner's required DB/Kafka
   variables in a separately created `<cell>-runtime` Secret. Payment simulation policy is seeded
   only by authorized disposable-fixture SQL; absent policy never means approval.
3. Create `<cell>-mtls` Secrets externally with `tls.crt`, `tls.key`, `ca.crt`. Each server certificate
   requires its own `<cell>.apex-m34.svc.cluster.local` DNS SAN and the expected workload identity.
   Create each `<cell>-configuration` ConfigMap from a reviewed owner-specific copy of
   `cell.properties.example`. Mount no other Cell's credentials. Enable Actuator in the selected
   application artifact before using these readiness/liveness probes (provided transitively by the
   Kubernetes adapter profile in this implementation).
4. Review/apply `rbac.yaml` only to the owned cluster. Render workloads without network access:

   ```powershell
   powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/m34/kubernetes/render-workloads.ps1
   kubectl apply --dry-run=client --validate=false -f out/m34-kubernetes.json
   # Only after selecting/authorizing the owned cluster:
   kubectl apply -f out/m34-kubernetes.json
   ```

   Even client dry-run requires API discovery; it is not an offline schema validator.

5. A discovery-aware client uses `client.properties.example` with its own mounted mTLS Secret and
   ServiceAccount `apex-discovery-reader`. Server-only Pods need no API token or Eureka registration:
   Service selectors and ready EndpointSlices represent them. Use `kubectl run -i`/an explicit client
   workload to send Java sample commands; never put private keys or raw payloads into manifests.
   For the HTTP Gateway, build `apex-api-gateway` with `-Pkubernetes` and its own image using the
   Dockerfile above. Review `gateway.properties.example`, create `apex-gateway-configuration`
   from that file as `application.properties`, then review/apply `gateway.yaml` to the owned cluster.
   Its exec probes use the image's curl against loopback; no Service/Ingress exposes the listener.
   Access it only through `kubectl -n apex-m34 port-forward deployment/apex-api-gateway 8080:8080 --address=127.0.0.1`.
   These deployment examples require real cluster validation; static parsing is insufficient.
6. Inspect actual EndpointSlices, then scale Order 2→3→2 and delete one Pod; independently verify
   resolver output, per-RPC backend selection, owner receipt and group offsets. A ClusterIP or
   successful Service DNS lookup alone is not proof of client-side balancing.

The adapter selects ready, serving, non-terminating **IPv4 Pod** endpoints with named `http`/`grpc`
TCP ports. Refresh defaults to 1 s; a failed reconciliation does not extend cache age (default15 s,
maximum2 min). API requests are bounded at2 s. Namespace Role permits service GET on three names
and EndpointSlice LIST in one namespace; Kubernetes RBAC cannot label-restrict LIST, so application
selection and service identity checks complement namespace scoping. Mount-token rotation is not yet
qualified (token read at startup); IPv6-only clusters and PKI rotation are not verified.

## Docker/Compose

`compose.yaml` is an explicit **Linux host-network, loopback-only** example for the registry and
Gateway plus 3/2/2 Cells. It requires operator-supplied external runtime configuration, certificates,
three owning DBs and pre-provisioned Kafka; it never provisions shared infrastructure. Windows
Docker Desktop host networking must be explicitly supported/enabled to use it. The native Windows
fixture above is the repeatable complete disposable arrangement and does not require Docker.

## Correctness boundaries / rollback

HTTP edge routing, direct gRPC round-robin and Kafka partition assignment are independent. More
replicas do not imply global Kafka ordering or exactly-once publication. PostgreSQL leases/claims,
Inbox and business keys retain durable effects; Kafka ACK and Outbox mark remain non-atomic.
After a lost mutating reply, explicitly retry the same supported receipt/key; do not change the key.
Disable discovery by removing the provider Maven/runtime profile and restore existing explicitly
configured static gRPC routes. No schema, wire contract, Kafka topic/group or migration changes are
needed. Run only against owned resources; never reset offsets or silently create topics.
