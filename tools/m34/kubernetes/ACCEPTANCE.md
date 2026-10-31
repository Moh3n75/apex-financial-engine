# Future owned-cluster acceptance runbook — NOT EXECUTED here

This is a repeatable operator procedure for an **already available, explicitly authorized disposable
kind/k3d context**. It neither installs/starts a container runtime nor chooses a current/shared
context implicitly. The checked-in native VM verifier is the executed acceptance; this procedure
does not establish Kubernetes PASS until its observations are recorded.

## Preconditions and explicit context

Use Java 21/Maven 3.9.x and the dependency/build sequence in `../README.md`. Build Cells/sample with
`-Pdiscovery-kubernetes`; Gateway with `-Pkubernetes`; build/load the five corresponding images
using `../Dockerfile`. Use the operator's existing kind `load docker-image --name <owned-cluster>`
or k3d image-import workflow; never load images into an unknown context. Keep the native Eureka
JARs/evidence separate. The Kubernetes artifact graph must contain no Eureka client.

Explicitly provision three **separate owner DBs** and the twelve three-partition topics in
`DualDiscoveryReplicaIT.SOURCES` plus their `-dlt` topics, with auto-creation disabled. Prepare each
owner's `<cell>-runtime`, `<cell>-mtls` and `<cell>-configuration` resources as documented in
`../README.md`. Replicas of an owner use that owner's single DB. Do not reuse development credentials.
Issue a CA-trusted server certificate with `order-cell.apex-m34.svc.cluster.local` DNS SAN, and
analogous Inventory/Payment identities; the client cert's URI SAN must match its configured
`operator-client` grant. No validation bypass. Grant only the explicitly tested contracts.

Start a PowerShell terminal:

```powershell
$M34Context = '<explicit-owned-kind-or-k3d-context>'
if ($M34Context -like '<*') { throw 'Choose the owned disposable context explicitly.' }
function K { & kubectl --context $M34Context --namespace apex-m34 @args; if ($LASTEXITCODE -ne 0) { throw 'kubectl failed' } }
K cluster-info
New-Item -ItemType Directory out/m34-kubernetes-evidence -Force | Out-Null
# All writes below are restricted to the explicitly selected owned context/namespace.
K apply -f tools/m34/kubernetes/rbac.yaml
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/m34/kubernetes/render-workloads.ps1
K apply -f out/m34-kubernetes.json
K rollout status deployment/order-cell --timeout=180s
K rollout status deployment/inventory-cell --timeout=180s
K rollout status deployment/payment-cell --timeout=180s
K get pods -o wide
K get endpointslices -l kubernetes.io/service-name=order-cell -o yaml
K auth can-i list endpointslices.discovery.k8s.io --as=system:serviceaccount:apex-m34:apex-discovery-reader
kubectl --context $M34Context -n apex-m34 auth can-i list secrets --as=system:serviceaccount:apex-m34:apex-discovery-reader # expect no
```

The negative `can-i` command intentionally returns nonzero: run it directly with the explicit
context rather than the fail-fast `K` helper, and record `no`. Service reads are name-scoped;
EndpointSlice LIST cannot be label-restricted by Kubernetes RBAC. No Pod/Secret API read is needed.

## Edge HTTP and actual Pod selection

Create `apex-gateway-configuration` externally from reviewed `gateway.properties.example` with key
`application.properties`; apply `gateway.yaml`. It has no Service/Ingress and binds loopback only.
In a separate terminal, keep the explicitly scoped port-forward running:

```powershell
K apply -f tools/m34/kubernetes/gateway.yaml
K rollout status deployment/apex-api-gateway --timeout=180s
kubectl --context $M34Context -n apex-m34 port-forward deployment/apex-api-gateway 8080:8080 --address=127.0.0.1
```

Seed stock/payment decisions only using three explicit owning-DB credentials and the existing
isolated-demo SQL conventions; no seed HTTP API is introduced. Submit the existing placement JSON:

```powershell
$productId = '<fresh UUID seeded in Inventory only>'
$body = @{externalReference="m34-k8s-$([guid]::NewGuid())";items=@(@{productId=$productId;quantity=1})} | ConvertTo-Json -Depth 4
$created = Invoke-RestMethod http://127.0.0.1:8080/api/v1/order-placements -Method Post -ContentType application/json -Body $body
$orderId = $created.orderId
1..30 | ForEach-Object { Invoke-RestMethod "http://127.0.0.1:8080/api/v1/order-placements/$orderId" }
```

Use reviewed operator-only Tomcat access logging (as in the native fixture), with unique request
IDs, to associate these GETs with **multiple distinct Pod UIDs**. Do not infer distribution from
30 successful responses or from a ClusterIP. Save owning SQL for the resulting Order, reservation,
payment and Inbox/Outbox; test happy, explicit decline/release and insufficient-stock outcomes.

## One reused mTLS/HTTP2 client and per-RPC backend evidence

Create `m34-operator-client-configuration` from `client.properties.example`, and a separately
issued `m34-operator-client-mtls` Secret. On Order explicitly authorize this peer for
`QUERY:GET_ORDER:1` and, for receipt tests only, `REQUEST:CREATE_ORDER:1`.
Enable `logging.level.com.apex.platform.grpc.internal.RpcDispatcher=DEBUG` only in the disposable
Order configuration to capture safe request IDs, not payloads. Apply `operator-client.yaml`.

```powershell
K apply -f tools/m34/kubernetes/operator-client.yaml
K wait pod/m34-operator-client --for=condition=Ready --timeout=120s
$requests = @(1..60 | ForEach-Object {
  @{action='query';orderId="$orderId";requestId=[guid]::NewGuid().ToString();deadlineMs=3000} | ConvertTo-Json -Compress
})
$requests | kubectl --context $M34Context -n apex-m34 exec -i m34-operator-client -- java -jar /app/app.jar |
  Tee-Object out/m34-kubernetes-evidence/client.txt
K logs -l app=order-cell --prefix --tail=-1 > out/m34-kubernetes-evidence/order-logs.txt
```

This single Java process handles all 60 lines and reuses its channel. Require all expected reply
slots, `contextCleared=true`, actual mTLS/HTTP2, and request IDs observed on >=2 Order Pod UIDs.
Save Pod UIDs/IPs, Service/EndpointSlices, certificate public identities and artifact hashes.
Do not print private keys/tokens or treat client text alone as proof of authorization.

## Scale, removal, receipt and fail-closed checks

Run the following in another explicitly scoped terminal while the interactive client remains
running (omit pipeline EOF; type JSON lines on stdin). Query throughout; writes are never retried
automatically. Bound each observation and record errors rather than assuming every in-flight call
succeeds.

```powershell
K scale deployment/order-cell --replicas=2
K rollout status deployment/order-cell --timeout=180s
K scale deployment/order-cell --replicas=3
K rollout status deployment/order-cell --timeout=180s
$victim = ((K get pods -l app=order-cell -o json | ConvertFrom-Json).items | Select-Object -First 1).metadata.name
K delete pod $victim --wait=true
K rollout status deployment/order-cell --timeout=180s
K get endpointslices -l kubernetes.io/service-name=order-cell -o yaml
```

Require added ready Pod addresses to receive RPCs and deleted/terminating/unready addresses to
leave the resolver. Retry a deliberate create with identical `key`, `externalReference`, `productId`,
`quantity` after a receiver restart: returned orderId and owning SQL must prove **one Order, one
request receipt and one ORDER_PLACED**. Reuse the key with a changed body and require ALREADY_EXISTS.
An actual lost-reply-after-commit proof needs controlled connection interruption at the receiver
(the native fixture already provides that proof); Pod deletion alone does not prove that window.

Use independent Kafka Admin clients to record group membership/assignments, end/committed offsets,
DLT counts and eventual drain. Kill one Inventory/Payment Pod and verify same stable group and DB
recovery. Require two publishers' non-overlapping same-row claims and stock/business conservation;
do not claim exact-once Kafka delivery. Verify no Eureka beans/client registrations/network attempts.

For negative cases use only disposable resources: wrong Service authority, missing named gRPC
port, unready/terminating EndpointSlice, unavailable scoped API past TTL, wrong server DNS SAN,
untrusted/missing/unauthorized client cert and forged caller. Failures must precede business effects;
stale cache must not permit new writes. Record each status and deadline. Never grant broad RBAC or
disable hostname validation to make a test pass. Namespace/tokens/cert rotation and full seven-Pod
Saga remain NOT VERIFIED until independently executed and archived.

Stop owned port-forwards/client processes, remove only explicitly owned fixtures using exact
resource names, and shut down only the operator's disposable cluster/DB/Kafka lifecycle. This
runbook deliberately issues no cluster deletion, offset reset or shared topic command.
