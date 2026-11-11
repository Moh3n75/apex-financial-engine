import { Badge, freshnessTone } from "@/components/ui/badge";
import { formatUtc } from "@/lib/format";
import type { Topology } from "@/lib/topology";
import type { TopologyNode, TopologyConnection } from "@/lib/topology-layout";

export function NodeDetails({ node, graph }: { node: TopologyNode; graph: Topology }) {
  const ownerId = node.kind === "CELL" && node.serviceId ? node.serviceId : node.id;
  const eventPaths = graph.connections.flatMap(e => e.details?.kafka ?? []).filter(p => p.publisherServiceId === ownerId || p.consumerServiceId === ownerId || p.topicId === node.id || p.consumerGroupId === node.id);
  const contracts = [...new Set(eventPaths.map(p => `${p.eventType}:${p.eventVersion}`))];
  return <div className="space-y-3 text-sm break-words">
    <p>{node.environment} · {node.kind} · <Badge tone={freshnessTone(node.freshness)}>{node.freshness}</Badge></p>
    <p>Health: {node.health} · {node.uncertainty}</p>
    {node.kind === "INSTANCE" ? <>
      <p>Service: {graph.nodes.find(n => n.id === node.serviceId)?.name ?? "unknown"}</p>
      <p>Epoch: {node.epoch ?? "not provided (registry record only)"}</p>
      {!node.epoch ? <p>Opaque registry alias. This is not verified membership, a logical instance identity, or restart/duplicate evidence.</p> : null}
    </> : null}
    <p className="text-xs">last seen UTC: {formatUtc(node.lastSeen)}</p>
    {node.evidence.map((e, i) => <p key={i} className="text-xs">Evidence: {e.kind} · {e.source} · {e.reference} · collected UTC {formatUtc(e.collectedAt)}</p>)}
    {graph.memberships.filter(m => m.id === node.id || (node.kind === "SERVICE" && m.service === node.name && m.environment === node.environment)).map(m =>
      <p key={m.id} className="text-xs">{m.instanceId} · <Badge tone={m.membershipState === "DUPLICATE" ? "danger" : "neutral"}>{m.membershipState}</Badge>
        {" · "}{m.freshness} · {m.discoverySource} / {m.registryIdentity} · registry ID {m.registryId} · epoch {m.epoch} · {m.coverage} · {m.transportStatus}</p>)}
    {contracts.length ? <div><h4 className="font-medium">Kafka Event contracts</h4><p className="text-xs">{contracts.join(", ")} · declarations, not processing evidence</p></div> : null}
    {graph.descriptors.filter(d => d.serviceId === ownerId).map((d, i) => <div key={i}><p>{d.protocol} {d.kind}: {d.contract} · registered capability</p>
      {d.kind.startsWith("CALLER_") ? d.evidence.map((e, index) => <p key={index} className="text-xs">{e.reference}</p>) : null}</div>)}
    {graph.unresolved.filter(u => u.serviceId === ownerId).map((u, i) => <p key={i} className="text-amber-800 dark:text-amber-300">Unresolved {u.contract}: {u.reason}</p>)}
  </div>;
}

export function ConnectionDetails({ connection, graph }: { connection: TopologyConnection; graph: Topology }) {
  const name = (id: string) => graph.nodes.find(n => n.id === id)?.name ?? id;
  const providers = graph.providers.filter(p =>
    (p.name === "gateway-routes" && connection.evidence.some(e => e.source === "SPRING_CLOUD_GATEWAY"))
    || ([`contracts:${connection.environment}:${name(connection.source)}`, `contracts:${connection.environment}:${name(connection.target)}`].includes(p.name)
      && connection.evidence.some(e => e.source === "CONTRACT_DISCOVERY")));
  return <div className="space-y-3 text-sm break-words">
    <dl className="space-y-2">
      <div><dt className="font-medium">Source</dt><dd>{name(connection.source)}</dd></div>
      <div><dt className="font-medium">Target</dt><dd>{name(connection.target)}</dd></div>
      <div><dt className="font-medium">Protocol</dt><dd>{connection.protocol}</dd></div>
      <div><dt className="font-medium">Contract / registered route</dt><dd>{connection.contract}</dd></div>
      <div><dt className="font-medium">Environment / scope</dt><dd>{connection.environment} · {connection.scope}</dd></div>
      <div><dt className="font-medium">Status</dt><dd>{Array.from(new Set(connection.evidence.map(e => e.kind))).join(" + ")} · Health: {connection.health}</dd></div>
      {providers.length ? <div><dt className="font-medium">Provider coverage</dt><dd>{providers.map(p => <p key={p.name}>{p.name}: {p.status} · {p.coverage}</p>)}</dd></div> : null}
    </dl>
    {connection.details?.http ? <section className="space-y-2"><h4 className="font-medium">Registered HTTP route</h4>
      <p>Route ID: {connection.details.http.routeId}</p><p>Destination service: {connection.details.http.destinationService}</p>
      <p>Predicates: {connection.details.http.predicates.join("; ") || "Unavailable"}</p>
      <p>Filters: {connection.details.http.filters.join("; ") || "None reported / unavailable"}</p>
    </section> : null}
    {connection.details?.grpc ? <section className="space-y-2"><h4 className="font-medium">gRPC {connection.details.grpc.kind}</h4>
      <p>Caller: {name(connection.details.grpc.callerServiceId)}</p><p>Target: {name(connection.details.grpc.targetServiceId)}</p>
      <p>Request / Query contract: {connection.contract}</p><p>Behavior: {connection.details.grpc.behavior}</p>
      {connection.details.grpc.behavior === "REQUEST_RESPONSE" ? <p>Request/Response contract; caller blocking style was not declared.</p> : null}
      <p>Discovery target: {connection.details.grpc.discoveryTarget}</p><p>Configuration status: {connection.details.grpc.availability}</p>
      <p>Matched caller declaration and registered target handler. No invocation is implied.</p>
    </section> : null}
    {connection.details?.kafka.length ? <section className="space-y-3"><h4 className="font-medium">Kafka Event paths</h4>
      {connection.details.kafka.map((match, index) => <div key={index} className="space-y-1 rounded border border-zinc-200 p-2 text-xs dark:border-zinc-700">
        <p className="font-medium">{match.eventType}:{match.eventVersion}</p>
        <p>Publisher Cell: {name(match.publisherServiceId)}</p><p>Kafka topic: {name(match.topicId)}</p>
        <p>Cluster: {match.cluster} · environment: {connection.environment}</p>
        <p>Consumer group: {match.consumerGroupId ? name(match.consumerGroupId) : "Unknown / no visible exact match"}</p>
        <p>Consumer Cell: {match.consumerServiceId ? name(match.consumerServiceId) : "Unknown"}</p>
        <p>Registered handler: {match.handlerContract ?? "No visible matching handler"}</p><p>Match: {match.matchStatus}</p>
      </div>)}
    </section> : null}
    <p>traffic last seen: {connection.lastSeen ? formatUtc(connection.lastSeen) : "unknown"}</p>
    {connection.evidence.every(e => e.kind === "DECLARED") ? <p>A declared capability or route does not prove a request or message was sent.</p> : null}
    {connection.evidence.map((e, i) => <p key={i} className="text-xs">Evidence: {e.kind} · {e.source} · {e.reference} · collected UTC {formatUtc(e.collectedAt)}</p>)}
  </div>;
}
