import { Badge, freshnessTone } from "@/components/ui/badge";
import { EmptyState, Panel } from "@/components/panel";
import { MetaRow } from "@/components/meta-row";
import { formatUtc } from "@/lib/format";
import type { Topology } from "@/lib/topology";

export function TopologyView({ graph }: { graph: Topology }) {
  const names = new Map(graph.nodes.map(n => [n.id, n.name]));
  return <Panel title="Service topology and contract evidence">
    <MetaRow asOf={graph.asOf} coverage={graph.coverage} uncertainty="PARTIAL_INVENTORY" />
    <dl className="my-3 grid gap-2 text-xs sm:grid-cols-2">
      {Object.entries(graph.capabilities).map(([name, value]) => <div key={name}><dt className="font-medium">{name}</dt><dd>{value}</dd></div>)}
    </dl>
    {graph.providers.length ? <ul className="mb-3 space-y-1 text-xs" aria-label="Provider coverage">
      {graph.providers.map(p => <li key={p.name}>{p.name}: <Badge tone="neutral">{p.status}</Badge> · {p.sourceMode} · {p.coverage} · collected UTC {formatUtc(p.collectedAt)}</li>)}
    </ul> : <p className="my-3 text-sm">Legacy provider coverage: {graph.capabilities.declared}</p>}
    {graph.nodes.length === 0 ? <EmptyState>No visible configured owners. This is not a complete runtime inventory.</EmptyState> : null}
    <div className="space-y-3">
      {graph.memberships.length>0?<div className="space-y-2" aria-label="Runtime membership">
        {graph.memberships.map(m=><p key={m.id} className="text-xs">{m.service} · {m.instanceId} · <Badge tone={m.membershipState==="DUPLICATE"?"danger":"neutral"}>{m.membershipState}</Badge>
          {" · "}{m.freshness} · {m.discoverySource} / {m.registryIdentity} · registry ID {m.registryId} · epoch {m.epoch} · {m.coverage} · {m.transportStatus}</p>)}
      </div>:null}
      {graph.nodes.map(node => <details key={node.id} className="rounded border border-zinc-200 p-3 dark:border-zinc-700">
        <summary className="cursor-pointer text-sm font-medium">{node.name} · {node.environment} · {node.kind}{" "}
          {Array.from(new Set(node.evidence.map(e => e.kind))).map(kind => <Badge key={kind} tone="neutral">{kind === "OBSERVED" && node.kind === "INSTANCE" ? "OBSERVED INSTANCE" : kind}</Badge>)}
          {" "}<Badge tone={freshnessTone(node.freshness)}>{node.freshness}</Badge>
        </summary>
        <div className="mt-3 space-y-2 text-sm">
          <p>Health: {node.health} · {node.uncertainty}</p>
          {node.kind==="SERVICE"&&graph.memberships.some(m=>m.service===node.name&&m.environment===node.environment)?<p>
            Current verified instances: {graph.memberships.filter(m=>m.service===node.name&&m.environment===node.environment&&["VERIFIED","DUPLICATE"].includes(m.membershipState)).length}
            {" · "}duplicate members: {graph.memberships.filter(m=>m.service===node.name&&m.environment===node.environment&&m.membershipState==="DUPLICATE").length}
          </p>:null}
          <p className="text-xs">Epoch: {node.epoch ?? "unknown"} · last seen UTC: {formatUtc(node.lastSeen)}</p>
          {node.evidence.map((e, i) => <p key={i} className="text-xs">Evidence: {e.kind} · {e.source} · {e.reference} · collected UTC {formatUtc(e.collectedAt)}</p>)}
          {graph.connections.filter(e => e.source === node.id || e.target === node.id).map(edge => <p key={edge.id}>
            {names.get(edge.source)} → {names.get(edge.target)} · {edge.protocol} · {edge.contract} · {Array.from(new Set(edge.evidence.map(e => e.kind))).join(", ")}
            {" · "}traffic last seen: {edge.lastSeen ? formatUtc(edge.lastSeen) : "unknown"}
          </p>)}
          {graph.descriptors.filter(d => d.serviceId === node.id).map((d, i) => <p key={i}>{d.protocol} {d.kind}: {d.contract} · registered capability</p>)}
          {graph.unresolved.filter(u => u.serviceId === node.id).map((u, i) => <p key={i} className="text-amber-800 dark:text-amber-300">Unresolved {u.contract}: {u.reason}</p>)}
        </div>
      </details>)}
    </div>
  </Panel>;
}
