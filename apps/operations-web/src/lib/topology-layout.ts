import dagre from "@dagrejs/dagre";
import { Position, type Node, type Edge } from "@xyflow/react";
import type { Topology } from "./topology";

export type TopologyNode = Topology["nodes"][number];
export type TopologyConnection = Topology["connections"][number];
export const PROTOCOLS = ["HTTP", "GRPC", "KAFKA"] as const;
export type Protocol = typeof PROTOCOLS[number];
export type FlowNode = Node<{ fact: TopologyNode; instanceCount: number; expanded: boolean; gateway: boolean }, "topology">;
export type FlowEdge = Edge<{ fact: TopologyConnection; facts: TopologyConnection[]; lane: number }, "relationship">;
export const NODE_WIDTH = 244;
export const NODE_HEIGHT = 104;

/** Presentation adapter only: IDs, facts and relationships are the v2 API's originals. */
export function layoutTopology(graph: Topology, protocols: ReadonlySet<Protocol>, expanded: ReadonlySet<string>, options: { focus?: string | null; aggregate?: boolean } = {}) {
  let candidates = graph.connections.filter(e => protocols.has(e.protocol));
  let focusedIds: Set<string> | null = null;
  if (options.focus) {
    const focus = options.focus;
    const related = new Set<string>();
    for (const edge of candidates) {
      if (edge.source === focus || edge.target === focus || edge.details?.kafka.some(p => p.publisherServiceId === focus || p.consumerServiceId === focus)) related.add(edge.id);
    }
    // Legacy v2 declarations lack structured matches: follow only the same verified event contract, at most one full chain.
    const frontier = candidates.filter(e => related.has(e.id) && e.protocol === "KAFKA" && !e.details?.kafka.length)
      .flatMap(e => [{ node: e.source, contract: e.contract }, { node: e.target, contract: e.contract }]);
    for (let depth = 0; depth < 3; depth++) for (const item of [...frontier]) for (const edge of candidates)
      if (edge.protocol === "KAFKA" && edge.contract === item.contract && (edge.source === item.node || edge.target === item.node) && !related.has(edge.id)) {
        related.add(edge.id); frontier.push({ node: edge.source, contract: edge.contract }, { node: edge.target, contract: edge.contract });
      }
    candidates = candidates.filter(e => related.has(e.id));
    focusedIds = new Set([focus, ...candidates.flatMap(e => [e.source, e.target])]);
    graph.nodes.filter(n => n.kind === "INSTANCE" && n.serviceId && focusedIds!.has(n.serviceId)).forEach(n => focusedIds!.add(n.id));
  }
  const visible = graph.nodes.filter(n => {
    if (focusedIds && !focusedIds.has(n.id)) return false;
    if (n.kind === "INSTANCE") return !!n.serviceId && expanded.has(n.serviceId);
    if (n.kind === "KAFKA_TOPIC" || n.kind === "CONSUMER_GROUP") return protocols.has("KAFKA");
    return true; // Keep discovered services visible even when they have no relationship evidence.
  });
  const ids = new Set(visible.map(n => n.id));
  const connections = candidates.filter(e => ids.has(e.source) && ids.has(e.target));
  const layout = new dagre.graphlib.Graph({ multigraph: true }).setDefaultEdgeLabel(() => ({}));
  layout.setGraph({ rankdir: "LR", nodesep: 40, ranksep: 140, marginx: 36, marginy: 60 });
  visible.forEach(n => layout.setNode(n.id, { width: NODE_WIDTH, height: NODE_HEIGHT }));
  connections.forEach(e => layout.setEdge(e.source, e.target, {}, e.id));
  // Parentage influences positioning only. It is never emitted as a connection or protocol edge.
  visible.filter(n => n.kind === "INSTANCE" && n.serviceId && ids.has(n.serviceId))
    .forEach(n => layout.setEdge(n.serviceId!, n.id, {}, `layout-only:${n.id}`));
  dagre.layout(layout);
  const nodes: FlowNode[] = visible.map(fact => {
    const point = layout.node(fact.id);
    return { id: fact.id, type: "topology", position: { x: point.x - NODE_WIDTH / 2, y: point.y - NODE_HEIGHT / 2 },
      width: NODE_WIDTH, height: NODE_HEIGHT, draggable: false, connectable: false,
      ariaLabel: `${fact.kind} ${fact.name}`, ariaRole: "button",
      handles: [{ id: "in", type: "target", position: Position.Left, x: 0, y: NODE_HEIGHT / 2 },
        { id: "out", type: "source", position: Position.Right, x: NODE_WIDTH, y: NODE_HEIGHT / 2 }],
      data: { fact, gateway: graph.connections.some(e => e.source === fact.id && e.protocol === "HTTP" && e.evidence.some(p => p.source === "SPRING_CLOUD_GATEWAY")),
        expanded: expanded.has(fact.id), instanceCount: graph.nodes.filter(n => n.kind === "INSTANCE" && n.serviceId === fact.id).length } };
  });
  const pairs = new Map<string, TopologyConnection[]>();
  connections.forEach(e => { const key = JSON.stringify([e.source, e.target]); pairs.set(key, [...(pairs.get(key) ?? []), e]); });
  const bundles = new Map<string, TopologyConnection[]>();
  connections.forEach(e => { const key = options.aggregate ? JSON.stringify([e.source, e.target, e.protocol]) : e.id; bundles.set(key, [...(bundles.get(key) ?? []), e]); });
  const grouped = [...bundles.values()];
  const edges: FlowEdge[] = grouped.map(facts => {
    const fact = facts[0];
    const siblings = pairs.get(JSON.stringify([fact.source, fact.target]))!;
    return { id: facts.length === 1 ? fact.id : `bundle:${fact.id}`, source: fact.source, target: fact.target, sourceHandle: "out", targetHandle: "in", type: "relationship",
      ariaLabel: facts.length === 1 ? `${fact.protocol} ${fact.contract}` : `${fact.protocol} ${facts.length} contracts`, reconnectable: false,
      data: { fact, facts, lane: options.aggregate
        ? grouped.filter(g => g[0].source === fact.source && g[0].target === fact.target).findIndex(g => g[0].id === fact.id)
          - (grouped.filter(g => g[0].source === fact.source && g[0].target === fact.target).length - 1) / 2
        : siblings.findIndex(e => e.id === fact.id) - (siblings.length - 1) / 2 } };
  });
  return { nodes, edges };
}
