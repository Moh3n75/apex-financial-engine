"use client";

import { useCallback, useMemo, useState } from "react";
import { Background, BaseEdge, Controls, EdgeLabelRenderer, Handle, MarkerType, Position, ReactFlow,
  type NodeProps, type EdgeProps } from "@xyflow/react";
import { Box, Boxes, Radio, Server, Users, Network } from "lucide-react";
import type { Topology } from "@/lib/topology";
import { layoutTopology, PROTOCOLS, type Protocol, type FlowNode, type FlowEdge } from "@/lib/topology-layout";
import { NodeDetails, ConnectionDetails } from "./topology-details";

const colors = { HTTP: "#2563eb", GRPC: "#7c3aed", KAFKA: "#b45309" };
const icons = { CELL: Boxes, SERVICE: Server, INSTANCE: Box, KAFKA_TOPIC: Radio, CONSUMER_GROUP: Users };
const label = (protocol: string) => protocol === "GRPC" ? "gRPC" : protocol === "KAFKA" ? "Kafka" : protocol;

function ServiceNode({ data, selected }: NodeProps<FlowNode>) {
  const { fact, instanceCount, expanded } = data;
  const Icon = data.gateway ? Network : icons[fact.kind];
  return <div className={`topology-node topology-node--${fact.kind.toLowerCase()} ${selected ? "is-selected" : ""}`}>
    <Handle id="in" type="target" position={Position.Left} isConnectable={false} />
    <div className="flex items-center gap-2 text-[10px] font-semibold tracking-wide"><Icon size={14} aria-hidden="true" />{data.gateway ? "API GATEWAY" : fact.kind.replaceAll("_", " ")}</div>
    <div className="mt-1 truncate text-sm font-semibold" title={fact.name}>{fact.name}</div>
    <div className="mt-1 flex justify-between gap-2 text-[10px] text-zinc-500"><span>{fact.environment} · {fact.freshness}</span>
      {fact.kind === "SERVICE" ? <span>{expanded ? "−" : "+"} {instanceCount} instance records</span> : null}
      {fact.kind === "INSTANCE" ? <span>{fact.epoch ? "epoch provided" : "registry only"}</span> : null}
    </div>
    <Handle id="out" type="source" position={Position.Right} isConnectable={false} />
  </div>;
}

function RelationshipEdge(props: EdgeProps<FlowEdge>) {
  const { sourceX: sx, sourceY: sy, targetX: tx, targetY: ty, data, selected, markerEnd } = props;
  if (!data) return null;
  const offset = data.lane * 44;
  const self = props.source === props.target;
  const dx = tx - sx, dy = ty - sy, length = Math.max(Math.hypot(dx, dy), 1);
  const cx = (sx + tx) / 2 - dy / length * offset, cy = (sy + ty) / 2 + dx / length * offset;
  const path = self ? `M ${sx} ${sy} C ${sx+100} ${sy-150}, ${tx-100} ${ty-150}, ${tx} ${ty}`
    : `M ${sx} ${sy} Q ${cx} ${cy} ${tx} ${ty}`;
  const color = colors[data.fact.protocol];
  return <>
    <BaseEdge id={props.id} path={path} markerEnd={markerEnd} interactionWidth={24}
      style={{ stroke: color, strokeWidth: selected ? 3 : 1.7, strokeDasharray: data.fact.evidence.every(e => e.kind === "DECLARED") ? "6 4" : undefined }} />
    <EdgeLabelRenderer><span className="topology-edge-label" style={{ transform: `translate(-50%, -50%) translate(${self ? (sx+tx)/2 : (sx+2*cx+tx)/4}px,${self ? sy-112 : (sy+2*cy+ty)/4}px)` }}>{label(data.fact.protocol)}{data.facts.length > 1 ? ` ×${data.facts.length}` : ""}</span></EdgeLabelRenderer>
  </>;
}
const nodeTypes = { topology: ServiceNode };
const edgeTypes = { relationship: RelationshipEdge };
type Selection = { kind: "node" | "connection"; id: string; contractId?: string } | null;

export function TopologyGraph({ graph }: { graph: Topology }) {
  const [protocols, setProtocols] = useState<Set<Protocol>>(new Set(PROTOCOLS));
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [selection, setSelection] = useState<Selection>(null);
  const [ready, setReady] = useState(false);
  const [focus, setFocus] = useState<string | null>(null);
  const layout = useMemo(() => layoutTopology(graph, protocols, expanded, { focus, aggregate: true }), [graph, protocols, expanded, focus]);
  const node = selection?.kind === "node" ? graph.nodes.find(n => n.id === selection.id) : null;
  const contextId = node?.kind === "CELL" ? node.serviceId ?? node.id : node?.id;
  const selectedConnections = selection?.kind === "connection" ? layout.edges.find(e => e.id === selection.id)?.data?.facts ?? graph.connections.filter(e => e.id === selection.id) : [];
  const connection = selectedConnections.find(e => e.id === selection?.contractId) ?? selectedConnections[0];
  const toggleExpanded = (id: string) => { setReady(false); setExpanded(previous => {
    const next = new Set(previous); if (next.has(id)) next.delete(id); else next.add(id); return next;
  }); };
  const selectNode = (id: string) => {
    setSelection({ kind: "node", id });
    const item = graph.nodes.find(n => n.id === id);
    const service = item?.kind === "SERVICE" ? id : item?.kind === "CELL" ? item.serviceId : null;
    if (service && !expanded.has(service)) {
      setReady(false); setExpanded(previous => new Set([...previous, service]));
    }
  };
  const syncSelection = useCallback(({ nodes, edges }: { nodes: FlowNode[]; edges: FlowEdge[] }) => {
    const item = nodes[0] ? { kind: "node" as const, id: nodes[0].id } : edges[0] ? { kind: "connection" as const, id: edges[0].id } : null;
    if (item) setSelection(previous => previous?.kind === item.kind && previous.id === item.id ? previous : item);
  }, []);
  const visibleEdges = layout.edges.map(e => ({ ...e, selected: selection?.id === e.id,
    markerEnd: { type: MarkerType.ArrowClosed, color: colors[e.data!.fact.protocol] } }));
  return <section className="my-4" aria-label="Interactive service relationship graph">
    <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
      <fieldset className="flex gap-4 text-xs"><legend className="sr-only">Filter connections by protocol</legend>
        {PROTOCOLS.map(protocol => <label key={protocol} className="flex cursor-pointer items-center gap-1.5"><input type="checkbox"
          checked={protocols.has(protocol)} onChange={() => { setReady(false); setSelection(null); setProtocols(previous => {
            const next = new Set(previous); if (next.has(protocol)) next.delete(protocol); else next.add(protocol); return next;
          }); }} />{label(protocol)} <span style={{ background: colors[protocol] }} className="inline-block h-0.5 w-4" /></label>)}
      </fieldset>
      <div className="flex items-center gap-2 text-xs"><label htmlFor="topology-focus">Service focus</label><select id="topology-focus" className="max-w-52 rounded border border-zinc-300 bg-transparent px-2 py-1 dark:border-zinc-600" value={focus ?? ""}
        onChange={e => { setReady(false); setSelection(null); setFocus(e.target.value || null); }}><option value="">Full graph</option>
        {graph.nodes.filter(n => n.kind === "SERVICE").map(n => <option key={n.id} value={n.id}>{n.name} · {n.environment}</option>)}</select>
        {focus ? <button className="topology-button" onClick={() => { setReady(false); setSelection(null); setFocus(null); }}>Full graph overview</button> : null}
      </div>
      <div className="flex items-center gap-3"><p className="text-xs text-zinc-500" aria-live="polite">{layout.nodes.length} nodes · {layout.edges.length} links · {layout.edges.reduce((n,e) => n + e.data!.facts.length, 0)} contracts · dashed = declared</p>
        <button className="topology-button text-xs" onClick={() => window.location.reload()}>Refresh topology</button>
      </div>
    </div>
    <div className="grid overflow-hidden rounded-lg border border-zinc-200 dark:border-zinc-700 lg:grid-cols-[minmax(0,1fr)_300px]">
      <div className="topology-canvas relative h-[560px] min-w-0 bg-zinc-50 dark:bg-zinc-950" data-testid="topology-canvas" data-ready={ready}>
        <ReactFlow<FlowNode, FlowEdge> key={`${[...protocols].join()}:${[...expanded].join()}:${focus}:${graph.asOf}`}
          nodes={layout.nodes.map(n => ({ ...n, selected: selection?.id === n.id }))} edges={visibleEdges}
          nodeTypes={nodeTypes} edgeTypes={edgeTypes}
          onInit={instance => { void instance.fitView({ padding: 0.22, maxZoom: 1 }).then(() => setReady(true)); }}
          minZoom={0.12} maxZoom={2} nodesDraggable={false} nodesConnectable={false} edgesReconnectable={false}
          deleteKeyCode={null} selectionKeyCode={null} multiSelectionKeyCode={null} colorMode="system"
          onNodeClick={(_, n) => selectNode(n.id)} onEdgeClick={(_, e) => setSelection({ kind: "connection", id: e.id })}
          onPaneClick={() => setSelection(null)}
          onSelectionChange={syncSelection}>
          <Background gap={22} size={1} color="#a1a1aa" /><Controls showInteractive={false} />
        </ReactFlow>
        {!ready ? <div className="absolute inset-0 z-20 flex items-center justify-center bg-zinc-50/90 text-xs text-zinc-500 dark:bg-zinc-950/90" role="status">Preparing graph…</div> : null}
      </div>
      <aside className="max-h-[560px] overflow-y-auto border-t border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900 lg:border-t-0 lg:border-l" aria-label="Topology details">
        <h3 className="mb-3 text-sm font-semibold">{node?.name ?? (connection ? "Connection details" : "Explore relationships")}</h3>
        {node ? <>
          <NodeDetails node={node} graph={graph} />
          {(node.kind === "SERVICE" || node.kind === "CELL") && contextId ? <div className="my-4 text-xs">
            <button className="topology-button mb-2 mr-2" disabled={focus === contextId} onClick={() => { setReady(false); setFocus(contextId); }}>Focus dependencies</button>
            {graph.nodes.some(n => n.kind === "INSTANCE" && n.serviceId === contextId) ? <button className="topology-button" onClick={() => toggleExpanded(contextId)}>
              {expanded.has(contextId) ? "Collapse instances" : "Expand instances"}</button> : <p>No instance evidence is available for this service.</p>}
          </div> : null}
          <h4 className="mt-4 text-xs font-semibold">Visible connections</h4>
          <ul className="mt-2 space-y-2">{layout.edges.filter(e => e.source === contextId || e.target === contextId).map(e =>
            <li key={e.id}><button className="text-left text-xs text-blue-700 underline dark:text-blue-300 break-all" onClick={() => setSelection({ kind: "connection", id: e.id })}>{e.data!.fact.protocol} · {e.data!.facts.length > 1 ? `${e.data!.facts.length} contracts` : e.data!.fact.contract}</button></li>)}</ul>
        </> : connection ? <>
          {selectedConnections.length > 1 ? <label className="mb-4 block text-xs">Contract ({selectedConnections.length})<select aria-label="Connection contract" className="mt-1 w-full rounded border border-zinc-300 bg-transparent p-1 dark:border-zinc-600" value={connection.id}
            onChange={e => setSelection(previous => previous ? { ...previous, contractId: e.target.value } : previous)}>
            {selectedConnections.map(e => <option key={e.id} value={e.id}>{e.details?.grpc?.kind ?? e.protocol} · {e.contract}</option>)}
          </select></label> : null}
          <ConnectionDetails connection={connection} graph={graph} />
        </> : <div className="space-y-4 text-sm text-zinc-500">
          <p>Select a node or connection for evidence and status. Select a service to reveal its instance records.</p>
          <p>Scroll to zoom, drag the canvas to pan, or use fit-to-view in the lower left.</p>
          <p>Disconnected services have no visible relationship evidence. Routes and registered handlers do not prove traffic.</p>
        </div>}
      </aside>
    </div>
    <div className="mt-2 flex flex-wrap gap-4 text-[10px] text-zinc-500" aria-label="Node kinds">
      {Object.entries(icons).map(([kind, Icon]) => <span key={kind} className="inline-flex items-center gap-1"><Icon size={12} />{kind.replaceAll("_", " ")}</span>)}
    </div>
  </section>;
}
