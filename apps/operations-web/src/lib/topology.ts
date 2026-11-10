import { z } from "zod";

const instant = z.string().refine((v) => !Number.isNaN(Date.parse(v)), "instant");
const evidence = z.object({
  kind: z.enum(["DECLARED", "OBSERVED"]), source: z.string().min(1).max(80), reference: z.string().max(200),
  collectedAt: instant.nullable(), lastSeen: instant.nullable(), scope: z.enum(["SERVICE", "INSTANCE"]),
}).superRefine((e, ctx) => {
  if (e.kind === "DECLARED" && e.lastSeen !== null) ctx.addIssue({ code: "custom", message: "Declaration is not an observation" });
  if (e.kind === "OBSERVED" && ["CONFIGURATION", "CONTRACT_DISCOVERY"].includes(e.source)) ctx.addIssue({ code: "custom", message: "Contract discovery is not observed traffic" });
});
const schema = z.object({
  schemaVersion: z.union([z.literal(1), z.literal(2)]), asOf: instant, coverage: z.string().max(200),
  capabilities: z.object({
    declared: z.string().max(100), instances: z.string().max(100), observedInteractions: z.string().max(100),
    messaging: z.string().max(100), traces: z.string().max(100), runtimeHealth: z.string().max(100),
  }),
  nodes: z.array(z.object({
    id: z.string().max(600), kind: z.enum(["CELL", "SERVICE", "INSTANCE", "KAFKA_TOPIC", "CONSUMER_GROUP"]),
    name: z.string().min(1).max(249), environment: z.string().min(1).max(64),
    serviceId: z.string().max(200).nullable(), instanceId: z.string().max(64).nullable(), epoch: z.string().uuid().nullable(),
    scope: z.enum(["SERVICE", "INSTANCE"]), health: z.enum(["UNKNOWN", "UP", "DEGRADED", "DOWN"]),
    freshness: z.string().max(40), uncertainty: z.string().max(100), lastSeen: instant.nullable(),
    evidence: z.array(evidence).min(1).max(16),
  })).max(200),
  connections: z.array(z.object({
    id: z.string().max(1500), source: z.string().max(600), target: z.string().max(600), environment: z.string().max(64),
    protocol: z.enum(["HTTP", "GRPC", "KAFKA"]), scope: z.enum(["SERVICE", "INSTANCE"]), contract: z.string().max(100),
    health: z.enum(["UNKNOWN", "UP", "DEGRADED", "DOWN"]), lastSeen: instant.nullable(), evidence: z.array(evidence).min(1).max(16),
  })).max(400),
  providers: z.array(z.object({ name: z.string().max(200), status: z.string().max(40), coverage: z.string().max(200),
    sourceMode: z.string().max(40), collectedAt: instant.nullable() })).max(32).default([]),
  unresolved: z.array(z.object({ serviceId: z.string().max(200), contract: z.string().max(100), reason: z.string().max(100) })).max(256).default([]),
  descriptors: z.array(z.object({ serviceId: z.string().max(200), protocol: z.enum(["HTTP", "GRPC", "KAFKA"]),
    kind: z.string().max(40), contract: z.string().max(100), evidence: z.array(evidence).min(1).max(16) })).max(512).default([]),
  memberships: z.array(z.object({id:z.string().max(600),environment:z.string().max(64),cell:z.string().max(64),service:z.string().max(64),
    instanceId:z.string().max(64),epoch:z.string().uuid(),registryId:z.string().max(128),registryIdentity:z.string().max(64),
    discoverySource:z.string().max(40),lastSeen:instant.nullable(),freshness:z.enum(["FRESH","STALE"]),
    membershipState:z.enum(["VERIFIED","DUPLICATE","DISAPPEARED","UNVERIFIED","REJECTED","UNKNOWN"]),coverage:z.string().max(80),transportStatus:z.string().max(40)})).max(128).default([]),
});
export const topologySchema = schema.superRefine((graph, ctx) => {
  const issue = (message: string) => ctx.addIssue({ code: "custom", message });
  const nodes = new Map(graph.nodes.map((n) => [n.id, n]));
  if (nodes.size !== graph.nodes.length) issue("Duplicate node");
  if (new Set(graph.connections.map(e => e.id)).size !== graph.connections.length) issue("Duplicate connection");
  for (const edge of graph.connections) {
    if (nodes.get(edge.source)?.environment !== edge.environment || nodes.get(edge.target)?.environment !== edge.environment) issue("Invalid connection endpoints");
    if (edge.lastSeen !== null && !edge.evidence.some(e => e.kind === "OBSERVED" && e.lastSeen !== null)) issue("Traffic requires observed evidence");
    if (edge.evidence.some(e => e.source === "OWNER_SNAPSHOT" && e.kind === "OBSERVED")) issue("Instance snapshot is not traffic");
    if (edge.evidence.some(e => e.source === "EUREKA" && e.kind === "OBSERVED")) issue("Registry presence is not traffic");
  }
  for (const node of graph.nodes) {
    if (node.kind === "INSTANCE") {
      const parent = node.serviceId ? nodes.get(node.serviceId) : null;
      if (parent?.kind !== "SERVICE" || parent.environment !== node.environment || !node.epoch || !node.instanceId) issue("Invalid instance scope");
    }
    if (node.health !== "UNKNOWN" && !node.evidence.some(e => e.kind === "OBSERVED" && ["ACTUATOR", "ORCHESTRATOR"].includes(e.source))) issue("Health requires runtime evidence");
  }
  for (const item of [...graph.descriptors, ...graph.unresolved]) if (nodes.get(item.serviceId)?.kind !== "SERVICE") issue("Invalid descriptor service");
  if (graph.schemaVersion === 2 && graph.providers.length === 0) issue("Version 2 requires provider coverage");
  for(const member of graph.memberships) {
    const node=nodes.get(member.id);
    if(node?.kind!=="INSTANCE"||node.environment!==member.environment||node.epoch!==member.epoch||node.instanceId!==member.instanceId)issue("Invalid membership scope");
    if(member.membershipState==="DUPLICATE"&&(member.freshness!=="FRESH"||member.transportStatus!=="AVAILABLE"||member.coverage!=="SCOPED_REGISTRY_COMPLETE"))issue("Duplicate requires fresh authoritative evidence");
  }
  if (graph.schemaVersion === 1) {
    for (const edge of graph.connections) if (edge.evidence.some(e => e.kind !== "DECLARED")) issue("Version 1 supports declared edges only");
  }
});
export type Topology = z.infer<typeof topologySchema>;
