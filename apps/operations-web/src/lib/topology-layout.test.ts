import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { topologySchema } from "./topology";
import { layoutTopology, PROTOCOLS } from "./topology-layout";

/** Source-generated STATIC_DECLARATION fixtures. These tests are not live observations. */
function declarationGraph() {
  return topologySchema.parse(JSON.parse(readFileSync(path.resolve(process.cwd(), "../../docs/examples/control-center-topology-v2.json"), "utf8")));
}
describe("v2 graph presentation", () => {
  it("lays out repository Kafka declarations without synthesizing relationships", () => {
    const graph = declarationGraph();
    const result = layoutTopology(graph, new Set(PROTOCOLS), new Set());
    expect(result.nodes.map(n => n.id).sort()).toEqual(graph.nodes.map(n => n.id).sort());
    expect(result.edges.map(e => e.id).sort()).toEqual(graph.connections.map(e => e.id).sort());
    expect(new Set(result.nodes.map(n => n.data.fact.kind))).toEqual(new Set(["CELL", "SERVICE", "KAFKA_TOPIC", "CONSUMER_GROUP"]));
    expect(result.nodes.every(n => Number.isFinite(n.position.x) && Number.isFinite(n.position.y))).toBe(true);
    expect(new Set(result.nodes.map(n => `${n.position.x}:${n.position.y}`)).size).toBe(result.nodes.length);
    expect(result.edges.every(e => graph.connections.includes(e.data!.fact))).toBe(true);
  });
  it("filters protocols and Kafka nodes while retaining disconnected discovered services", () => {
    const graph = declarationGraph();
    const result = layoutTopology(graph, new Set(["HTTP", "GRPC"]), new Set());
    expect(result.edges).toEqual([]);
    expect(result.nodes.filter(n => n.data.fact.kind === "SERVICE")).toHaveLength(5);
    expect(result.nodes.some(n => ["KAFKA_TOPIC", "CONSUMER_GROUP"].includes(n.data.fact.kind))).toBe(false);
  });
  it("expands actual epochless registry records without inserting containment edges", () => {
    const graph = declarationGraph(); const service = graph.nodes.find(n => n.kind === "SERVICE")!;
    graph.nodes.push({ ...service, id: "local-registry-instance", kind: "INSTANCE", name: "eureka-opaque-alias", instanceId: "eureka-opaque-alias",
      epoch: null, scope: "INSTANCE", serviceId: service.id, health: "UNKNOWN", uncertainty: "REGISTRY_INSTANCE_IDENTITY_UNVERIFIED_NO_EPOCH",
      evidence: [{ kind: "OBSERVED", source: "EUREKA", reference: "UP_REGISTRATION_NO_EPOCH", collectedAt: graph.asOf, lastSeen: graph.asOf, scope: "INSTANCE" }] });
    expect(topologySchema.safeParse(graph).success).toBe(true);
    expect(layoutTopology(graph, new Set(PROTOCOLS), new Set()).nodes.some(n => n.id === "local-registry-instance")).toBe(false);
    const expanded = layoutTopology(graph, new Set(PROTOCOLS), new Set([service.id]));
    expect(expanded.nodes.find(n => n.id === "local-registry-instance")?.data.fact.epoch).toBeNull();
    expect(expanded.edges).toHaveLength(graph.connections.length);
    graph.nodes[graph.nodes.length-1].evidence[0].source = "CONFIGURATION";
    expect(topologySchema.safeParse(graph).success).toBe(false);
  });
  it("refuses to promote registered routes into observed traffic", () => {
    const graph = declarationGraph();
    graph.connections[0].evidence = [{ kind: "OBSERVED", source: "SPRING_CLOUD_GATEWAY", reference: "REGISTERED_ROUTE:test", collectedAt: graph.asOf, lastSeen: graph.asOf, scope: "SERVICE" }];
    expect(topologySchema.safeParse(graph).success).toBe(false);
  });
});
