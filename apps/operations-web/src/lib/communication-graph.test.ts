import { readFileSync } from "node:fs";
import path from "node:path";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { topologySchema } from "./topology";
import { layoutTopology, PROTOCOLS } from "./topology-layout";
import { ConnectionDetails } from "@/components/topology-details";

/** Source-generated STATIC_DECLARATION fixture, not live traffic. */
function declarations() { return topologySchema.parse(JSON.parse(readFileSync(path.resolve(process.cwd(), "../../docs/examples/control-center-topology-v2.json"), "utf8"))); }

describe("complete communication graph", () => {
  it("preserves individual event contracts when visually bundling shared topic links", () => {
    const graph = declarations(); const layout = layoutTopology(graph, new Set(PROTOCOLS), new Set(), { aggregate: true });
    expect(layout.edges.length).toBeLessThan(graph.connections.length);
    expect(layout.edges.flatMap(e => e.data!.facts.map(f => f.id)).sort()).toEqual(graph.connections.map(e => e.id).sort());
    expect(layout.edges.some(e => e.data!.facts.length > 1 && new Set(e.data!.facts.map(f => f.contract)).size > 1)).toBe(true);
  });
  it("shows all five reference paths using actual repository contracts and exact handlers", () => {
    const graph = declarations();
    for (const [contract, publisher, consumer] of [["ORDER_PLACED:1", "order-cell", "inventory-cell"], ["INVENTORY_RESERVED:1", "inventory-cell", "order-cell"],
      ["ORDER_READY_FOR_PAYMENT:1", "order-cell", "payment-cell"], ["PAYMENT_AUTHORIZED:1", "payment-cell", "order-cell"], ["CUSTOMER_CREATED:1", "customer-cell", "reporting-cell"]]) {
      const edge = graph.connections.find(e => e.contract === contract && e.details?.kafka.some(p => p.publisherServiceId.endsWith(publisher) && p.consumerServiceId?.endsWith(consumer)))!;
      expect(edge).toBeTruthy(); const html = renderToStaticMarkup(createElement(ConnectionDetails, { connection: edge, graph }));
      for (const text of [contract, publisher, consumer, "Kafka topic", "Consumer group", "Registered handler", "EXACT_DECLARED_CONTRACT_MATCH", "DECLARED", "traffic last seen: unknown"]) expect(html).toContain(text);
    }
  });
  it("focuses on one service's exact dependency paths and removes unrelated Customer flow", () => {
    const graph = declarations(); const order = graph.nodes.find(n => n.kind === "SERVICE" && n.name === "order-cell")!;
    const focused = layoutTopology(graph, new Set(PROTOCOLS), new Set(), { focus: order.id, aggregate: true });
    expect(focused.nodes.some(n => n.id === order.id)).toBe(true);
    expect(focused.edges.flatMap(e => e.data!.facts).some(e => e.contract === "CUSTOMER_CREATED:1")).toBe(false);
    expect(focused.edges.flatMap(e => e.data!.facts).some(e => e.contract === "ORDER_PLACED:1")).toBe(true);
    const filtered = layoutTopology(graph, new Set(["HTTP", "GRPC"]), new Set(), { focus: order.id, aggregate: true });
    expect(filtered.edges).toEqual([]); expect(filtered.nodes.map(n => n.id)).toEqual([order.id]);
  });
  it("rejects wrong Kafka cluster/type and protocol details without converting declarations to traffic", () => {
    const graph = declarations(); const edge = graph.connections.find(e => e.details?.kafka.length)!;
    edge.details!.kafka[0].cluster = "wrong-cluster";
    expect(topologySchema.safeParse(graph).success).toBe(false);
    edge.details!.kafka[0].cluster = "test-cluster"; edge.protocol = "HTTP";
    expect(topologySchema.safeParse(graph).success).toBe(false);
  });
  it("renders explicit gRPC Query and Request metadata and configuration availability", () => {
    const graph = declarations(); const source = graph.nodes.find(n => n.kind === "SERVICE")!;
    const target = graph.nodes.find(n => n.kind === "SERVICE" && n.id !== source.id)!;
    for (const kind of ["QUERY", "REQUEST"] as const) {
      const connection = { id: `test-${kind}`, source: source.id, target: target.id, environment: source.environment, protocol: "GRPC" as const, scope: "SERVICE" as const,
        contract: "EXPLICIT_TEST_CONTRACT:1", health: "UNKNOWN" as const, lastSeen: null,
        evidence: [{ kind: "DECLARED" as const, source: "CONTRACT_DISCOVERY", reference: "FIXTURE_JAVA_CALLER_DECLARATION", collectedAt: graph.asOf, lastSeen: null, scope: "SERVICE" as const }],
        details: { http: null, kafka: [], grpc: { kind, callerServiceId: source.id, targetServiceId: target.id, discoveryTarget: target.name, behavior: "REQUEST_RESPONSE" as const, availability: "DISCOVERY_CONFIGURED" } } };
      const html = renderToStaticMarkup(createElement(ConnectionDetails, { connection, graph }));
      for (const text of [kind, "EXPLICIT_TEST_CONTRACT:1", "Discovery target", "DISCOVERY_CONFIGURED", "DECLARED"]) expect(html).toContain(text);
    }
  });
});
