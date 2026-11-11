import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { topologySchema } from "./topology";
import { TopologyView } from "@/components/topology-view";

describe("backend-generated repository declaration contract", () => {
  it("validates and renders all five cells, explicit Kafka nodes, and static provenance", () => {
    const raw = JSON.parse(readFileSync(path.resolve(process.cwd(), "../../docs/examples/control-center-topology-v2.json"), "utf8"));
    const graph = topologySchema.parse(raw);
    expect(graph.schemaVersion).toBe(2);
    expect(graph.nodes.filter(n => n.kind === "SERVICE").map(n => n.name).sort()).toEqual(
      ["customer-cell", "inventory-cell", "order-cell", "payment-cell", "reporting-cell"]);
    expect(graph.connections).toHaveLength(28);
    expect(graph.connections.every(e => e.protocol === "KAFKA" && e.evidence.every(p => p.kind === "DECLARED") && e.lastSeen === null)).toBe(true);
    expect(graph.providers.filter(p => p.name.startsWith("contracts:")).every(p => p.sourceMode === "STATIC_DECLARATION")).toBe(true);
    const html = renderToStaticMarkup(createElement(TopologyView, { graph }));
    for (const text of ["KAFKA_TOPIC", "CONSUMER_GROUP", "STATIC_DECLARATION", "GET_ORDER:1", "CUSTOMER_CREATED:1", "apex.order.payment-requests.v1"]) expect(html).toContain(text);
  });
  it("supports observed OTel evidence without converting declarations to observations", () => {
    const raw = JSON.parse(readFileSync(path.resolve(process.cwd(), "../../docs/examples/control-center-topology-v2.json"), "utf8"));
    const edge = raw.connections[0];
    edge.lastSeen = "2026-11-09T13:00:00Z";
    expect(topologySchema.safeParse(raw).success).toBe(false);
    edge.evidence.push({ kind: "OBSERVED", source: "OPEN_TELEMETRY", reference: "sampled-span", scope: "SERVICE",
      collectedAt: edge.lastSeen, lastSeen: edge.lastSeen });
    expect(new Set(topologySchema.parse(raw).connections[0].evidence.map(p => p.kind))).toEqual(new Set(["DECLARED", "OBSERVED"]));
    edge.evidence[edge.evidence.length - 1].source = "CONTRACT_DISCOVERY";
    expect(topologySchema.safeParse(raw).success).toBe(false);
  });
});
