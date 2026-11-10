import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { topologySchema } from "./topology";
import { TopologyView } from "@/components/topology-view";

describe("registry service presence contract (test declaration, not live evidence)", () => {
  const seen = "2026-11-10T12:00:00Z";
  function graph() {
    return topologySchema.parse({ schemaVersion: 2, asOf: seen, coverage: "PARTIAL_REGISTRY",
      capabilities: { declared: "NOT_CONFIGURED", instances: "NOT_CONFIGURED", observedInteractions: "NOT_CONFIGURED",
        messaging: "NOT_CONFIGURED", traces: "NOT_CONFIGURED", runtimeHealth: "NOT_CONFIGURED" },
      nodes: [{ id: "local:service:order-cell", name: "order-cell", kind: "SERVICE", environment: "local",
        serviceId: "local:service:order-cell", instanceId: null, epoch: null, scope: "SERVICE", health: "UNKNOWN",
        freshness: "FRESH", uncertainty: "LOCAL_REGISTRY_PRESENCE_NOT_HEALTH_OR_TRAFFIC", lastSeen: seen,
        evidence: [{ kind: "OBSERVED", source: "EUREKA", reference: "UP_REGISTRATION_LOCAL_REGISTRY",
          collectedAt: seen, lastSeen: seen, scope: "SERVICE" }] }], connections: [],
      providers: [{ name: "eureka-local-inventory", status: "AVAILABLE", coverage: "UP_REGISTRATIONS_ONLY",
        sourceMode: "REGISTRY_OBSERVATION", collectedAt: seen }], memberships: [], descriptors: [], unresolved: [] });
  }
  it("renders discovery without owners, fabricated epochs, health or traffic", () => {
    const value = graph();
    const html = renderToStaticMarkup(createElement(TopologyView, { graph: value }));
    for (const text of ["order-cell", "REGISTRY PRESENCE", "Health: UNKNOWN", "EUREKA", "not confirmed health or traffic"])
      expect(html).toContain(text);
    expect(html).not.toContain("Epoch:");
    expect(value.memberships).toEqual([]);
    expect(value.connections).toEqual([]);
    value.nodes[0].health = "UP";
    expect(topologySchema.safeParse(value).success).toBe(false);
  });
  it("renders unavailable or empty inventory with explicit coverage", () => {
    const value = graph(); value.nodes = []; value.providers[0].status = "SOURCE_UNAVAILABLE";
    const html = renderToStaticMarkup(createElement(TopologyView, { graph: value }));
    expect(html).toContain("SOURCE_UNAVAILABLE");
    expect(html).toContain("No visible discovered services");
  });
  it("refuses registry presence as observed interaction evidence", () => {
    const value = graph();
    value.connections.push({ id: "invented-traffic", source: value.nodes[0].id, target: value.nodes[0].id,
      environment: "local", protocol: "HTTP", scope: "SERVICE", contract: "UNKNOWN", health: "UNKNOWN",
      lastSeen: seen, evidence: value.nodes[0].evidence });
    expect(topologySchema.safeParse(value).success).toBe(false);
  });
});
