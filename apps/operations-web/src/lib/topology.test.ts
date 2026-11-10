import { describe, expect, it } from "vitest";
import { topologySchema } from "./topology";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TopologyView } from "@/components/topology-view";

export function sampleTopology() {
  const declaration = { kind: "DECLARED", source: "CONFIGURATION", reference: "apex.ops03.owners",
    collectedAt: "2026-11-09T10:00:00Z", lastSeen: null, scope: "SERVICE" };
  return { schemaVersion: 1, asOf: "2026-11-09T10:00:00Z", coverage: "CONFIGURED_OWNERS_ONLY",
    capabilities: { declared: "CONFIGURED_OWNER_QUERIES_ONLY", instances: "OWNER_SNAPSHOTS_PARTIAL",
      observedInteractions: "NOT_CONFIGURED", messaging: "NOT_CONFIGURED", traces: "NOT_CONFIGURED", runtimeHealth: "NOT_CONFIGURED" },
    nodes: ["console", "order-cell"].map((id) => ({ id, kind: "SERVICE", name: id, environment: "test",
      serviceId: id, instanceId: null, epoch: null, scope: "SERVICE", health: "UNKNOWN", freshness: "MISSING",
      uncertainty: "UNKNOWN", lastSeen: null, evidence: [declaration] })),
    connections: [{ id: "read", source: "console", target: "order-cell", environment: "test", protocol: "GRPC",
      scope: "SERVICE", contract: "OPS_OWNER_SNAPSHOT:1", health: "UNKNOWN", lastSeen: null, evidence: [declaration] }],
  };
}

describe("topology contract", () => {
  it("validates runtime provenance and refuses stale duplicate claims", () => {
    const graph=topologySchema.parse({...sampleTopology(),schemaVersion:2,providers:[{name:"membership",status:"AVAILABLE",coverage:"SCOPED_REGISTRY_COMPLETE",sourceMode:"REGISTRY_AND_MTLS",collectedAt:"2026-11-10T07:00:00Z"}]});
    const epoch="00000000-0000-4000-8000-000000000001";
    graph.nodes.push({...graph.nodes[1],id:"replica",kind:"INSTANCE",scope:"INSTANCE",instanceId:"one",epoch});
    graph.memberships.push({id:"replica",environment:"test",cell:"order-cell",service:"order-cell",instanceId:"one",epoch,
      registryId:"registry-one",registryIdentity:"owned-eureka",discoverySource:"EUREKA",lastSeen:"2026-11-10T07:00:00Z",
      freshness:"FRESH",membershipState:"DUPLICATE",coverage:"SCOPED_REGISTRY_COMPLETE",transportStatus:"AVAILABLE"});
    expect(topologySchema.safeParse(graph).success).toBe(true);
    const html=renderToStaticMarkup(createElement(TopologyView,{graph}));
    expect(html).toContain("DUPLICATE");expect(html).toContain("owned-eureka");
    graph.memberships[0].freshness="STALE";
    expect(topologySchema.safeParse(graph).success).toBe(false);
    graph.memberships[0].membershipState="UNKNOWN";
    expect(topologySchema.safeParse(graph).success).toBe(true);
    graph.memberships[0].environment="other";
    expect(topologySchema.safeParse(graph).success).toBe(false);
  });
  it("accepts a declared dependency without claiming traffic or health", () => {
    expect(topologySchema.parse(sampleTopology()).connections[0].lastSeen).toBeNull();
  });
  it("rejects dangling and cross-environment edges, duplicate identities and invented health", () => {
    const graph = sampleTopology();
    graph.connections[0].target = "missing";
    expect(topologySchema.safeParse(graph).success).toBe(false);
    graph.connections[0].target = "order-cell";
    graph.nodes[1].environment = "prod";
    expect(topologySchema.safeParse(graph).success).toBe(false);
    graph.nodes[1] = graph.nodes[0];
    expect(topologySchema.safeParse(graph).success).toBe(false);
    graph.nodes[0].health = "UP";
    expect(topologySchema.safeParse(graph).success).toBe(false);
  });
  it("rejects treating a declared edge as observed traffic", () => {
    const graph = sampleTopology();
    graph.connections[0].evidence[0].kind = "OBSERVED";
    expect(topologySchema.safeParse(graph).success).toBe(false);
  });
  it("renders declared edges and stale instance evidence with explicit limits", () => {
    const graph = topologySchema.parse(sampleTopology());
    graph.nodes.push({ ...graph.nodes[1], id: "order-instance", kind: "INSTANCE", scope: "INSTANCE",
      instanceId: "one", epoch: "00000000-0000-4000-8000-000000000001", freshness: "STALE",
      lastSeen: "2026-11-09T09:00:00Z", evidence: [{ kind: "OBSERVED", source: "OWNER_SNAPSHOT",
        reference: "OPS_OWNER_SNAPSHOT:1", collectedAt: "2026-11-09T09:00:00Z",
        lastSeen: "2026-11-09T09:00:00Z", scope: "INSTANCE" }] });
    const html = renderToStaticMarkup(createElement(TopologyView, { graph }));
    for (const text of ["DECLARED", "OBSERVED INSTANCE", "STALE", "Health: UNKNOWN", "traffic last seen: unknown",
      "00000000-0000-4000-8000-000000000001", "NOT_CONFIGURED", "OWNER_SNAPSHOT"]) expect(html).toContain(text);
  });
});
