import { readFileSync } from "node:fs";
import path from "node:path";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { topologySchema } from "./topology";
import { TopologyGraph } from "@/components/topology-graph";

describe("visual graph rendering from repository declarations (not live traffic)", () => {
  it("renders distinct Kafka, group, service and Cell nodes with selectable declared edges", () => {
    const graph = topologySchema.parse(JSON.parse(readFileSync(path.resolve(process.cwd(), "../../docs/examples/control-center-topology-v2.json"), "utf8")));
    const html = renderToStaticMarkup(createElement(TopologyGraph, { graph }));
    for (const kind of ["service", "cell", "kafka_topic", "consumer_group"]) expect(html).toContain(`topology-node--${kind}`);
    expect(html).toContain("react-flow__edge");
    expect(html).toContain("Topology details");
    expect(html).toContain("HTTP"); expect(html).toContain("gRPC"); expect(html).toContain("Kafka");
    expect(html).toContain("dashed = declared");
  });
});
