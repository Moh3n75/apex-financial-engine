import { test, expect } from "@playwright/test";
import { topologySchema } from "../src/lib/topology";

test("live registered Gateway routes and registry instances render interactively", async ({ page, request }) => {
  test.skip(process.env.APEX_LOCAL_INVENTORY_LIVE !== "true", "Requires actual local Gateway, Console and Eureka");
  const routeResponse = await request.get("http://127.0.0.1:8080/actuator/gateway/routes");
  expect(routeResponse.status()).toBe(200);
  const routes: { route_id: string; uri: string }[] = await routeResponse.json();
  const response = await request.get("/api/ops/topology");
  expect(response.status()).toBe(200);
  const graph = topologySchema.parse(await response.json());
  expect(graph.providers.find(p => p.name === "gateway-routes")?.status).toBe("AVAILABLE");
  const edges = graph.connections.filter(e => e.protocol === "HTTP");
  const visualCount = (connections: typeof graph.connections) => new Set(connections.map(e => JSON.stringify([e.source, e.target, e.protocol]))).size;
  const expected = routes.filter(r => r.uri.startsWith("lb://") && graph.nodes.some(n => n.kind === "SERVICE" && n.name === new URL(r.uri).hostname.toLowerCase()));
  expect(edges.map(e => e.contract).sort()).toEqual(expected.map(r => `ROUTE:${r.route_id}`).sort());
  expect(edges.length).toBeGreaterThan(0);
  expect(edges.every(e => e.lastSeen === null && e.evidence.every(p => p.kind === "DECLARED"))).toBe(true);
  const externalRequests: string[] = [];
  page.on("request", r => { if (!r.url().startsWith("http://127.0.0.1:3000") && !r.url().startsWith("data:")) externalRequests.push(r.url()); });
  const errors: string[] = []; page.on("pageerror", error => errors.push(error.message));
  await page.goto("/topology");
  await expect(page.getByTestId("topology-canvas")).toBeVisible();
  await expect(page.getByTestId("topology-canvas")).toHaveAttribute("data-ready", "true");
  await expect(page.locator(".react-flow__edge")).toHaveCount(visualCount(graph.connections));
  const service = graph.nodes.find(n => n.kind === "SERVICE" && graph.nodes.some(i => i.kind === "INSTANCE" && i.serviceId === n.id))!;
  expect(service).toBeTruthy();
  await page.getByRole("button", { name: `SERVICE ${service.name}`, exact: true }).click();
  await expect(page.getByTestId("topology-canvas")).toHaveAttribute("data-ready", "true");
  const sidebar = page.getByRole("complementary", { name: "Topology details" });
  await expect(sidebar.getByRole("heading", { name: service.name, exact: true })).toBeVisible();
  const instances = graph.nodes.filter(n => n.kind === "INSTANCE" && n.serviceId === service.id);
  for (const instance of instances) await expect(page.getByRole("button", { name: `INSTANCE ${instance.name}`, exact: true })).toBeVisible();
  await page.getByRole("button", { name: `INSTANCE ${instances[0].name}`, exact: true }).click();
  await expect(sidebar.getByText("Epoch: not provided (registry record only)")).toBeVisible();
  await page.locator('.react-flow__edge[aria-label^="HTTP "]').first().click({ force: true });
  await expect(sidebar.getByRole("heading", { name: "Connection details" })).toBeVisible();
  await expect(sidebar.getByText("SPRING_CLOUD_GATEWAY", { exact: false })).toBeVisible();
  await page.getByRole("checkbox", { name: "HTTP", exact: true }).uncheck();
  await expect(page.getByTestId("topology-canvas")).toHaveAttribute("data-ready", "true");
  await expect(page.locator(".react-flow__edge")).toHaveCount(visualCount(graph.connections.filter(e => e.protocol !== "HTTP")));
  await page.getByRole("checkbox", { name: "HTTP", exact: true }).check();
  await expect(page.getByTestId("topology-canvas")).toHaveAttribute("data-ready", "true");
  await expect(page.locator(".react-flow__edge")).toHaveCount(visualCount(graph.connections));
  await page.getByRole("button", { name: "Fit View", exact: true }).click();
  await page.getByRole("button", { name: "Zoom In", exact: true }).click();
  expect(errors).toEqual([]); expect(externalRequests).toEqual([]);
  await page.screenshot({ path: "test-results/live-relationship-graph.png", fullPage: true });
  await test.info().attach("live-graph-counts", { contentType: "application/json", body: JSON.stringify({
    source: "LIVE_GATEWAY_CONSOLE_BFF_BROWSER", asOf: graph.asOf, services: graph.nodes.filter(n => n.kind === "SERVICE").length,
    registryInstances: graph.nodes.filter(n => n.kind === "INSTANCE").length, httpEdges: edges.length,
    kafkaEdges: graph.connections.filter(e => e.protocol === "KAFKA").length, memberships: graph.memberships.length,
  }) });
});
