import { test, expect } from "@playwright/test";
import { topologySchema } from "../src/lib/topology";

test("effective five-Cell event paths, HTTP details, focus and missing RPC callers are truthful", async ({ page, request }) => {
  test.skip(process.env.APEX_LOCAL_COMMUNICATION_LIVE !== "true", "Requires opt-in real five-Cell effective manifests");
  const response = await request.get("/api/ops/topology"); expect(response.status()).toBe(200);
  const graph = topologySchema.parse(await response.json());
  const paths = graph.connections.flatMap(e => e.details?.kafka ?? []);
  for (const [contract, producer, consumer] of [["ORDER_PLACED:1", "order-cell", "inventory-cell"], ["INVENTORY_RESERVED:1", "inventory-cell", "order-cell"],
    ["ORDER_READY_FOR_PAYMENT:1", "order-cell", "payment-cell"], ["PAYMENT_AUTHORIZED:1", "payment-cell", "order-cell"], ["CUSTOMER_CREATED:1", "customer-cell", "reporting-cell"]]) {
    expect(paths.some(p => `${p.eventType}:${p.eventVersion}` === contract && p.publisherServiceId.endsWith(`:service:${producer}`)
      && p.consumerServiceId?.endsWith(`:service:${consumer}`) && p.matchStatus === "EXACT_DECLARED_CONTRACT_MATCH")).toBe(true);
  }
  expect(graph.providers.filter(p => p.name.startsWith("contracts:")).every(p => p.sourceMode === "EFFECTIVE_DECLARATION")).toBe(true);
  expect(graph.unresolved.some(u => u.reason === "NO_VISIBLE_GRPC_CALLER_DECLARATION")).toBe(true);
  const errors: string[] = []; page.on("pageerror", error => errors.push(error.message));
  const external: string[] = []; page.on("request", r => { if (!r.url().startsWith("http://127.0.0.1:3000") && !r.url().startsWith("data:")) external.push(r.url()); });
  await page.goto("/topology");
  const canvas = page.getByTestId("topology-canvas"); await expect(canvas).toHaveAttribute("data-ready", "true");
  const sidebar = page.getByRole("complementary", { name: "Topology details" });
  const chosen = graph.connections.find(e => e.contract === "INVENTORY_RESERVED:1" && graph.nodes.some(n => n.id === e.source && n.kind === "SERVICE"))!;
  const bundle = graph.connections.filter(e => e.protocol === chosen.protocol && e.source === chosen.source && e.target === chosen.target);
  expect(bundle.length).toBeGreaterThan(1);
  const visualId = `bundle:${bundle[0].id}`;
  await page.locator(`.react-flow__edge[data-id=${JSON.stringify(visualId)}]`).click({ force: true });
  await expect(sidebar.getByRole("heading", { name: "Connection details" })).toBeVisible();
  await sidebar.getByRole("combobox", { name: "Connection contract" }).selectOption(chosen.id);
  for (const text of ["INVENTORY_RESERVED:1", "Publisher Cell: inventory-cell", "Consumer Cell: order-cell", "EXACT_DECLARED_CONTRACT_MATCH"]) await expect(sidebar.locator("p").filter({ hasText: text }).first()).toBeVisible();
  const order = graph.nodes.find(n => n.kind === "SERVICE" && n.name === "order-cell")!;
  await page.getByRole("combobox", { name: "Service focus" }).selectOption(order.id);
  await expect(canvas).toHaveAttribute("data-ready", "true");
  await expect(page.getByRole("button", { name: "SERVICE customer-cell", exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: "SERVICE order-cell", exact: true }).click();
  await expect(sidebar.getByText("NO_VISIBLE_GRPC_CALLER_DECLARATION", { exact: false }).first()).toBeVisible();
  await page.getByRole("button", { name: "Full graph overview" }).click(); await expect(canvas).toHaveAttribute("data-ready", "true");
  await page.getByRole("checkbox", { name: "Kafka", exact: true }).uncheck(); await expect(canvas).toHaveAttribute("data-ready", "true");
  await expect(page.locator(".topology-node--kafka_topic")).toHaveCount(0); await expect(page.locator('.react-flow__edge[aria-label^="KAFKA "]')).toHaveCount(0);
  await page.getByRole("checkbox", { name: "Kafka", exact: true }).check(); await expect(canvas).toHaveAttribute("data-ready", "true");
  await page.locator('.react-flow__edge[aria-label^="HTTP "]').first().click({ force: true });
  await expect(sidebar.getByText("Predicates:", { exact: false })).toBeVisible(); await expect(sidebar.getByText("Path:", { exact: false })).toBeVisible();
  expect(errors).toEqual([]); expect(external).toEqual([]);
  await page.getByRole("combobox", { name: "Service focus" }).selectOption(order.id); await expect(canvas).toHaveAttribute("data-ready", "true");
  await page.screenshot({ path: "test-results/live-communication-graph.png", fullPage: true });
  await test.info().attach("actual-communication-counts", { contentType: "application/json", body: JSON.stringify({
    source: "LIVE_EFFECTIVE_MANIFESTS_JAVA_BFF_BROWSER", asOf: graph.asOf, protocols: graph.connections.reduce<Record<string, number>>((counts,e) => ({ ...counts, [e.protocol]: (counts[e.protocol] ?? 0) + 1 }), {}),
    gRPC: "No caller edges manufactured from handlers", observedTraffic: false,
  }) });
});
