import { test, expect } from "@playwright/test";
import { topologySchema } from "../src/lib/topology";

/** Opt-in only; contacts the existing real Console/BFF/registry, never starts fixtures. */
test("local registry inventory survives Java to BFF to rendered Topology", async ({ page, request }) => {
  test.skip(process.env.APEX_LOCAL_INVENTORY_LIVE !== "true", "Requires explicitly running local inventory services");
  const consoleOrigin = process.env.APEX_OPERATIONS_API_ORIGIN ?? "http://127.0.0.1:9190";
  expect(new URL(consoleOrigin).hostname).toBe("127.0.0.1");
  const consoleResponse = await request.get(`${consoleOrigin}/ops/v1/topology?schemaVersion=2`);
  expect(consoleResponse.status()).toBe(200);
  const java = topologySchema.parse(await consoleResponse.json());
  const response = await request.get("/api/ops/topology");
  expect(response.status()).toBe(200);
  const bff = topologySchema.parse(await response.json());
  expect(bff.nodes.map(n => n.id).sort()).toEqual(java.nodes.map(n => n.id).sort());
  expect(bff.providers.find(p => p.name === "eureka-local-inventory")?.status).toBe("AVAILABLE");
  const services = bff.nodes.filter(n => n.kind === "SERVICE" && n.evidence.some(e => e.source === "EUREKA"));
  expect(services.length).toBeGreaterThan(0);
  await page.goto("/topology");
  await expect(page.getByTestId("topology-canvas")).toHaveAttribute("data-ready", "true");
  await expect(page.getByRole("heading", { name: "Topology", exact: true })).toBeVisible();
  await expect(page.getByText("Eureka UP means registry presence", { exact: false })).toBeVisible();
  for (const service of services) {
    await expect(page.getByRole("button", { name: `SERVICE ${service.name}`, exact: true })).toBeVisible();
    expect(service.kind).toBe("SERVICE"); expect(service.health).toBe("UNKNOWN");
    expect(service.epoch).toBeNull();
  }
  expect((await request.get("/api/ops/overview")).status()).toBe(200);
  expect((await request.post("/api/ops/topology")).status()).toBe(405);
  await test.info().attach("live-inventory-counts", { contentType: "application/json", body: JSON.stringify({
    source: "LIVE_EUREKA_VIA_CONSOLE_AND_BFF", asOf: bff.asOf, services: services.map(n => n.name),
    nodeCount: bff.nodes.length, connectionCount: bff.connections.length, membershipCount: bff.memberships.length,
  }) });
});
