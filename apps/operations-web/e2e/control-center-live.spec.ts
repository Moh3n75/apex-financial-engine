import { test, expect } from "@playwright/test";
import { loginViaIdentity, assertNoTokenLeakInHtml } from "./m36a-auth-helpers";

test("real OIDC, BFF and five-cell declared topology", async ({ page }) => {
  expect(process.env.APEX_OPS_UI_FIXTURE_MODE).toBe("false");
  const infrastructure: string[] = [];
  page.on("request", req => { const url=new URL(req.url());if(url.port==="9190"||url.pathname.startsWith("/eureka"))infrastructure.push(req.url()); });
  await loginViaIdentity(page);
  await page.goto("/topology");
  await expect(page.getByRole("heading", { name: "Topology", exact: true })).toBeVisible();
  await expect(page.getByText("SYNTHETIC DEMO", { exact: true })).toHaveCount(0);
  const response = await page.request.get("/api/ops/topology");
  expect(response.status()).toBe(200);
  expect(response.headers()["cache-control"]).toBe("no-store");
  const graph = await response.json();
  expect(graph.schemaVersion).toBe(2);
  expect(graph.nodes.filter((n: {kind:string}) => n.kind === "SERVICE")).toHaveLength(5);
  expect(graph.connections).toHaveLength(28);
  if(process.env.APEX_MEMBERSHIP_EXPECTED) {
    const current=graph.memberships.filter((m:{membershipState:string})=>["VERIFIED","DUPLICATE"].includes(m.membershipState));
    if(process.env.APEX_MEMBERSHIP_EXPECTED==="DUPLICATE") {
      expect(current.filter((m:{membershipState:string})=>m.membershipState==="DUPLICATE")).toHaveLength(2);
      await expect(page.getByLabel("Runtime membership").getByText("DUPLICATE",{exact:true})).toHaveCount(2);
    } else expect(current).toHaveLength(Number(process.env.APEX_MEMBERSHIP_EXPECTED));
    await expect(page.getByLabel("Runtime membership")).toBeVisible();
  }
  expect(graph.providers.filter((p: {name:string;status:string;sourceMode:string}) => p.name.startsWith("contracts:") &&
    (process.env.APEX_MEMBERSHIP_EXPECTED?["AVAILABLE","PARTIAL","SOURCE_UNAVAILABLE"].includes(p.status):p.status==="AVAILABLE") && p.sourceMode === "EFFECTIVE_DECLARATION")).toHaveLength(5);
  for (const cell of ["order-cell", "inventory-cell", "payment-cell", "customer-cell", "reporting-cell"]) {
    await expect(page.locator("summary").filter({ hasText: `${cell} · control-center-live · SERVICE` })).toBeVisible();
  }
  await page.locator("summary").filter({ hasText: "order-cell · control-center-live · SERVICE" }).click();
  await expect(page.getByText(/order-cell → apex.order.events.v1 · KAFKA · ORDER_PLACED:1/).first()).toBeVisible();
  assertNoTokenLeakInHtml(await page.content());
  expect(infrastructure).toEqual([]);
  expect((await page.request.post("/api/ops/topology")).status()).toBe(405);
});
