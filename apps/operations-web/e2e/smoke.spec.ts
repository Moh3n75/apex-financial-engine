import { expect, test } from "@playwright/test";

test.describe("OPS-04 local smoke (fixture mode)", () => {
  test("topology labels synthetic coverage and unavailable providers", async ({ page, request }) => {
    const leaked: string[] = [];
    page.on("request", (req) => { if (req.url().includes(":9190")) leaked.push(req.url()); });
    await page.goto("/topology");
    await expect(page.getByRole("heading", { name: "Topology", exact: true })).toBeVisible();
    await expect(page.getByText("SYNTHETIC DEMO", { exact: true })).toBeVisible();
    await expect(page.getByText(/No visible configured owners/)).toBeVisible();
    await expect(page.getByText("NOT_CONFIGURED", { exact: true }).first()).toBeVisible();
    const response = await request.get("/api/ops/topology");
    expect(response.status()).toBe(200);
    expect((await response.json()).coverage).toBe("SYNTHETIC_DEMO_EMPTY_TOPOLOGY");
    expect((await request.post("/api/ops/topology")).status()).toBe(405);
    expect(leaked).toEqual([]);
  });
  test("overview renders local banners and sources", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByText("LOCAL", { exact: true })).toBeVisible();
    await expect(page.getByText("READ-ONLY", { exact: true })).toBeVisible();
    await expect(page.getByText("NOT PRODUCTION", { exact: true })).toBeVisible();
    await expect(page.getByText("SYNTHETIC DEMO", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
    await expect(page.getByText("order-cell").first()).toBeVisible();
  });

  test("events page can search without browser calling :9190", async ({ page }) => {
    const leaked: string[] = [];
    page.on("request", (req) => {
      if (req.url().includes(":9190")) leaked.push(req.url());
    });
    await page.goto("/events");
    await page.getByRole("button", { name: "Search" }).click();
    await expect(page.getByText(/BROKER ACK/)).toBeVisible({ timeout: 15_000 });
    await expect(page.getByText("PENDING_PAYMENT").first()).toBeVisible();
    expect(leaked).toEqual([]);
  });

  test("mutation methods are rejected on BFF", async ({ request }) => {
    const res = await request.post("/api/ops/overview");
    expect(res.status()).toBe(405);
  });
});
