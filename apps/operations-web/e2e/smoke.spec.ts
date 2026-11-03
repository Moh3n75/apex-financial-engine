import { expect, test } from "@playwright/test";

test.describe("OPS-04 local smoke (fixture mode)", () => {
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
