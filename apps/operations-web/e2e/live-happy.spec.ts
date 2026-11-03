import { expect, test } from "@playwright/test";
import { assertNoDirectUpstream, requireLiveHandshake, toFormInstant } from "./live-helpers";

test.describe.configure({ mode: "serial" });

test.describe("OPS-04B live happy path (real Java Console)", () => {
  const hs = requireLiveHandshake("ready");
  const from = toFormInstant(hs.windowFrom);
  const to = toFormInstant(hs.windowTo);

  test("overview loads live sources without SYNTHETIC DEMO", async ({ page }) => {
    const leaked: string[] = [];
    page.on("request", (req) => leaked.push(req.url()));
    await page.goto("/");
    await expect(page.getByText("LOCAL", { exact: true })).toBeVisible();
    await expect(page.getByText("READ-ONLY", { exact: true })).toBeVisible();
    await expect(page.getByText("NOT PRODUCTION", { exact: true })).toBeVisible();
    await expect(page.getByText("SYNTHETIC DEMO", { exact: true })).toHaveCount(0);
    await expect(page.getByText("order-cell").first()).toBeVisible({ timeout: 30_000 });
    // Grants and/or snapshot freshness; empty snapshot list alone is not a fixture fallback.
    await expect(
      page.getByText(/OUTBOX_EVENT|WORKFLOW|FRESH|STALE|MISSING|READABLE/).first(),
    ).toBeVisible({ timeout: 30_000 });
    assertNoDirectUpstream(leaked, hs.consolePort);
  });

  test("recent events discovers newly created outbox ref without preconfigured UUID", async ({
    page,
  }) => {
    const leaked: string[] = [];
    page.on("request", (req) => leaked.push(req.url()));
    const eventId = hs.eventIds["order-cell"]?.[0];
    expect(eventId, "handshake must include an order-cell outbox event id").toBeTruthy();

    await page.goto("/events");
    await page.getByLabel("Environment").fill(hs.environment);
    await page.getByLabel("Owner").fill("order-cell");
    await page.getByLabel("Scope").fill("owner");
    await page.getByLabel("From (UTC Z)").fill(from);
    await page.getByLabel("To (UTC Z)").fill(to);
    await page.getByLabel("State").fill("ANY");
    // Respect Console one-admitted-read/s; retry if a prior probe left SHED/BACKOFF.
    let found = false;
    for (let attempt = 0; attempt < 4 && !found; attempt++) {
      await page.waitForTimeout(2200);
      await page.getByRole("button", { name: "Search", exact: true }).click();
      try {
        await expect(page.getByText(/BROKER ACK/)).toBeVisible({ timeout: 15_000 });
        await expect(page.getByText(/AVAILABLE|FRESH/).first()).toBeVisible({ timeout: 5_000 });
        found = true;
      } catch {
        // SHED/BACKOFF — wait and retry
      }
    }
    expect(found, "recent events search did not become AVAILABLE/FRESH").toBeTruthy();
    await expect(page.getByText("OTHER CONSUMERS UNKNOWN")).toBeVisible();
    await expect(page.getByText("DLT NOT INSPECTED")).toBeVisible();
    await expect(page.getByText(eventId!).first()).toBeVisible({ timeout: 45_000 });
    await expect(
      page.getByText(/RELATION_UNKNOWN|LATE_COMMIT_GAP|PARTIAL_CURRENT_STATE/).first(),
    ).toBeVisible();
    assertNoDirectUpstream(leaked, hs.consolePort);
  });

  test("workflows show Order / Inventory / Payment authorization-simulation states", async ({
    page,
  }) => {
    await page.goto("/workflows");
    await expect(page.getByText(/authorization-simulation|Payment/i).first()).toBeVisible();

    async function searchPanel(title: string, owner: string, scope: string, state: string) {
      const panel = page.locator("section,div").filter({ has: page.getByRole("heading", { name: title }) }).first();
      // Fallback: locate by heading then nearest form
      const heading = page.getByRole("heading", { name: title });
      await expect(heading).toBeVisible();
      const root = heading.locator("xpath=ancestor::*[.//button[normalize-space()='Search']][1]");
      await root.getByLabel("Owner").fill(owner);
      await root.getByLabel("Scope").fill(scope);
      await root.getByLabel("From (UTC Z)").fill(from);
      await root.getByLabel("To (UTC Z)").fill(to);
      await page.waitForTimeout(2200);
      await root.getByRole("button", { name: "Search", exact: true }).click();
      await expect(root.getByText(hs.orderId).first()).toBeVisible({ timeout: 45_000 });
      await expect(root.getByText(state).first()).toBeVisible();
      void panel;
    }

    await searchPanel("Order workflows", "order-cell", "order", hs.workflowStates.order);
    await searchPanel(
      "Inventory reservations",
      "inventory-cell",
      "reservation",
      hs.workflowStates.reservation,
    );
    await searchPanel(
      "Payment authorization simulation",
      "payment-cell",
      "authorization-simulation",
      hs.workflowStates["authorization-simulation"],
    );
  });

  test("wrong scope is denied without inventing rows; mutations 405; no detail bypass", async ({
    page,
    request,
  }) => {
    await page.goto("/events");
    await page.getByLabel("Owner").fill("order-cell");
    await page.getByLabel("Scope").fill("not-a-configured-grant");
    await page.getByLabel("From (UTC Z)").fill(from);
    await page.getByLabel("To (UTC Z)").fill(to);
    await page.waitForTimeout(2200);
    await page.getByRole("button", { name: "Search", exact: true }).click();
    await expect(
      page.getByText(/denied|SCOPE|BAD_REQUEST|Request failed|PERMISSION|rejected/i).first(),
    ).toBeVisible({ timeout: 30_000 });

    expect((await request.post("/api/ops/overview")).status()).toBe(405);
    expect((await request.put("/api/ops/recent/events")).status()).toBe(405);

    const detail = await request.get(`/api/ops/events/${hs.orderId}`);
    expect([404, 405]).toContain(detail.status());
  });

  test("active operations page stays bounded (no ETA invention)", async ({ page }) => {
    await page.goto("/operations");
    await expect(page.getByRole("heading", { name: /Active Operations/i })).toBeVisible();
    // Disclaimer may mention ETA; assert no countdown control / invented metric widget.
    await expect(page.getByRole("timer")).toHaveCount(0);
    await expect(page.getByText(/countdown|ETA remaining/i)).toHaveCount(0);
  });

  test("BFF overview is live JSON (fail closed if Console down)", async ({ request }) => {
    const res = await request.get("/api/ops/overview");
    expect(res.status(), "live Console must respond via BFF").toBe(200);
    const body = await res.json();
    expect(body.asOf).toBeTruthy();
    expect(body.asOf).not.toBe("2026-11-03T12:00:00.000Z");
    // Prefer non-empty snapshot rows; grants-only still proves live Console if data is [].
    if (Array.isArray(body.data) && body.data.length === 0) {
      const grants = await request.get("/api/ops/recent/sources");
      expect(grants.status()).toBe(200);
      const g = await grants.json();
      expect(Array.isArray(g) ? g.length : 0).toBeGreaterThan(0);
    } else {
      expect(body.data.length).toBeGreaterThan(0);
    }
  });
});
