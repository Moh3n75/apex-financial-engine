import { expect, test } from "@playwright/test";
import { assertNoTokenLeakInHtml, loginViaIdentity } from "./m36a-auth-helpers";
import { assertNoDirectUpstream, requireLiveHandshake, toFormInstant } from "./live-helpers";

test.describe.configure({ mode: "serial" });

test.describe("M36A.1 authenticated live happy path", () => {
  test.skip(process.env.APEX_OPS_M36A_E2E !== "true", "Requires APEX_OPS_M36A_E2E=true");

  const hs = requireLiveHandshake("ready");
  const from = toFormInstant(hs.windowFrom);
  const to = toFormInstant(hs.windowTo);

  test("OIDC login then overview shows live owners without tokens in HTML", async ({ page }) => {
    const leaked: string[] = [];
    page.on("request", (req) => leaked.push(req.url()));
    await loginViaIdentity(page);
    await page.goto("/");
    await expect(page.getByText("SYNTHETIC DEMO", { exact: true })).toHaveCount(0);
    await expect(page.getByText("order-cell").first()).toBeVisible({ timeout: 30_000 });
    await expect(
      page.getByText(/OUTBOX_EVENT|WORKFLOW|FRESH|STALE|MISSING|READABLE/).first(),
    ).toBeVisible({ timeout: 30_000 });
    assertNoDirectUpstream(leaked, hs.consolePort);
    assertNoTokenLeakInHtml(await page.content());
  });

  test("recent events discovers outbox ref via authenticated BFF", async ({ page }) => {
    const leaked: string[] = [];
    page.on("request", (req) => leaked.push(req.url()));
    const eventId = hs.eventIds["order-cell"]?.[0];
    expect(eventId).toBeTruthy();
    await loginViaIdentity(page);
    await page.goto("/events");
    await page.getByLabel("Environment").fill(hs.environment);
    await page.getByLabel("Owner").fill("order-cell");
    await page.getByLabel("Scope").fill("owner");
    await page.getByLabel("From (UTC Z)").fill(from);
    await page.getByLabel("To (UTC Z)").fill(to);
    await page.getByLabel("State").fill("ANY");
    let found = false;
    for (let attempt = 0; attempt < 4 && !found; attempt++) {
      await page.waitForTimeout(2200);
      await page.getByRole("button", { name: "Search", exact: true }).click();
      try {
        await expect(page.getByText(/BROKER ACK/)).toBeVisible({ timeout: 15_000 });
        await expect(page.getByText(/AVAILABLE|FRESH/).first()).toBeVisible({ timeout: 5_000 });
        found = true;
      } catch {
        /* SHED/BACKOFF */
      }
    }
    expect(found).toBeTruthy();
    await expect(page.getByText(eventId!).first()).toBeVisible({ timeout: 45_000 });
    assertNoDirectUpstream(leaked, hs.consolePort);
  });

  test("workflows match SQL CONFIRMED / RESERVED / AUTHORIZED", async ({ page }) => {
    await loginViaIdentity(page);
    await page.goto("/workflows");
    await expect(page.getByText(/authorization-simulation|Payment/i).first()).toBeVisible();
    for (const [title, owner, scope, state] of [
      ["Order", "order-cell", "order", hs.workflowStates.order],
      ["Inventory", "inventory-cell", "reservation", hs.workflowStates.reservation],
      [
        "Payment",
        "payment-cell",
        "authorization-simulation",
        hs.workflowStates["authorization-simulation"],
      ],
    ] as const) {
      const heading = page.getByRole("heading", { name: new RegExp(title, "i") }).first();
      await expect(heading).toBeVisible();
      const root = heading.locator("xpath=ancestor::*[.//button[normalize-space()='Search']][1]");
      await root.getByLabel("Owner").fill(owner);
      await root.getByLabel("Scope").fill(scope);
      await root.getByLabel("From (UTC Z)").fill(from);
      await root.getByLabel("To (UTC Z)").fill(to);
      await root.getByLabel("State").fill("ANY");
      await page.waitForTimeout(2200);
      await root.getByRole("button", { name: "Search", exact: true }).click();
      await expect(page.getByText(state).first()).toBeVisible({ timeout: 45_000 });
    }
  });
});
