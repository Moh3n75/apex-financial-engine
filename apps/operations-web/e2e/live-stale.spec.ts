import { expect, test } from "@playwright/test";
import { requireLiveHandshake, toFormInstant } from "./live-helpers";

test.describe("OPS-04B live stale / gap after owned source stop", () => {
  const hs = requireLiveHandshake("payment-stopped");
  const from = toFormInstant(hs.windowFrom);
  const to = toFormInstant(hs.windowTo);

  test("payment workflows surface STALE or MISSING without inventing recovery", async ({
    page,
  }) => {
    await page.goto("/workflows");
    const heading = page.getByRole("heading", { name: "Payment authorization simulation" });
    await expect(heading).toBeVisible();
    const root = heading.locator("xpath=ancestor::*[.//button[normalize-space()='Search']][1]");
    await root.getByLabel("Owner").fill("payment-cell");
    await root.getByLabel("Scope").fill("authorization-simulation");
    await root.getByLabel("From (UTC Z)").fill(from);
    await root.getByLabel("To (UTC Z)").fill(to);
    await page.waitForTimeout(2200);
    await root.getByRole("button", { name: "Search", exact: true }).click();
    await expect(root.getByText(/STALE|MISSING|SOURCE_UNAVAILABLE|PERMISSION_GAP/i).first()).toBeVisible({
      timeout: 60_000,
    });
    await expect(page.getByText(/durable projection recovered|all systems healthy/i)).toHaveCount(0);
  });
});
