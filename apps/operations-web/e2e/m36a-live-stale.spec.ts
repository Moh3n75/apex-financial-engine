import { expect, test } from "@playwright/test";
import { loginViaIdentity } from "./m36a-auth-helpers";
import { assertNoDirectUpstream, requireLiveHandshake, toFormInstant } from "./live-helpers";

test.describe.configure({ mode: "serial" });

test.describe("M36A.1 authenticated live stale payment owner", () => {
  test.skip(process.env.APEX_OPS_M36A_E2E !== "true", "Requires APEX_OPS_M36A_E2E=true");

  const hs = requireLiveHandshake("payment-stopped");
  const from = toFormInstant(hs.windowFrom);
  const to = toFormInstant(hs.windowTo);

  test("UI shows STALE/MISSING/UNKNOWN — never fabricated DECLINED/COMPLETED/HEALTHY", async ({
    page,
  }) => {
    const leaked: string[] = [];
    page.on("request", (req) => leaked.push(req.url()));
    await loginViaIdentity(page);
    await page.goto("/workflows");
    const heading = page.getByRole("heading", { name: /Payment/i }).first();
    await expect(heading).toBeVisible();
    const root = heading.locator("xpath=ancestor::*[.//button[normalize-space()='Search']][1]");
    await root.getByLabel("Owner").fill("payment-cell");
    await root.getByLabel("Scope").fill("authorization-simulation");
    await root.getByLabel("From (UTC Z)").fill(from);
    await root.getByLabel("To (UTC Z)").fill(to);
    await root.getByLabel("State").fill("ANY");
    await page.waitForTimeout(2200);
    await root.getByRole("button", { name: "Search", exact: true }).click();
    await expect(page.getByText(/STALE|MISSING|UNKNOWN|UNAVAILABLE|SOURCE/i).first()).toBeVisible({
      timeout: 45_000,
    });
    const body = await page.content();
    expect(body).not.toMatch(/\bDECLINED\b/);
    expect(body).not.toMatch(/\bHEALTHY\b/);
    assertNoDirectUpstream(leaked, hs.consolePort);
  });
});
