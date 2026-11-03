import { expect, test } from "@playwright/test";

/**
 * When M36A is enabled on the running Next process, anonymous callers must not
 * receive /api/ops data. Skipped unless APEX_OPS_M36A_E2E=true (authenticated stack).
 */
test.describe("M36A unauthenticated BFF", () => {
  test.skip(
    process.env.APEX_OPS_M36A_E2E !== "true",
    "Set APEX_OPS_M36A_E2E=true against an M36A-enabled Next instance",
  );

  test("API ops overview returns 401 without session cookie", async ({ request }) => {
    const res = await request.get("/api/ops/overview");
    expect(res.status()).toBe(401);
    const body = await res.json();
    expect(body.code).toBe("UNAUTHENTICATED");
  });

  test("browser overview redirects to login", async ({ page }) => {
    await page.goto("/");
    await expect(page).toHaveURL(/\/login/);
    await expect(page.getByRole("heading", { name: /sign-in/i })).toBeVisible();
  });
});
